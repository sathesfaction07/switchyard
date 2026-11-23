from __future__ import annotations

import json
import re

from ..models import WorkerKind, WorkItem
from ..resilience.errors import PermanentError, WorkerError, WorkerUnavailableError
from .base import Worker

_JSON_BLOCK = re.compile(r"\{.*\}", re.DOTALL)


class BrowserWorker(Worker):
    """
    Playwright worker for items that a plain HTTP call cannot satisfy.

    Optional. Playwright is an extra and the import is deferred, so `doctor` and the
    whole API path work on a machine that has never installed a browser. Requesting
    `--worker browser` without it produces a clear instruction rather than an
    ImportError traceback.

    One browser and one context are shared across the batch. A fresh page per item is
    cheap; a fresh browser per item costs roughly a second of process startup and would
    dominate everything the limiter and breaker are trying to manage.

    This worker is the reason the scheduler is built around a single event loop. A page
    load takes hundreds of milliseconds and yields throughout, so API items keep flowing
    while it waits. Running the browser in a thread pool instead would mean two separate
    concurrency budgets and two separate places to reason about the rate limit.
    """

    kind = WorkerKind.BROWSER

    def __init__(
        self,
        *,
        timeout: float = 15.0,
        headless: bool = True,
        selector: str = "pre",
    ) -> None:
        self._timeout = timeout
        self._headless = headless
        self._selector = selector
        self._playwright = None
        self._browser = None
        self._context = None

    @classmethod
    def is_available(cls) -> bool:
        try:
            import playwright.async_api  # noqa: F401
        except ImportError:
            return False
        return True

    @classmethod
    def unavailable_reason(cls) -> str:
        if cls.is_available():
            return ""
        return (
            "playwright is not installed - run: "
            "pip install 'parallel-ops[browser]' && playwright install chromium"
        )

    async def _ensure_context(self):
        if self._context is not None:
            return self._context

        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise WorkerUnavailableError(self.unavailable_reason()) from exc

        self._playwright = await async_playwright().start()
        try:
            self._browser = await self._playwright.chromium.launch(headless=self._headless)
        except Exception as exc:
            await self._shutdown_playwright()
            raise WorkerUnavailableError(
                f"could not launch chromium ({exc}) - run: playwright install chromium"
            ) from exc

        self._context = await self._browser.new_context()
        return self._context

    async def execute(self, item: WorkItem) -> dict:
        context = await self._ensure_context()
        page = await context.new_page()
        try:
            try:
                response = await page.goto(
                    item.target(),
                    timeout=self._timeout * 1000,
                    wait_until="domcontentloaded",
                )
            except Exception as exc:
                raise WorkerError(f"navigation failed: {type(exc).__name__}") from exc

            if response is not None:
                status = response.status
                if 400 <= status < 500 and status not in (408, 425, 429):
                    raise PermanentError(f"HTTP {status}")
                if status >= 500 or status in (408, 425, 429):
                    raise WorkerError(f"HTTP {status}")

            body = await page.content()
            return self._extract(body)
        finally:
            await page.close()

    def _extract(self, html: str) -> dict:
        """
        Pull a JSON object out of a rendered page.

        Deliberately simple, and deliberately raising `WorkerError` rather than
        `SchemaError` when nothing is found. An empty page is usually a rendering race
        worth retrying, whereas a payload that parsed but has the wrong shape is a
        contract problem for the validator to reject permanently. Getting those two the
        wrong way round means either retrying forever or giving up on a blip.
        """
        match = _JSON_BLOCK.search(html)
        if match is None:
            raise WorkerError("no JSON object found in the rendered page")
        try:
            payload = json.loads(match.group(0))
        except ValueError as exc:
            raise WorkerError(f"page content was not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise WorkerError(f"expected a JSON object, got {type(payload).__name__}")
        return payload

    async def _shutdown_playwright(self) -> None:
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def aclose(self) -> None:
        if self._context is not None:
            await self._context.close()
            self._context = None
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        await self._shutdown_playwright()
