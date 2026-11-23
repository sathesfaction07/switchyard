from __future__ import annotations

import httpx
import pytest

from parallel_ops.models import WorkerKind, WorkItem
from parallel_ops.resilience.errors import PermanentError, WorkerError, WorkerUnavailableError
from parallel_ops.workers import WORKERS, availability, get_worker
from parallel_ops.workers.api import RETRYABLE_STATUS, ApiWorker
from parallel_ops.workers.base import Worker
from parallel_ops.workers.browser import BrowserWorker


def worker_for(handler) -> ApiWorker:
    """ApiWorker wired to a mock transport, so nothing touches the network."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ApiWorker(client=client)


@pytest.fixture
def api_item():
    return WorkItem(item_id="T-1", host="https://catalog.example.com", path="items/SKU-1")


class TestApiWorkerSuccess:
    async def test_returns_the_decoded_json_object(self, api_item):
        def handler(request):
            return httpx.Response(200, json={"product_id": "SKU-1", "status": "active"})

        worker = worker_for(handler)
        try:
            assert await worker.execute(api_item) == {"product_id": "SKU-1", "status": "active"}
        finally:
            await worker.aclose()

    async def test_builds_the_url_from_host_and_path(self, api_item):
        seen: list[str] = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, json={})

        worker = worker_for(handler)
        try:
            await worker.execute(api_item)
        finally:
            await worker.aclose()

        assert seen == ["https://catalog.example.com/items/SKU-1"]

    async def test_a_bare_host_needs_no_path(self):
        def handler(request):
            assert str(request.url) == "https://catalog.example.com"
            return httpx.Response(200, json={})

        worker = worker_for(handler)
        try:
            await worker.execute(WorkItem(item_id="T-1", host="https://catalog.example.com"))
        finally:
            await worker.aclose()

    async def test_joining_does_not_double_the_slash(self):
        seen: list[str] = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, json={})

        worker = worker_for(handler)
        try:
            await worker.execute(
                WorkItem(item_id="T-1", host="https://h.example.com/", path="/items/1")
            )
        finally:
            await worker.aclose()

        assert seen == ["https://h.example.com/items/1"]


class TestStatusClassification:
    """
    The split that decides whether a batch finishes or spends its budget re-asking for
    things that do not exist.
    """

    @pytest.mark.parametrize("status", sorted(RETRYABLE_STATUS))
    async def test_retryable_statuses_raise_worker_error(self, status, api_item):
        worker = worker_for(lambda request: httpx.Response(status))
        try:
            with pytest.raises(WorkerError, match=str(status)):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
    async def test_client_errors_are_permanent(self, status, api_item):
        worker = worker_for(lambda request: httpx.Response(status))
        try:
            with pytest.raises(PermanentError, match=str(status)):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    async def test_429_is_retryable_not_permanent(self, api_item):
        """
        429 sits inside the 4xx range but is the one code in it that explicitly means
        "try again later", so the retryable set is checked before the 4xx rule.
        """
        worker = worker_for(lambda request: httpx.Response(429))
        try:
            with pytest.raises(WorkerError):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    @pytest.mark.parametrize("status", [501, 505, 599])
    async def test_unlisted_server_errors_are_still_retryable(self, status, api_item):
        worker = worker_for(lambda request: httpx.Response(status))
        try:
            with pytest.raises(WorkerError, match=str(status)):
                await worker.execute(api_item)
        finally:
            await worker.aclose()


class TestTransportAndBodyFailures:
    async def test_a_timeout_becomes_a_retryable_worker_error(self, api_item):
        def handler(request):
            raise httpx.ReadTimeout("too slow", request=request)

        worker = worker_for(handler)
        try:
            with pytest.raises(WorkerError, match="timeout"):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    async def test_a_transport_error_becomes_a_retryable_worker_error(self, api_item):
        def handler(request):
            raise httpx.ConnectError("refused", request=request)

        worker = worker_for(handler)
        try:
            with pytest.raises(WorkerError, match="transport error"):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    async def test_a_non_json_body_is_retryable(self, api_item):
        """
        Usually a proxy error page or a truncated response, both of which are worth
        another attempt.
        """
        worker = worker_for(lambda request: httpx.Response(200, text="<html>oops</html>"))
        try:
            with pytest.raises(WorkerError, match="not JSON"):
                await worker.execute(api_item)
        finally:
            await worker.aclose()

    async def test_a_json_array_is_rejected_with_its_type(self, api_item):
        """
        Valid JSON but the wrong top-level shape. Naming the type received saves a round
        of guessing.
        """
        worker = worker_for(lambda request: httpx.Response(200, json=[1, 2, 3]))
        try:
            with pytest.raises(WorkerError, match="list"):
                await worker.execute(api_item)
        finally:
            await worker.aclose()


class TestClientLifecycle:
    async def test_an_injected_client_is_not_closed_by_the_worker(self, api_item):
        """
        Closing a client the caller owns would break the next batch that shares it, and
        the bug would surface far from here.
        """
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
        )
        worker = ApiWorker(client=client)

        await worker.execute(api_item)
        await worker.aclose()

        assert client.is_closed is False
        await client.aclose()

    async def test_a_self_made_client_is_reused_across_items(self):
        """One client per batch, not per item, or 500 items means 500 TLS handshakes."""
        worker = ApiWorker()
        first = worker._ensure_client()
        second = worker._ensure_client()
        assert first is second
        await worker.aclose()

    async def test_closing_twice_is_safe(self):
        worker = ApiWorker()
        worker._ensure_client()
        await worker.aclose()
        await worker.aclose()


class TestWorkerRegistry:
    def test_the_api_worker_is_always_available(self):
        assert ApiWorker.is_available() is True
        assert ApiWorker.unavailable_reason() == ""

    def test_get_worker_builds_an_api_worker(self):
        assert isinstance(get_worker(WorkerKind.API, timeout=5.0), ApiWorker)

    def test_an_unknown_kind_names_the_known_ones(self):
        with pytest.raises(WorkerUnavailableError, match="known workers"):
            get_worker("carrier-pigeon")  # type: ignore[arg-type]

    def test_availability_covers_every_registered_worker(self):
        report = availability()
        assert set(report) == {kind.value for kind in WORKERS}

    def test_availability_reports_a_reason_when_unavailable(self):
        """
        `doctor` should print an instruction, not a bare False. A missing optional
        dependency is a fixable state and the fix belongs in the message.
        """
        for name, (ok, reason) in availability().items():
            if not ok:
                assert reason, f"{name} is unavailable with no explanation"
                assert "install" in reason.lower() or "pip" in reason.lower()

    def test_the_browser_worker_declares_its_kind_without_importing_playwright(self):
        """
        Class attributes and availability checks must work on a machine that has never
        installed a browser, or `doctor` cannot run there.
        """
        assert BrowserWorker.kind is WorkerKind.BROWSER
        assert isinstance(BrowserWorker.is_available(), bool)

    def test_an_unavailable_browser_worker_refuses_construction_with_instructions(self):
        if BrowserWorker.is_available():
            pytest.skip("playwright is installed in this environment")
        with pytest.raises(WorkerUnavailableError, match="install"):
            get_worker(WorkerKind.BROWSER)


class TestWorkerContract:
    def test_the_base_class_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            Worker()  # type: ignore[abstract]

    def test_every_registered_worker_declares_a_kind_matching_its_key(self):
        for kind, cls in WORKERS.items():
            assert cls.kind is kind, f"{cls.__name__} is registered under the wrong kind"

    async def test_the_default_aclose_is_a_no_op(self, api_item):
        class Minimal(Worker):
            kind = WorkerKind.API

            async def execute(self, item):
                return {}

        await Minimal().aclose()
