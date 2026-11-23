from __future__ import annotations

import httpx

from ..models import WorkerKind, WorkItem
from ..resilience.errors import PermanentError, WorkerError
from .base import Worker

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class ApiWorker(Worker):
    """
    HTTP worker over a shared `httpx.AsyncClient`.

    One client is reused for the whole batch so connection pooling and keep-alive
    actually apply. Creating a client per item is the common mistake and it turns a
    batch of 500 into 500 TLS handshakes.

    Status codes are sorted into the two failure classes rather than lumped together,
    because that decision is the difference between a batch that finishes and one that
    spends its budget re-asking for things that do not exist. A 404 fails once; a 503
    is worth another go.
    """

    kind = WorkerKind.API

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._timeout = timeout
        self._headers = headers or {}
        self._client = client
        self._owns_client = client is None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers=self._headers,
                follow_redirects=True,
            )
        return self._client

    async def execute(self, item: WorkItem) -> dict:
        client = self._ensure_client()
        url = item.target()

        try:
            response = await client.get(url)
        except httpx.TimeoutException as exc:
            raise WorkerError(f"timeout after {self._timeout}s") from exc
        except httpx.HTTPError as exc:
            raise WorkerError(f"transport error: {type(exc).__name__}") from exc

        status = response.status_code

        if status in RETRYABLE_STATUS:
            raise WorkerError(f"HTTP {status}")
        if 400 <= status < 500:
            raise PermanentError(f"HTTP {status}")
        if status >= 500:
            raise WorkerError(f"HTTP {status}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise WorkerError(f"response was not JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise WorkerError(f"expected a JSON object, got {type(payload).__name__}")

        return payload

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None
