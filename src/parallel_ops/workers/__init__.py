from __future__ import annotations

from ..models import WorkerKind
from ..resilience.errors import WorkerUnavailableError
from .api import ApiWorker
from .base import Worker
from .browser import BrowserWorker

WORKERS: dict[WorkerKind, type[Worker]] = {
    WorkerKind.API: ApiWorker,
    WorkerKind.BROWSER: BrowserWorker,
}


def get_worker(kind: WorkerKind, **kwargs) -> Worker:
    """
    Build a worker, failing with an instruction rather than a traceback.

    Availability is checked before construction so a missing optional dependency reads
    as "here is the command to fix this" instead of an ImportError from three frames
    down inside a coroutine.
    """
    cls = WORKERS.get(kind)
    if cls is None:
        known = ", ".join(sorted(k.value for k in WORKERS))
        raise WorkerUnavailableError(f"unknown worker '{kind}'; known workers: {known}")
    if not cls.is_available():
        raise WorkerUnavailableError(cls.unavailable_reason())
    return cls(**kwargs)


def availability() -> dict[str, tuple[bool, str]]:
    """Per-worker `(available, reason_if_not)`, for the `doctor` command."""
    return {
        kind.value: (cls.is_available(), cls.unavailable_reason()) for kind, cls in WORKERS.items()
    }


__all__ = ["WORKERS", "ApiWorker", "BrowserWorker", "Worker", "availability", "get_worker"]
