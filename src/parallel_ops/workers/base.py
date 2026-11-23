from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import WorkerKind, WorkItem


class Worker(ABC):
    """
    Executes one work item against a dependency.

    Every worker is async and every worker runs on the same event loop, which is the
    whole point: an item needing a browser and an item needing a plain HTTP call are
    scheduled by the same governor and share the same per-host limiter and breaker. A
    slow browser task yields at every await, so it delays only itself rather than the
    API items queued behind it.

    Implementations raise from the taxonomy in `resilience.errors`:

      WorkerError     transient — retry may help (timeout, 5xx, 429)
      PermanentError  settled — retry cannot help (404, 403)

    Returning anything other than a dict is a programming error. Validation of the dict
    against a declared schema happens later and separately, so a worker never decides
    whether a payload is acceptable.
    """

    kind: WorkerKind

    @abstractmethod
    async def execute(self, item: WorkItem) -> dict:
        """Perform one physical call. Raise on failure, never return a sentinel."""

    @classmethod
    def is_available(cls) -> bool:
        """Whether this worker's dependencies are installed on this machine."""
        return True

    @classmethod
    def unavailable_reason(cls) -> str:
        return ""

    async def aclose(self) -> None:
        """Release held resources. Safe to call more than once."""
        return None
