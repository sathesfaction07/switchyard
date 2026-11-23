from __future__ import annotations


class ParallelOpsError(Exception):
    """Base for everything this package raises deliberately."""


class WorkerError(ParallelOpsError):
    """
    The dependency was reached and did not satisfy the request.

    Retryable. Covers timeouts, connection resets, 5xx and 429 — conditions that are
    plausibly transient.
    """


class PermanentError(ParallelOpsError):
    """
    The dependency answered clearly and the answer will not change.

    A 404 or a 403 is not a blip. Retrying it three times with backoff spends real
    wall-clock and real quota to be told the same thing three more times, and on a large
    batch that is the difference between a run finishing and a run timing out.
    """


class SchemaError(ParallelOpsError):
    """
    A response arrived and did not match the declared schema.

    Deliberately NOT retryable. A malformed payload is a contract problem, not a blip:
    calling again returns the same malformed payload, so a retry burns quota and delays
    the batch to learn nothing. This is what makes response validation worth having as a
    separate step rather than folding it into the worker.
    """


class CircuitOpenError(ParallelOpsError):
    """
    The circuit for this host is open, so the call was never made.

    Deliberately NOT retryable. Retrying into an open circuit is worse than pointless:
    the breaker exists precisely to stop traffic, and a retry loop that backs off and
    tries again is defeating it while burning wall-clock.
    """


class WorkerUnavailableError(ParallelOpsError):
    """A worker was requested whose optional dependency is not installed."""
