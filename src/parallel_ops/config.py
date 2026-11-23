from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuration, `PO_`-prefixed environment variables or a `.env` file.

    No list-typed fields anywhere. Pydantic Settings parses those as JSON from the
    environment, which produces confusing failures for anyone who reasonably writes
    `PO_HOSTS=a,b` and gets a JSON decode error back.

    The defaults are conservative on purpose. A tool whose entire job is to avoid
    hammering someone else's service should not ship with a rate of 100/s and 64-way
    concurrency, because the first run somebody does will be with the defaults.
    """

    model_config = SettingsConfigDict(
        env_prefix="PO_",
        env_file=".env",
        extra="ignore",
    )

    concurrency: int = 8

    rate: float = 10.0
    burst: float = 0.0

    failure_threshold: int = 5
    reset_timeout: float = 30.0
    success_threshold: int = 1

    attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.1

    timeout: float = 10.0
    headless: bool = True

    schema_name: str = "catalog"

    output_dir: Path = Path("output")
    output_format: str = "csv"
    log_level: str = "INFO"

    @property
    def effective_burst(self) -> float | None:
        """
        Burst capacity, or None to let the bucket derive it from the rate.

        Zero means "not set" rather than "no burst", because a burst of zero would block
        every call forever and there is no sensible reason to configure that.
        """
        return self.burst if self.burst > 0 else None
