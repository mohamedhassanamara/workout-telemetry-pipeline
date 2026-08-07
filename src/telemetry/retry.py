"""Retry with capped exponential backoff and full jitter, for TRANSIENT failures only."""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
log = logging.getLogger(__name__)


def retry(
    fn: Callable[[], T],
    *,
    retry_on: tuple[type[BaseException], ...],
    attempts: int = 6,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    sleep: Callable[[float], None] = time.sleep,
    what: str = "operation",
) -> T:
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == attempts:
                log.error("%s failed after %d attempts: %s", what, attempts, exc)
                raise
            delay = random.uniform(0, min(max_delay, base_delay * 2 ** (attempt - 1)))
            log.warning(
                "%s failed (attempt %d/%d): %s; retrying in %.2fs", what, attempt, attempts, exc, delay
            )
            sleep(delay)
    raise AssertionError("unreachable")
