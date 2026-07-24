"""Bronze sink, first version: consume, count, commit."""
import logging
from collections import Counter

from telemetry.config import get_settings
from telemetry.consumers.common import (
    GracefulShutdown,
    build_consumer,
    fmt_partitions,
    real_messages,
)
from telemetry.log import setup_logging

log = logging.getLogger("bronze_sink")


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    consumer = build_consumer(settings, "bronze-sink")
    consumer.subscribe(
        [settings.topic_sets, settings.topic_recovery, settings.topic_dlq],
        on_assign=lambda c, ps: log.info("assigned %s", fmt_partitions(ps)),
        on_revoke=lambda c, ps: log.info("revoked %s", fmt_partitions(ps)),
    )
    shutdown = GracefulShutdown()
    seen: Counter[str] = Counter()
    try:
        while not shutdown.requested:
            msgs = real_messages(consumer.consume(num_messages=500, timeout=1.0))
            if not msgs:
                continue
            for m in msgs:
                seen[m.topic()] += 1
            consumer.commit(asynchronous=False)  # commits the position after the last message
            log.info("progress %s", dict(seen))
    finally:
        consumer.close()  # leave the group now, so a rebalance happens immediately


if __name__ == "__main__":
    main()
