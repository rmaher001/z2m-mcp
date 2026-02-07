"""Log collector entry point — long-lived MQTT subscriber for Z2M logs.

Runs as a sidecar container, continuously capturing Zigbee2MQTT log messages
from the bridge/logging MQTT topic and writing them to a persistent JSONL file.

Usage:
    python -m app.collector
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from app.config import load_config
from app.mqtt_client import Z2MClient

logger = logging.getLogger(__name__)


async def run_collector() -> None:
    """Start the Z2M log collector and run until signalled to stop."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config()
    z2m = Z2MClient(config.mqtt, log_config=config.log)

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await z2m.start()
    logger.info("Z2M log collector running — writing to %s", z2m.get_log_file_path())

    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down Z2M log collector")
        await z2m.stop()


def main() -> None:
    try:
        asyncio.run(run_collector())
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
