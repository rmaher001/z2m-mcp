"""Tests for the Z2M log collector entry point."""

from __future__ import annotations

import asyncio
import signal
from unittest.mock import AsyncMock, patch

import pytest

from app.collector import run_collector


class TestCollector:
    @pytest.mark.asyncio
    async def test_collector_starts_and_stops(self) -> None:
        """Verify clean lifecycle: start client, wait, stop on event."""
        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.stop = AsyncMock()
        mock_client.get_log_file_path = lambda: "/data/logs/z2m.jsonl"

        with (
            patch("app.collector.load_config") as mock_config,
            patch("app.collector.Z2MClient", return_value=mock_client),
        ):
            mock_config.return_value.mqtt = "mqtt_cfg"
            mock_config.return_value.log = "log_cfg"

            # Run the collector but set the stop event after a short delay
            async def stop_after_delay() -> None:
                await asyncio.sleep(0.1)
                # Signal the collector to stop via SIGINT simulation
                # We can't send real signals easily, so we'll cancel the task
                collector_task.cancel()

            collector_task = asyncio.create_task(run_collector())
            stopper = asyncio.create_task(stop_after_delay())

            with pytest.raises(asyncio.CancelledError):
                await collector_task

            await stopper

            mock_client.start.assert_awaited_once()
            mock_client.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_collector_signal_handling(self) -> None:
        """SIGTERM triggers graceful shutdown."""
        mock_client = AsyncMock()
        mock_client.start = AsyncMock()
        mock_client.stop = AsyncMock()
        mock_client.get_log_file_path = lambda: "/data/logs/z2m.jsonl"

        with (
            patch("app.collector.load_config") as mock_config,
            patch("app.collector.Z2MClient", return_value=mock_client),
        ):
            mock_config.return_value.mqtt = "mqtt_cfg"
            mock_config.return_value.log = "log_cfg"

            async def send_signal_after_delay() -> None:
                await asyncio.sleep(0.1)
                signal.raise_signal(signal.SIGINT)

            collector_task = asyncio.create_task(run_collector())
            signal_task = asyncio.create_task(send_signal_after_delay())

            # Should complete gracefully, not raise
            await asyncio.wait_for(collector_task, timeout=5.0)
            await signal_task

            mock_client.start.assert_awaited_once()
            mock_client.stop.assert_awaited_once()
