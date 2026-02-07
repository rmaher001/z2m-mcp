"""MQTT client for Zigbee2MQTT communication."""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import aiomqtt

from app.config import LogConfig, MQTTConfig

logger = logging.getLogger(__name__)

Z2M_BASE = "zigbee2mqtt"
Z2M_BRIDGE = f"{Z2M_BASE}/bridge"
Z2M_BRIDGE_DEVICES = f"{Z2M_BRIDGE}/devices"
Z2M_BRIDGE_INFO = f"{Z2M_BRIDGE}/info"
Z2M_BRIDGE_LOGGING = f"{Z2M_BRIDGE}/logging"
Z2M_BRIDGE_STATE = f"{Z2M_BRIDGE}/state"
Z2M_BRIDGE_RESPONSE = f"{Z2M_BRIDGE}/response"

LOG_BUFFER_MAX = 1000


class Z2MClient:
    """Async MQTT client for Zigbee2MQTT."""

    def __init__(self, config: MQTTConfig, log_config: LogConfig | None = None) -> None:
        self._config = config
        self._devices: list[dict[str, Any]] = []
        self._device_states: dict[str, dict[str, Any]] = {}
        self._bridge_info: dict[str, Any] | None = None
        self._response_events: dict[str, asyncio.Event] = {}
        self._response_data: dict[str, dict[str, Any]] = {}
        self._client: aiomqtt.Client | None = None
        self._listen_task: asyncio.Task[None] | None = None

        # Log capture
        self._log_buffer: collections.deque[dict[str, str]] = collections.deque(
            maxlen=LOG_BUFFER_MAX,
        )
        self._log_writer: RotatingFileHandler | None = None
        self._log_file_path: str | None = None

        if log_config:
            self._init_log_writer(log_config)

    def _init_log_writer(self, log_config: LogConfig) -> None:
        """Set up rotating JSONL file writer for Z2M logs."""
        os.makedirs(log_config.dir, exist_ok=True)
        self._log_file_path = os.path.join(log_config.dir, "z2m.jsonl")
        self._log_writer = RotatingFileHandler(
            self._log_file_path,
            maxBytes=log_config.max_size_mb * 1024 * 1024,
            backupCount=log_config.backup_count,
            encoding="utf-8",
        )

    async def start(self) -> None:
        """Connect to MQTT broker and start listening for Z2M messages."""
        self._client = aiomqtt.Client(
            hostname=self._config.host,
            port=self._config.port,
            username=self._config.username,
            password=self._config.password,
        )
        try:
            await self._client.__aenter__()

            # Subscribe to Z2M topics
            await self._client.subscribe(f"{Z2M_BRIDGE}/#")
            await self._client.subscribe(f"{Z2M_BASE}/+")

            self._listen_task = asyncio.create_task(self._listen())
            logger.info("Z2M MQTT client started")

            # Wait briefly for retained messages
            await asyncio.sleep(1.0)
        except Exception:
            if self._client:
                await self._client.__aexit__(None, None, None)
                self._client = None
            raise

    async def stop(self) -> None:
        """Disconnect from MQTT broker."""
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.__aexit__(None, None, None)
        if self._log_writer:
            self._log_writer.close()
        logger.info("Z2M MQTT client stopped")

    async def _listen(self) -> None:
        """Listen for incoming MQTT messages."""
        if not self._client:
            return
        async for message in self._client.messages:
            topic = str(message.topic)
            payload = message.payload
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8", errors="replace")

            try:
                self._route_message(topic, payload)
            except Exception:
                logger.exception("Error processing message on %s", topic)

    def _route_message(self, topic: str, payload: str) -> None:
        """Route incoming message to appropriate handler."""
        if topic == Z2M_BRIDGE_DEVICES:
            self._process_devices_message(payload)
        elif topic == Z2M_BRIDGE_INFO:
            self._process_bridge_info_message(payload)
        elif topic == Z2M_BRIDGE_LOGGING:
            self._process_log_message(payload)
        elif topic.startswith(f"{Z2M_BRIDGE_RESPONSE}/"):
            self._process_response(topic, payload)
        elif topic.startswith(f"{Z2M_BRIDGE}/"):
            # Other bridge messages (state, etc.) - ignore
            pass
        else:
            # Device state updates
            name = self._device_name_from_topic(topic)
            if name:
                self._process_device_state(name, payload)

    def _process_devices_message(self, payload: str) -> None:
        """Update device cache from bridge/devices message."""
        devices = json.loads(payload)
        self._devices = devices
        logger.debug("Updated device cache: %d devices", len(devices))

    def _process_bridge_info_message(self, payload: str) -> None:
        """Update bridge info cache."""
        self._bridge_info = json.loads(payload)
        logger.debug("Updated bridge info")

    def _process_log_message(self, payload: str) -> None:
        """Process a Z2M bridge/logging message into buffer and JSONL file."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug("Non-JSON log payload: %.100s", payload)
            return

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": data.get("level", "info"),
            "message": data.get("message", ""),
        }

        self._log_buffer.append(entry)

        if self._log_writer:
            try:
                line = json.dumps(entry, separators=(",", ":"))
                record = logging.LogRecord(
                    name="z2m",
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=line,
                    args=(),
                    exc_info=None,
                )
                self._log_writer.emit(record)
            except Exception:
                logger.exception("Failed to write log entry to file")

    def _process_device_state(self, device_name: str, payload: str) -> None:
        """Update device state from individual device topic."""
        try:
            state = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug("Non-JSON payload for %s: %.100s", device_name, payload)
            return
        self._device_states[device_name] = state

    def _process_response(self, topic: str, payload: str) -> None:
        """Handle response to a request-response call."""
        data = json.loads(payload)
        if topic in self._response_events:
            self._response_data[topic] = data
            self._response_events[topic].set()

    def _device_name_from_topic(self, topic: str) -> str | None:
        """Extract device friendly_name from topic like 'zigbee2mqtt/Device Name'."""
        if not topic.startswith(f"{Z2M_BASE}/"):
            return None
        remainder = topic[len(f"{Z2M_BASE}/"):]
        if remainder.startswith("bridge"):
            return None
        return remainder

    def get_all_devices(self) -> list[dict[str, Any]]:
        """Return all cached devices."""
        result = []
        for dev in self._devices:
            enriched = dict(dev)
            name = dev.get("friendly_name", "")
            if name in self._device_states:
                enriched["state"] = self._device_states[name]
            result.append(enriched)
        return result

    def get_device(self, identifier: str) -> dict[str, Any] | None:
        """Look up a device by friendly_name or ieee_address."""
        for dev in self._devices:
            if dev.get("friendly_name") == identifier or dev.get("ieee_address") == identifier:
                enriched = dict(dev)
                name = dev.get("friendly_name", "")
                if name in self._device_states:
                    enriched["state"] = self._device_states[name]
                return enriched
        return None

    def get_bridge_info(self) -> dict[str, Any] | None:
        """Return cached bridge info."""
        return self._bridge_info

    def get_logs(
        self,
        minutes_back: int = 60,
        level: str | None = None,
    ) -> list[dict[str, str]]:
        """Return log entries from the in-memory buffer.

        Args:
            minutes_back: Only return entries from the last N minutes.
            level: Filter by log level (error, warn, info, debug).

        Returns:
            List of log entry dicts with timestamp, level, message.
        """
        now = datetime.now(timezone.utc)
        results = []

        for entry in self._log_buffer:
            # Filter by time
            try:
                ts = datetime.fromisoformat(entry["timestamp"])
                age_minutes = (now - ts).total_seconds() / 60
                if age_minutes > minutes_back:
                    continue
            except (ValueError, KeyError):
                continue

            # Filter by level
            if level and entry.get("level") != level:
                continue

            results.append(entry)

        return results

    def get_logs_from_file(
        self,
        minutes_back: int = 60,
        level: str | None = None,
    ) -> list[dict[str, str]]:
        """Read log entries from the persistent JSONL file.

        Used to retrieve logs written by the collector sidecar container.

        Args:
            minutes_back: Only return entries from the last N minutes.
            level: Filter by log level (error, warn, info, debug).

        Returns:
            List of log entry dicts with timestamp, level, message.
        """
        if not self._log_file_path:
            return []

        now = datetime.now(timezone.utc)
        results = []

        try:
            f = open(self._log_file_path, encoding="utf-8")
        except FileNotFoundError:
            return []

        with f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Filter by time
                try:
                    ts = datetime.fromisoformat(entry["timestamp"])
                    age_minutes = (now - ts).total_seconds() / 60
                    if age_minutes > minutes_back:
                        continue
                except (ValueError, KeyError):
                    continue

                # Filter by level
                if level and entry.get("level") != level:
                    continue

                results.append(entry)

        return results

    def get_log_buffer_size(self) -> int:
        """Return the number of entries in the in-memory log buffer."""
        return len(self._log_buffer)

    def get_log_file_path(self) -> str | None:
        """Return the path to the current JSONL log file."""
        return self._log_file_path

    async def request_response(
        self,
        request_topic: str,
        response_topic: str,
        payload: dict[str, Any],
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Publish a request and wait for a response.

        Raises:
            TimeoutError: If no response within timeout.
            RuntimeError: If response indicates error.
        """
        event = asyncio.Event()
        self._response_events[response_topic] = event

        try:
            # Publish request
            if self._client:
                await self._client.publish(request_topic, json.dumps(payload))

            # Wait for response
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"No response on {response_topic} within {timeout}s"
                )

            data = self._response_data.pop(response_topic, {})

            # Check for error response
            if data.get("status") == "error":
                raise RuntimeError(data.get("error", "Unknown Z2M error"))

            return data
        finally:
            self._response_events.pop(response_topic, None)

    async def publish(self, topic: str, payload: dict[str, Any] | str) -> None:
        """Publish a message to a topic."""
        if not self._client:
            raise RuntimeError("MQTT client not connected")
        msg = payload if isinstance(payload, str) else json.dumps(payload)
        await self._client.publish(topic, msg)
