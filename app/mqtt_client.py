"""MQTT client for Zigbee2MQTT communication."""

from __future__ import annotations

import asyncio
import collections
import glob
import json
import logging
import os
import time
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

    def __init__(
        self,
        config: MQTTConfig,
        log_config: LogConfig | None = None,
        *,
        reconnect_interval: float = 5.0,
        connect_timeout: float = 10.0,
        settle_delay: float = 1.0,
    ) -> None:
        self._config = config
        self._reconnect_interval = reconnect_interval
        self._connect_timeout = connect_timeout
        self._settle_delay = settle_delay
        self._running = False
        self._connected_event = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None
        self._devices: list[dict[str, Any]] = []
        self._device_states: dict[str, dict[str, Any]] = {}
        self._device_availability: dict[str, str] = {}
        self._bridge_info: dict[str, Any] | None = None
        self._response_events: dict[str, asyncio.Event] = {}
        self._response_data: dict[str, dict[str, Any]] = {}
        self._response_locks: dict[str, asyncio.Lock] = {}
        self._client: aiomqtt.Client | None = None

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
        self._cleanup_old_logs(log_config)
        self._log_writer = RotatingFileHandler(
            self._log_file_path,
            maxBytes=log_config.max_size_mb * 1024 * 1024,
            backupCount=log_config.backup_count,
            encoding="utf-8",
        )

    def _cleanup_old_logs(self, log_config: LogConfig) -> None:
        """Delete old log files based on retention_days and max_total_mb."""
        pattern = os.path.join(log_config.dir, "z2m.jsonl*")
        try:
            files = glob.glob(pattern)
        except OSError:
            logger.exception("Failed to glob log files in %s", log_config.dir)
            return
        if not files:
            return

        cutoff = time.time() - (log_config.retention_days * 86400)

        # Delete files older than retention_days
        remaining = []
        for path in files:
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    logger.info("Deleted old log file: %s", path)
                else:
                    remaining.append(path)
            except OSError:
                logger.exception("Failed to check/remove log file: %s", path)

        # Enforce max_total_mb cap — delete oldest first
        max_bytes = log_config.max_total_mb * 1024 * 1024
        remaining.sort(key=lambda p: os.path.getmtime(p))
        sizes: dict[str, int] = {}
        for p in remaining:
            try:
                sizes[p] = os.path.getsize(p)
            except OSError:
                sizes[p] = 0
        total = sum(sizes.values())
        while total > max_bytes and remaining:
            oldest = remaining.pop(0)
            try:
                total -= sizes[oldest]
                os.remove(oldest)
                logger.info("Deleted log file for size cap: %s", oldest)
            except OSError:
                logger.exception("Failed to remove log file: %s", oldest)

    async def start(self) -> None:
        """Start the MQTT connection manager.

        Launches a background task that connects, subscribes, listens, and
        RECONNECTS automatically when the broker connection drops. Waits up to
        ``connect_timeout`` for the first successful connection so the caller
        sees populated caches — but never blocks forever: if the broker is
        unreachable at startup, the manager keeps retrying in the background
        instead of raising (so the MCP server still boots).
        """
        self._running = True
        self._connected_event = asyncio.Event()
        self._run_task = asyncio.create_task(self._run())
        try:
            await asyncio.wait_for(
                self._connected_event.wait(), timeout=self._connect_timeout
            )
        except asyncio.TimeoutError:
            logger.warning(
                "MQTT broker not reachable within %.0fs at startup; "
                "reconnecting in the background",
                self._connect_timeout,
            )
        else:
            # Let retained messages (bridge info/devices, states) arrive.
            if self._settle_delay > 0:
                await asyncio.sleep(self._settle_delay)

    async def stop(self) -> None:
        """Stop the connection manager and disconnect from the broker."""
        self._running = False
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None
        if self._log_writer:
            self._log_writer.close()
        logger.info("Z2M MQTT client stopped")

    async def _run(self) -> None:
        """Connect, subscribe, and listen — reconnecting on connection loss.

        aiomqtt 2.x has no built-in reconnection, so a dropped socket ends the
        ``async for`` message iterator. Without this loop the process stays
        alive serving frozen caches indefinitely (the observed 'up 4 days,
        10h-stale' failure). Each loop iteration re-establishes the connection
        and re-subscribes to every Z2M topic.
        """
        while self._running:
            try:
                async with aiomqtt.Client(
                    hostname=self._config.host,
                    port=self._config.port,
                    username=self._config.username,
                    password=self._config.password,
                ) as client:
                    self._client = client
                    await client.subscribe(f"{Z2M_BRIDGE}/#")
                    await client.subscribe(f"{Z2M_BASE}/+")
                    await client.subscribe(f"{Z2M_BASE}/+/availability")
                    self._connected_event.set()
                    logger.info("Z2M MQTT client connected")
                    async for message in client.messages:
                        self._handle_message(message)
            except aiomqtt.MqttError as exc:
                logger.warning("MQTT connection lost: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Defense-in-depth: any unexpected exception must NOT kill the
                # loop, or the caches would silently freeze again via a
                # different trigger than the original MqttError bug.
                logger.exception("Unexpected error in Z2M MQTT run loop")
            finally:
                self._client = None
            if self._running:
                await asyncio.sleep(self._reconnect_interval)

    def _handle_message(self, message: aiomqtt.Message) -> None:
        """Decode and route a single incoming MQTT message."""
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
        elif topic.endswith("/availability"):
            # Per-device availability: zigbee2mqtt/<device>/availability
            name = topic[len(f"{Z2M_BASE}/"):-len("/availability")]
            if name and "/" not in name:
                self._process_device_availability(name, payload)
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

    def _process_device_availability(self, device_name: str, payload: str) -> None:
        """Update device availability from zigbee2mqtt/<device>/availability topic."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug("Non-JSON availability payload for %s: %.100s", device_name, payload)
            return
        state = data.get("state")
        if state is None:
            # Z2M may publish null to clear retained availability message
            self._device_availability.pop(device_name, None)
            return
        self._device_availability[device_name] = state

    def get_device_availability(self, device_name: str) -> str | None:
        """Return cached availability for a device, or None if unknown."""
        return self._device_availability.get(device_name)

    def close_log_writer(self) -> None:
        """Close the log file handler (sync-safe for atexit use)."""
        if self._log_writer is not None:
            self._log_writer.close()

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
            if name in self._device_availability:
                enriched["availability"] = self._device_availability[name]
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
                if name in self._device_availability:
                    enriched["availability"] = self._device_availability[name]
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
            with open(self._log_file_path, encoding="utf-8") as f:
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
        except FileNotFoundError:
            return []

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

        Serializes concurrent callers on the same response_topic to prevent
        one caller's Event from overwriting another's.

        Raises:
            TimeoutError: If no response within timeout.
            RuntimeError: If response indicates error.
        """
        # Get or create a per-topic lock to serialize concurrent callers
        if response_topic not in self._response_locks:
            self._response_locks[response_topic] = asyncio.Lock()
        lock = self._response_locks[response_topic]

        async with lock:
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

    def build_ieee_map(self) -> dict[str, str]:
        """Build ieee_address -> friendly_name map from cached devices."""
        result: dict[str, str] = {}
        for dev in self._devices:
            ieee = dev.get("ieee_address")
            name = dev.get("friendly_name")
            if ieee and name:
                result[ieee] = name
        return result

    def build_address_info_map(self) -> dict[int, dict[str, str]]:
        """Build network_address -> {name, type} map from cached devices.

        Best-effort: network addresses are ephemeral. Callers should
        display the raw address alongside the resolved name.
        """
        result: dict[int, dict[str, str]] = {}
        for dev in self._devices:
            addr = dev.get("network_address")
            name = dev.get("friendly_name")
            dev_type = dev.get("type", "Unknown")
            if addr is not None and name:
                result[addr] = {"name": name, "type": dev_type}
        return result

    async def publish(self, topic: str, payload: dict[str, Any] | str) -> None:
        """Publish a message to a topic."""
        if not self._client:
            raise RuntimeError("MQTT client not connected")
        msg = payload if isinstance(payload, str) else json.dumps(payload)
        await self._client.publish(topic, msg)
