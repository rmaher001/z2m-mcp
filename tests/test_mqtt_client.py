"""Tests for Z2M MQTT client."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import aiomqtt
import pytest

from app.config import LogConfig, MQTTConfig
from app.mqtt_client import Z2MClient
from tests.conftest import (
    SAMPLE_BRIDGE_INFO,
    SAMPLE_DEVICES_LIST,
    SAMPLE_DEVICE_END_DEVICE,
    SAMPLE_DEVICE_ROUTER,
)


@pytest.fixture
def z2m_client(mqtt_config: MQTTConfig) -> Z2MClient:
    return Z2MClient(mqtt_config)


@pytest.fixture
def z2m_client_with_logs(mqtt_config: MQTTConfig, log_config: LogConfig) -> Z2MClient:
    return Z2MClient(mqtt_config, log_config=log_config)


class TestZ2MClientCache:
    def test_initial_state_empty(self, z2m_client: Z2MClient) -> None:
        assert z2m_client.get_all_devices() == []
        assert z2m_client.get_bridge_info() is None

    def test_process_devices_message(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        devices = z2m_client.get_all_devices()
        assert len(devices) == 3

    def test_process_devices_filters_coordinator(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        # get_all_devices returns all including coordinator
        all_devs = z2m_client.get_all_devices()
        assert any(d["type"] == "Coordinator" for d in all_devs)

    def test_get_device_by_name(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        device = z2m_client.get_device("Living Room Plug")
        assert device is not None
        assert device["friendly_name"] == "Living Room Plug"
        assert device["type"] == "Router"

    def test_get_device_by_ieee(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        device = z2m_client.get_device("0x00158d0009876543")
        assert device is not None
        assert device["friendly_name"] == "Kitchen Sensor"

    def test_get_device_not_found(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))
        assert z2m_client.get_device("nonexistent") is None

    def test_process_bridge_info(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_bridge_info_message(json.dumps(SAMPLE_BRIDGE_INFO))

        info = z2m_client.get_bridge_info()
        assert info is not None
        assert info["version"] == "2.1.1-1"
        assert info["network"]["channel"] == 20

    def test_process_device_state_update(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        # Simulate state update for a device
        state = {"temperature": 72.5, "humidity": 45, "linkquality": 120}
        z2m_client._process_device_state("Living Room Plug", json.dumps(state))

        device = z2m_client.get_device("Living Room Plug")
        assert device is not None
        assert device.get("state", {}).get("linkquality") == 120
        assert device.get("state", {}).get("temperature") == 72.5


class TestZ2MClientRequestResponse:
    @pytest.mark.asyncio
    async def test_request_response_success(self, z2m_client: Z2MClient) -> None:
        response_data = {"status": "ok", "data": {"permit_join": True}}

        # Simulate the response arriving
        async def simulate_response() -> None:
            await asyncio.sleep(0.05)
            topic = "zigbee2mqtt/bridge/response/permit_join"
            z2m_client._process_response(topic, json.dumps(response_data))

        asyncio.create_task(simulate_response())

        result = await z2m_client.request_response(
            request_topic="zigbee2mqtt/bridge/request/permit_join",
            response_topic="zigbee2mqtt/bridge/response/permit_join",
            payload={"value": True, "time": 120},
            timeout=2.0,
        )

        assert result == response_data

    @pytest.mark.asyncio
    async def test_request_response_timeout(self, z2m_client: Z2MClient) -> None:
        with pytest.raises(TimeoutError, match="No response"):
            await z2m_client.request_response(
                request_topic="zigbee2mqtt/bridge/request/permit_join",
                response_topic="zigbee2mqtt/bridge/response/permit_join",
                payload={"value": True},
                timeout=0.1,
            )

    @pytest.mark.asyncio
    async def test_request_response_error_status(self, z2m_client: Z2MClient) -> None:
        response_data = {"status": "error", "error": "Device not found"}

        async def simulate_response() -> None:
            await asyncio.sleep(0.05)
            topic = "zigbee2mqtt/bridge/response/permit_join"
            z2m_client._process_response(topic, json.dumps(response_data))

        asyncio.create_task(simulate_response())

        with pytest.raises(RuntimeError, match="Device not found"):
            await z2m_client.request_response(
                request_topic="zigbee2mqtt/bridge/request/permit_join",
                response_topic="zigbee2mqtt/bridge/response/permit_join",
                payload={"value": True},
                timeout=2.0,
            )


class TestZ2MClientTopicParsing:
    def test_extract_device_name_from_topic(self, z2m_client: Z2MClient) -> None:
        assert z2m_client._device_name_from_topic("zigbee2mqtt/Living Room Plug") == "Living Room Plug"

    def test_extract_device_name_ignores_bridge(self, z2m_client: Z2MClient) -> None:
        assert z2m_client._device_name_from_topic("zigbee2mqtt/bridge/info") is None
        assert z2m_client._device_name_from_topic("zigbee2mqtt/bridge/devices") is None


class TestZ2MClientLogCapture:
    def test_process_log_message_populates_buffer(self, z2m_client: Z2MClient) -> None:
        payload = json.dumps({"level": "error", "message": "Failed to configure device"})
        z2m_client._process_log_message(payload)

        assert len(z2m_client._log_buffer) == 1
        entry = z2m_client._log_buffer[0]
        assert entry["level"] == "error"
        assert entry["message"] == "Failed to configure device"
        assert "timestamp" in entry

    def test_process_log_message_multiple(self, z2m_client: Z2MClient) -> None:
        for i in range(5):
            payload = json.dumps({"level": "info", "message": f"Message {i}"})
            z2m_client._process_log_message(payload)

        assert len(z2m_client._log_buffer) == 5

    def test_buffer_respects_maxlen(self, z2m_client: Z2MClient) -> None:
        # Fill beyond maxlen
        for i in range(1100):
            payload = json.dumps({"level": "info", "message": f"Message {i}"})
            z2m_client._process_log_message(payload)

        assert len(z2m_client._log_buffer) == 1000
        # Oldest entries dropped -- buffer should contain messages 100-1099
        assert z2m_client._log_buffer[0]["message"] == "Message 100"
        assert z2m_client._log_buffer[-1]["message"] == "Message 1099"

    def test_process_log_message_non_json_ignored(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_log_message("not valid json")
        assert len(z2m_client._log_buffer) == 0

    def test_get_logs_filters_by_level(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_log_message(json.dumps({"level": "error", "message": "err1"}))
        z2m_client._process_log_message(json.dumps({"level": "info", "message": "info1"}))
        z2m_client._process_log_message(json.dumps({"level": "warn", "message": "warn1"}))
        z2m_client._process_log_message(json.dumps({"level": "error", "message": "err2"}))

        errors = z2m_client.get_logs(minutes_back=60, level="error")
        assert len(errors) == 2
        assert all(e["level"] == "error" for e in errors)

    def test_get_logs_filters_by_time(self, z2m_client: Z2MClient) -> None:
        # Insert an old entry directly
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        z2m_client._log_buffer.append(
            {"timestamp": old_ts, "level": "error", "message": "old error"},
        )
        # Insert a recent entry
        z2m_client._process_log_message(json.dumps({"level": "error", "message": "new error"}))

        results = z2m_client.get_logs(minutes_back=60)
        assert len(results) == 1
        assert results[0]["message"] == "new error"

    def test_get_logs_all_recent(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_log_message(json.dumps({"level": "info", "message": "msg1"}))
        z2m_client._process_log_message(json.dumps({"level": "warn", "message": "msg2"}))

        results = z2m_client.get_logs(minutes_back=60)
        assert len(results) == 2

    def test_route_message_logging_topic(self, z2m_client: Z2MClient) -> None:
        payload = json.dumps({"level": "warn", "message": "Device not responding"})
        z2m_client._route_message("zigbee2mqtt/bridge/logging", payload)

        assert len(z2m_client._log_buffer) == 1
        assert z2m_client._log_buffer[0]["level"] == "warn"

    def test_get_log_file_path_none_without_config(self, z2m_client: Z2MClient) -> None:
        assert z2m_client.get_log_file_path() is None


class TestGetLogsFromFile:
    def test_reads_from_jsonl_file(self, z2m_client_with_logs: Z2MClient) -> None:
        """Write entries via _process_log_message, flush, read back with get_logs_from_file."""
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "error", "message": "file error 1"})
        )
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "info", "message": "file info 1"})
        )
        z2m_client_with_logs._log_writer.flush()

        results = z2m_client_with_logs.get_logs_from_file(minutes_back=60)
        assert len(results) == 2
        messages = [r["message"] for r in results]
        assert "file error 1" in messages
        assert "file info 1" in messages

    def test_filters_by_time(self, z2m_client_with_logs: Z2MClient) -> None:
        """Old entries excluded by minutes_back filter."""
        # Write an old entry directly to the file
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        old_line = json.dumps({"timestamp": old_ts, "level": "error", "message": "old"})
        record = logging.LogRecord(
            name="z2m", level=logging.INFO, pathname="", lineno=0,
            msg=old_line, args=(), exc_info=None,
        )
        z2m_client_with_logs._log_writer.emit(record)

        # Write a recent entry via normal path
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "info", "message": "recent"})
        )
        z2m_client_with_logs._log_writer.flush()

        results = z2m_client_with_logs.get_logs_from_file(minutes_back=60)
        assert len(results) == 1
        assert results[0]["message"] == "recent"

    def test_filters_by_level(self, z2m_client_with_logs: Z2MClient) -> None:
        """Level filtering works on file-based entries."""
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "error", "message": "err"})
        )
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "info", "message": "inf"})
        )
        z2m_client_with_logs._log_writer.flush()

        results = z2m_client_with_logs.get_logs_from_file(minutes_back=60, level="error")
        assert len(results) == 1
        assert results[0]["level"] == "error"

    def test_missing_file_returns_empty(self, z2m_client_with_logs: Z2MClient) -> None:
        """No crash when the JSONL file doesn't exist yet."""
        # Point to a file that doesn't exist
        z2m_client_with_logs._log_file_path = "/tmp/nonexistent_z2m_test.jsonl"
        results = z2m_client_with_logs.get_logs_from_file(minutes_back=60)
        assert results == []

    def test_no_log_config_returns_empty(self, z2m_client: Z2MClient) -> None:
        """Client without log config returns empty list."""
        results = z2m_client.get_logs_from_file(minutes_back=60)
        assert results == []

    def test_malformed_lines_skipped(self, z2m_client_with_logs: Z2MClient) -> None:
        """Corrupt JSON lines are silently skipped."""
        # Write a valid entry
        z2m_client_with_logs._process_log_message(
            json.dumps({"level": "info", "message": "valid"})
        )
        z2m_client_with_logs._log_writer.flush()

        # Append a malformed line directly to the file
        log_path = z2m_client_with_logs.get_log_file_path()
        with open(log_path, "a") as f:
            f.write("this is not valid json\n")
            f.write('{"timestamp": "bad-ts", "level": "info", "message": "bad ts"}\n')

        results = z2m_client_with_logs.get_logs_from_file(minutes_back=60)
        assert len(results) == 1
        assert results[0]["message"] == "valid"


class TestZ2MClientLogPersistence:
    def test_jsonl_file_written(self, z2m_client_with_logs: Z2MClient) -> None:
        payload = json.dumps({"level": "error", "message": "test error"})
        z2m_client_with_logs._process_log_message(payload)

        # Flush the handler
        z2m_client_with_logs._log_writer.flush()

        log_path = z2m_client_with_logs.get_log_file_path()
        assert log_path is not None
        assert os.path.exists(log_path)

        with open(log_path) as f:
            lines = f.readlines()

        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["level"] == "error"
        assert parsed["message"] == "test error"
        assert "timestamp" in parsed

    def test_jsonl_multiple_entries(self, z2m_client_with_logs: Z2MClient) -> None:
        for i in range(3):
            payload = json.dumps({"level": "info", "message": f"entry {i}"})
            z2m_client_with_logs._process_log_message(payload)

        z2m_client_with_logs._log_writer.flush()

        log_path = z2m_client_with_logs.get_log_file_path()
        with open(log_path) as f:
            lines = f.readlines()

        assert len(lines) == 3

    def test_get_log_file_path_with_config(self, z2m_client_with_logs: Z2MClient) -> None:
        path = z2m_client_with_logs.get_log_file_path()
        assert path is not None
        assert path.endswith("z2m.jsonl")


class TestBuildIeeeMap:
    def test_build_ieee_map(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        ieee_map = z2m_client.build_ieee_map()

        assert ieee_map["0x00158d0001234567"] == "Living Room Plug"
        assert ieee_map["0x00158d0009876543"] == "Kitchen Sensor"
        assert ieee_map["0x00124b002345abcd"] == "Coordinator"

    def test_build_ieee_map_empty(self, z2m_client: Z2MClient) -> None:
        ieee_map = z2m_client.build_ieee_map()
        assert ieee_map == {}

    def test_build_ieee_map_skips_missing_fields(self, z2m_client: Z2MClient) -> None:
        """Devices without ieee_address are skipped."""
        devices = [{"friendly_name": "NoIEEE", "network_address": 123}]
        z2m_client._process_devices_message(json.dumps(devices))

        ieee_map = z2m_client.build_ieee_map()
        assert ieee_map == {}


class TestBuildAddressInfoMap:
    def test_build_address_info_map(self, z2m_client: Z2MClient) -> None:
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        addr_info = z2m_client.build_address_info_map()

        assert addr_info[12345] == {"name": "Living Room Plug", "type": "Router"}
        assert addr_info[54321] == {"name": "Kitchen Sensor", "type": "EndDevice"}
        assert addr_info[0] == {"name": "Coordinator", "type": "Coordinator"}

    def test_build_address_info_map_empty(self, z2m_client: Z2MClient) -> None:
        addr_info = z2m_client.build_address_info_map()
        assert addr_info == {}

    def test_build_address_info_map_skips_missing_fields(self, z2m_client: Z2MClient) -> None:
        """Devices without network_address are skipped."""
        devices = [{"friendly_name": "NoAddr", "type": "Router"}]
        z2m_client._process_devices_message(json.dumps(devices))

        addr_info = z2m_client.build_address_info_map()
        assert addr_info == {}

    def test_build_address_info_map_skips_missing_name(self, z2m_client: Z2MClient) -> None:
        """Devices without friendly_name are skipped."""
        devices = [{"network_address": 100, "type": "Router"}]
        z2m_client._process_devices_message(json.dumps(devices))

        addr_info = z2m_client.build_address_info_map()
        assert addr_info == {}

    def test_build_address_info_map_default_type(self, z2m_client: Z2MClient) -> None:
        """Devices without type get 'Unknown' as default."""
        devices = [{"friendly_name": "NoType", "network_address": 999}]
        z2m_client._process_devices_message(json.dumps(devices))

        addr_info = z2m_client.build_address_info_map()
        assert addr_info[999] == {"name": "NoType", "type": "Unknown"}


class TestDeviceAvailability:
    def test_initial_availability_empty(self, z2m_client: Z2MClient) -> None:
        """Availability cache is empty on init."""
        assert z2m_client._device_availability == {}

    def test_process_availability_online(self, z2m_client: Z2MClient) -> None:
        """Routing an online availability message stores 'online'."""
        z2m_client._route_message(
            "zigbee2mqtt/Living Room Plug/availability",
            json.dumps({"state": "online"}),
        )
        assert z2m_client._device_availability["Living Room Plug"] == "online"

    def test_process_availability_offline(self, z2m_client: Z2MClient) -> None:
        """Routing an offline availability message stores 'offline'."""
        z2m_client._route_message(
            "zigbee2mqtt/Living Room Plug/availability",
            json.dumps({"state": "offline"}),
        )
        assert z2m_client._device_availability["Living Room Plug"] == "offline"

    def test_get_device_availability_returns_cached(self, z2m_client: Z2MClient) -> None:
        """get_device_availability returns the cached value."""
        z2m_client._route_message(
            "zigbee2mqtt/Living Room Plug/availability",
            json.dumps({"state": "online"}),
        )
        assert z2m_client.get_device_availability("Living Room Plug") == "online"

    def test_get_device_availability_unknown_device(self, z2m_client: Z2MClient) -> None:
        """get_device_availability returns None for uncached device."""
        assert z2m_client.get_device_availability("Unknown Device") is None

    def test_route_message_availability_topic(self, z2m_client: Z2MClient) -> None:
        """Availability topic routes to availability handler, not device state."""
        z2m_client._route_message(
            "zigbee2mqtt/Device/availability",
            json.dumps({"state": "online"}),
        )
        assert z2m_client._device_availability.get("Device") == "online"

    def test_device_state_not_polluted_by_availability(self, z2m_client: Z2MClient) -> None:
        """Availability messages do not create entries in _device_states."""
        z2m_client._route_message(
            "zigbee2mqtt/Living Room Plug/availability",
            json.dumps({"state": "online"}),
        )
        assert "Living Room Plug/availability" not in z2m_client._device_states
        assert "Living Room Plug" not in z2m_client._device_states

    def test_availability_non_json_ignored(self, z2m_client: Z2MClient) -> None:
        """Non-JSON availability payload is silently ignored."""
        z2m_client._route_message(
            "zigbee2mqtt/Device/availability",
            "not valid json",
        )
        assert z2m_client._device_availability == {}

    def test_null_state_evicts_from_cache(self, z2m_client: Z2MClient) -> None:
        """Payload with state: null clears cached availability."""
        z2m_client._device_availability["Device"] = "online"
        z2m_client._route_message(
            "zigbee2mqtt/Device/availability",
            json.dumps({"state": None}),
        )
        assert "Device" not in z2m_client._device_availability

    def test_multi_segment_name_rejected(self, z2m_client: Z2MClient) -> None:
        """Topics with multi-segment device names don't route to availability."""
        z2m_client._route_message(
            "zigbee2mqtt/some/deep/availability",
            json.dumps({"state": "online"}),
        )
        assert z2m_client._device_availability == {}

    def test_get_all_devices_includes_availability(self, z2m_client: Z2MClient) -> None:
        """get_all_devices enriches devices with cached availability."""
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))
        z2m_client._device_availability["Living Room Plug"] = "online"

        devices = z2m_client.get_all_devices()
        plug = next(d for d in devices if d["friendly_name"] == "Living Room Plug")
        assert plug["availability"] == "online"

    def test_get_device_includes_availability(self, z2m_client: Z2MClient) -> None:
        """get_device enriches device with cached availability."""
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))
        z2m_client._device_availability["Living Room Plug"] = "offline"

        device = z2m_client.get_device("Living Room Plug")
        assert device is not None
        assert device["availability"] == "offline"

    def test_get_device_no_availability_key_when_absent(self, z2m_client: Z2MClient) -> None:
        """Devices without cached availability don't have the key."""
        z2m_client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))

        device = z2m_client.get_device("Kitchen Sensor")
        assert device is not None
        assert "availability" not in device


class TestCleanupOldLogs:
    def test_deletes_files_older_than_retention(self, tmp_path: os.PathLike, mqtt_config: MQTTConfig) -> None:
        """Files with mtime older than retention_days are deleted."""
        log_dir = str(tmp_path / "logs")
        os.makedirs(log_dir)

        # Create a file and backdate its mtime to 10 days ago
        old_file = os.path.join(log_dir, "z2m.jsonl.1")
        with open(old_file, "w") as f:
            f.write("old data\n")
        old_mtime = time.time() - (10 * 86400)
        os.utime(old_file, (old_mtime, old_mtime))

        # Create a recent file
        recent_file = os.path.join(log_dir, "z2m.jsonl.2")
        with open(recent_file, "w") as f:
            f.write("recent data\n")

        config = LogConfig(
            dir=log_dir, max_size_mb=10, backup_count=3,
            retention_days=7, max_total_mb=100,
        )
        Z2MClient(mqtt_config, log_config=config)

        assert not os.path.exists(old_file)
        assert os.path.exists(recent_file)

    def test_enforces_max_total_mb(self, tmp_path: os.PathLike, mqtt_config: MQTTConfig) -> None:
        """Oldest files deleted when total exceeds max_total_mb."""
        log_dir = str(tmp_path / "logs")
        os.makedirs(log_dir)

        # Create two files, each ~60KB, with a cap of 0.1 MB (~100KB)
        for i, age_seconds in enumerate([200, 100]):
            path = os.path.join(log_dir, f"z2m.jsonl.{i}")
            with open(path, "w") as f:
                f.write("x" * 60_000 + "\n")
            mtime = time.time() - age_seconds
            os.utime(path, (mtime, mtime))

        config = LogConfig(
            dir=log_dir, max_size_mb=10, backup_count=3,
            retention_days=30, max_total_mb=0,  # 0 MB cap → delete all
        )
        Z2MClient(mqtt_config, log_config=config)

        # Both files should be deleted (total > 0 MB)
        remaining = [f for f in os.listdir(log_dir) if f.startswith("z2m.jsonl")]
        # The main z2m.jsonl may be created by RotatingFileHandler, but the .0 and .1 are gone
        assert not os.path.exists(os.path.join(log_dir, "z2m.jsonl.0"))
        assert not os.path.exists(os.path.join(log_dir, "z2m.jsonl.1"))

    def test_no_crash_on_empty_dir(self, tmp_path: os.PathLike, mqtt_config: MQTTConfig) -> None:
        """No error when log directory has no matching files."""
        log_dir = str(tmp_path / "logs")
        config = LogConfig(
            dir=log_dir, max_size_mb=10, backup_count=3,
            retention_days=7, max_total_mb=100,
        )
        # Should not raise
        client = Z2MClient(mqtt_config, log_config=config)
        assert client.get_log_file_path() is not None

    def test_keeps_files_within_retention(self, tmp_path: os.PathLike, mqtt_config: MQTTConfig) -> None:
        """Recent files within retention are kept."""
        log_dir = str(tmp_path / "logs")
        os.makedirs(log_dir)

        recent_file = os.path.join(log_dir, "z2m.jsonl.1")
        with open(recent_file, "w") as f:
            f.write("data\n")

        config = LogConfig(
            dir=log_dir, max_size_mb=10, backup_count=3,
            retention_days=7, max_total_mb=100,
        )
        Z2MClient(mqtt_config, log_config=config)

        assert os.path.exists(recent_file)

    def test_size_cap_deletes_oldest_first(self, tmp_path: os.PathLike, mqtt_config: MQTTConfig) -> None:
        """With a realistic cap, only the oldest files are deleted to fit under the limit."""
        log_dir = str(tmp_path / "logs")
        os.makedirs(log_dir)

        # Create 3 files: oldest (60KB), middle (60KB), newest (60KB)
        # Total ~180KB, cap at 1MB (enough) vs cap we set to trigger partial deletion
        files = []
        for i, age_seconds in enumerate([300, 200, 100]):
            path = os.path.join(log_dir, f"z2m.jsonl.{i}")
            with open(path, "w") as f:
                f.write("x" * 60_000 + "\n")
            mtime = time.time() - age_seconds
            os.utime(path, (mtime, mtime))
            files.append(path)

        # Cap at ~100KB — only the newest file should survive (oldest two deleted)
        # 60KB * 3 = 180KB total, need to delete oldest until <= ~100KB
        # After deleting .0 (oldest): 120KB > 100KB → delete .1 too: 60KB <= 100KB
        config = LogConfig(
            dir=log_dir, max_size_mb=10, backup_count=3,
            retention_days=30, max_total_mb=1,  # 1MB cap — all fit
        )
        Z2MClient(mqtt_config, log_config=config)

        # All 3 should survive under 1MB cap
        assert os.path.exists(files[0])
        assert os.path.exists(files[1])
        assert os.path.exists(files[2])


class _ReconnectFakeClient:
    """Fake aiomqtt.Client whose first connection drops, then stays up.

    Simulates a broker connection that succeeds, is lost (raises MqttError
    the way aiomqtt does when the socket dies mid-stream), and is then
    re-established. Lets us assert the client reconnects and RE-SUBSCRIBES
    rather than freezing its caches forever — the real-world failure that
    left the running container serving 10h-stale data with an up process.

    Class-level counters aggregate across every (re)connection so the test
    can count how many times the bridge topic was subscribed.
    """

    connect_count = 0
    subscribe_topics: list[str] = []

    def __init__(self, **kwargs: object) -> None:
        self._kwargs = kwargs
        self._my_connect = 0

    async def __aenter__(self) -> "_ReconnectFakeClient":
        type(self).connect_count += 1
        self._my_connect = type(self).connect_count
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def subscribe(self, topic: str, *args: object, **kwargs: object) -> None:
        type(self).subscribe_topics.append(topic)

    async def publish(self, *args: object, **kwargs: object) -> None:
        return None

    @property
    def messages(self):
        return self._message_stream()

    async def _message_stream(self):
        if self._my_connect == 1:
            # First connection drops immediately, the way a keepalive
            # timeout or broker restart surfaces in aiomqtt.
            raise aiomqtt.MqttError("simulated connection loss")
        # Later connections stay up until the listener task is cancelled.
        await asyncio.sleep(3600)
        return
        yield  # pragma: no cover — marks this an async generator


class TestZ2MClientReconnect:
    @pytest.mark.asyncio
    async def test_reconnects_and_resubscribes_after_connection_loss(
        self, mqtt_config: MQTTConfig
    ) -> None:
        _ReconnectFakeClient.connect_count = 0
        _ReconnectFakeClient.subscribe_topics = []

        client = Z2MClient(mqtt_config, reconnect_interval=0.01, settle_delay=0.0)
        bridge_topic = "zigbee2mqtt/bridge/#"

        with patch("app.mqtt_client.aiomqtt.Client", _ReconnectFakeClient):
            await client.start()
            try:
                # Bounded wait for the reconnect to re-subscribe.
                for _ in range(200):
                    if _ReconnectFakeClient.subscribe_topics.count(bridge_topic) >= 2:
                        break
                    await asyncio.sleep(0.01)
            finally:
                await client.stop()

        # Subscribed once on the initial connect and again after reconnect.
        assert _ReconnectFakeClient.connect_count >= 2
        assert _ReconnectFakeClient.subscribe_topics.count(bridge_topic) >= 2


class _FailThenConnectFakeClient:
    """Fake whose first connect fails, then succeeds and stays up.

    Models a broker that is down when the MCP server boots but comes back
    shortly after. start() must NOT raise (that would take the whole server
    down); it should return and keep retrying in the background.
    """

    connect_attempts = 0

    def __init__(self, **kwargs: object) -> None:
        self._kwargs = kwargs

    async def __aenter__(self) -> "_FailThenConnectFakeClient":
        type(self).connect_attempts += 1
        if type(self).connect_attempts == 1:
            raise aiomqtt.MqttError("broker unreachable at startup")
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def subscribe(self, topic: str, *args: object, **kwargs: object) -> None:
        return None

    @property
    def messages(self):
        return self._message_stream()

    async def _message_stream(self):
        await asyncio.sleep(3600)
        return
        yield  # pragma: no cover — marks this an async generator


class TestZ2MClientStartupResilience:
    @pytest.mark.asyncio
    async def test_start_does_not_raise_when_broker_unreachable(
        self, mqtt_config: MQTTConfig
    ) -> None:
        _FailThenConnectFakeClient.connect_attempts = 0

        client = Z2MClient(
            mqtt_config,
            reconnect_interval=0.01,
            connect_timeout=2.0,
            settle_delay=0.0,
        )

        with patch("app.mqtt_client.aiomqtt.Client", _FailThenConnectFakeClient):
            # Must return normally despite the failed first connection.
            await client.start()
            try:
                for _ in range(200):
                    if _FailThenConnectFakeClient.connect_attempts >= 2:
                        break
                    await asyncio.sleep(0.01)
            finally:
                await client.stop()

        # Retried past the initial failure and connected.
        assert _FailThenConnectFakeClient.connect_attempts >= 2


class _UnexpectedErrorThenConnectFakeClient:
    """First connection raises a NON-MqttError, then stays up.

    Guards against an unexpected exception type silently killing the reconnect
    loop — which would re-freeze the caches via a different trigger than the
    original bug. The loop must recover from *any* exception, not just
    aiomqtt.MqttError.
    """

    connect_count = 0

    def __init__(self, **kwargs: object) -> None:
        self._my_connect = 0

    async def __aenter__(self) -> "_UnexpectedErrorThenConnectFakeClient":
        type(self).connect_count += 1
        self._my_connect = type(self).connect_count
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def subscribe(self, topic: str, *args: object, **kwargs: object) -> None:
        return None

    @property
    def messages(self):
        return self._message_stream()

    async def _message_stream(self):
        if self._my_connect == 1:
            raise RuntimeError("unexpected non-MqttError failure")
        await asyncio.sleep(3600)
        return
        yield  # pragma: no cover — marks this an async generator


class TestZ2MClientReconnectOnUnexpectedError:
    @pytest.mark.asyncio
    async def test_reconnects_after_unexpected_exception(
        self, mqtt_config: MQTTConfig
    ) -> None:
        _UnexpectedErrorThenConnectFakeClient.connect_count = 0

        client = Z2MClient(mqtt_config, reconnect_interval=0.01, settle_delay=0.0)

        with patch(
            "app.mqtt_client.aiomqtt.Client", _UnexpectedErrorThenConnectFakeClient
        ):
            await client.start()
            try:
                for _ in range(200):
                    if _UnexpectedErrorThenConnectFakeClient.connect_count >= 2:
                        break
                    await asyncio.sleep(0.01)
            finally:
                await client.stop()

        # Recovered from the unexpected exception and reconnected.
        assert _UnexpectedErrorThenConnectFakeClient.connect_count >= 2


class _AlwaysDownFakeClient:
    """Every connection attempt fails — models a broker that stays down."""

    connect_attempts = 0

    def __init__(self, **kwargs: object) -> None:
        pass

    async def __aenter__(self) -> "_AlwaysDownFakeClient":
        type(self).connect_attempts += 1
        raise aiomqtt.MqttError("broker stays down")

    async def __aexit__(self, *exc: object) -> bool:
        return False


class TestZ2MClientStartupTimeout:
    @pytest.mark.asyncio
    async def test_start_returns_at_timeout_and_keeps_retrying(
        self, mqtt_config: MQTTConfig
    ) -> None:
        _AlwaysDownFakeClient.connect_attempts = 0

        client = Z2MClient(
            mqtt_config,
            reconnect_interval=0.02,
            connect_timeout=0.1,
            settle_delay=0.0,
        )

        with patch("app.mqtt_client.aiomqtt.Client", _AlwaysDownFakeClient):
            # Broker never comes up: start() must return via the connect_timeout
            # branch WITHOUT raising, rather than blocking forever.
            await client.start()
            attempts_at_return = _AlwaysDownFakeClient.connect_attempts
            try:
                # The background task keeps retrying after start() returned.
                for _ in range(200):
                    if _AlwaysDownFakeClient.connect_attempts > attempts_at_return:
                        break
                    await asyncio.sleep(0.01)
            finally:
                await client.stop()

        assert _AlwaysDownFakeClient.connect_attempts > attempts_at_return
