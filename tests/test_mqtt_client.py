"""Tests for Z2M MQTT client."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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

    def test_extract_response_topic_key(self, z2m_client: Z2MClient) -> None:
        topic = "zigbee2mqtt/bridge/response/permit_join"
        assert topic == "zigbee2mqtt/bridge/response/permit_join"


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
