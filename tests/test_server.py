"""Tests for MCP server tools."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config import MQTTConfig
from app.mqtt_client import Z2MClient
from app.server import (
    analyze_debug_logs,
    analyze_logs,
    get_bridge_info,
    get_device_health,
    get_device_info,
    get_network_map,
    get_routing_table,
    get_signal_history,
    list_devices,
    list_weak_devices,
    permit_join,
    reconfigure_device,
    rename_device,
    remove_device,
    restart_z2m,
    set_log_level,
)
from tests.conftest import (
    SAMPLE_BRIDGE_INFO,
    SAMPLE_DEBUG_INCOMING_MSG,
    SAMPLE_DEBUG_ROUTE_ERROR,
    SAMPLE_DEBUG_ROUTE_RECORD,
    SAMPLE_DEBUG_UART_NOISE,
    SAMPLE_DEVICES_LIST,
    SAMPLE_DEVICE_END_DEVICE,
    SAMPLE_DEVICE_ROUTER,
)


@pytest.fixture
def z2m(mqtt_config: MQTTConfig) -> Z2MClient:
    """Z2M client populated with sample data."""
    client = Z2MClient(mqtt_config)
    client._process_devices_message(json.dumps(SAMPLE_DEVICES_LIST))
    client._process_bridge_info_message(json.dumps(SAMPLE_BRIDGE_INFO))
    # Add some state data
    client._process_device_state("Living Room Plug", json.dumps({
        "linkquality": 150,
        "last_seen": "2026-02-07T10:00:00Z",
        "state": "ON",
    }))
    client._process_device_state("Kitchen Sensor", json.dumps({
        "linkquality": 30,
        "last_seen": "2026-02-01T10:00:00Z",
        "temperature": 72.5,
        "battery": 15,
    }))
    return client


@pytest.fixture(autouse=True)
def mock_mcp_context(z2m: Z2MClient):
    """Mock the FastMCP context to inject our test clients."""
    mock_ctx = MagicMock()
    mock_ctx.request_context.lifespan_context = {
        "z2m": z2m,
    }

    with patch("app.server.mcp.get_context", return_value=mock_ctx):
        yield


# ---------------------------------------------------------------------------
# Diagnostic Tools
# ---------------------------------------------------------------------------


class TestGetBridgeInfo:
    @pytest.mark.asyncio
    async def test_returns_bridge_info(self) -> None:
        result = await get_bridge_info()

        assert result["version"] == "2.1.1-1"
        assert result["coordinator"]["type"] == "zStack30x"
        assert result["network"]["channel"] == 20
        assert result["network"]["pan_id"] == 6754
        assert result["device_count"] == 3
        assert result["log_level"] == "info"
        assert result["permit_join"] is False

    @pytest.mark.asyncio
    async def test_device_type_counts(self) -> None:
        result = await get_bridge_info()

        assert result["device_types"]["Coordinator"] == 1
        assert result["device_types"]["Router"] == 1
        assert result["device_types"]["EndDevice"] == 1

    @pytest.mark.asyncio
    async def test_no_bridge_info_raises(self, z2m: Z2MClient) -> None:
        z2m._bridge_info = None
        with pytest.raises(RuntimeError, match="not yet available"):
            await get_bridge_info()


class TestListDevices:
    @pytest.mark.asyncio
    async def test_lists_all_devices(self) -> None:
        result = await list_devices()

        assert result["count"] == 3
        assert result["total"] == 3
        assert not result["truncated"]

        names = [d["friendly_name"] for d in result["devices"]]
        assert "Living Room Plug" in names
        assert "Kitchen Sensor" in names
        assert "Coordinator" in names

    @pytest.mark.asyncio
    async def test_filter_by_type(self) -> None:
        result = await list_devices(device_type="Router")

        assert result["count"] == 1
        assert result["devices"][0]["friendly_name"] == "Living Room Plug"

    @pytest.mark.asyncio
    async def test_limit(self) -> None:
        result = await list_devices(limit=1)

        assert result["count"] == 1
        assert result["total"] == 3
        assert result["truncated"]

    @pytest.mark.asyncio
    async def test_includes_lqi_from_state(self) -> None:
        result = await list_devices(device_type="Router")

        device = result["devices"][0]
        assert device["lqi"] == 150


class TestGetDeviceInfo:
    @pytest.mark.asyncio
    async def test_basic_info(self) -> None:
        result = await get_device_info(device="Living Room Plug")

        assert result["friendly_name"] == "Living Room Plug"
        assert result["type"] == "Router"
        assert result["model"] == "SP 224"
        assert result["vendor"] == "Innr"
        assert result["power_source"] == "Mains (single phase)"

    @pytest.mark.asyncio
    async def test_detailed_includes_endpoints(self) -> None:
        result = await get_device_info(device="Living Room Plug", detailed=True)

        assert "endpoints" in result
        assert "1" in result["endpoints"]

    @pytest.mark.asyncio
    async def test_not_detailed_excludes_endpoints(self) -> None:
        result = await get_device_info(device="Living Room Plug", detailed=False)

        assert "endpoints" not in result

    @pytest.mark.asyncio
    async def test_device_not_found(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            await get_device_info(device="nonexistent")

    @pytest.mark.asyncio
    async def test_includes_state(self) -> None:
        result = await get_device_info(device="Living Room Plug")

        assert result["state"]["linkquality"] == 150


class TestGetNetworkMap:
    @pytest.mark.asyncio
    async def test_network_map(self, z2m: Z2MClient) -> None:
        sample_map_response = {
            "status": "ok",
            "data": {
                "value": {
                    "nodes": [
                        {
                            "ieeeAddr": "0x001",
                            "friendlyName": "Coordinator",
                            "type": "Coordinator",
                            "networkAddress": 0,
                            "definition": None,
                        },
                        {
                            "ieeeAddr": "0x002",
                            "friendlyName": "Router1",
                            "type": "Router",
                            "networkAddress": 100,
                            "definition": {"model": "SP 224"},
                        },
                    ],
                    "links": [
                        {
                            "source": {"ieeeAddr": "0x001"},
                            "target": {"ieeeAddr": "0x002"},
                            "linkquality": 200,
                            "depth": 1,
                            "relationship": 1,
                        },
                    ],
                },
            },
        }
        z2m.request_response = AsyncMock(return_value=sample_map_response)

        result = await get_network_map()

        assert result["node_count"] == 2
        assert result["link_count"] == 1
        assert result["nodes"][0]["friendly_name"] == "Coordinator"
        assert result["links"][0]["lqi"] == 200


class TestGetDeviceHealth:
    @pytest.mark.asyncio
    async def test_healthy_device(self) -> None:
        result = await get_device_health(device="Living Room Plug")

        assert result["friendly_name"] == "Living Room Plug"
        assert result["lqi"] == 150
        assert result["lqi_status"] == "good"

    @pytest.mark.asyncio
    async def test_weak_device(self) -> None:
        result = await get_device_health(device="Kitchen Sensor")

        assert result["lqi"] == 30
        assert result["lqi_status"] == "poor"
        assert result["battery"] == 15
        assert result["battery_status"] == "critical"

    @pytest.mark.asyncio
    async def test_device_not_found(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            await get_device_health(device="nonexistent")


class TestListWeakDevices:
    @pytest.mark.asyncio
    async def test_finds_weak_devices(self) -> None:
        result = await list_weak_devices(lqi_threshold=50)

        assert result["count"] >= 1
        names = [d["friendly_name"] for d in result["devices"]]
        assert "Kitchen Sensor" in names

    @pytest.mark.asyncio
    async def test_threshold_adjustment(self) -> None:
        # With high threshold, the router should also appear
        result = await list_weak_devices(lqi_threshold=200)

        names = [d["friendly_name"] for d in result["devices"]]
        assert "Living Room Plug" in names

    @pytest.mark.asyncio
    async def test_excludes_coordinator(self) -> None:
        result = await list_weak_devices(lqi_threshold=999)

        names = [d["friendly_name"] for d in result["devices"]]
        assert "Coordinator" not in names


class TestAnalyzeLogs:
    @pytest.mark.asyncio
    async def test_analyze_logs_empty(self) -> None:
        result = await analyze_logs(minutes_back=30)

        assert result["error_count"] == 0
        assert result["warning_count"] == 0
        assert result["entries"] == []
        assert result["total_in_buffer"] == 0
        assert "note" in result

    @pytest.mark.asyncio
    async def test_analyze_logs_with_buffer_entries(self, z2m: Z2MClient) -> None:
        z2m._process_log_message(json.dumps({"level": "error", "message": "err1"}))
        z2m._process_log_message(json.dumps({"level": "warn", "message": "warn1"}))
        z2m._process_log_message(json.dumps({"level": "info", "message": "info1"}))
        z2m._process_log_message(json.dumps({"level": "error", "message": "err2"}))

        result = await analyze_logs(minutes_back=60)

        assert result["error_count"] == 2
        assert result["warning_count"] == 1
        assert len(result["entries"]) == 4
        assert result["total_in_buffer"] == 4

    @pytest.mark.asyncio
    async def test_analyze_logs_filter_by_level(self, z2m: Z2MClient) -> None:
        z2m._process_log_message(json.dumps({"level": "error", "message": "err1"}))
        z2m._process_log_message(json.dumps({"level": "info", "message": "info1"}))

        result = await analyze_logs(minutes_back=60, level="error")

        assert result["error_count"] == 1
        assert len(result["entries"]) == 1
        assert result["entries"][0]["level"] == "error"

    @pytest.mark.asyncio
    async def test_analyze_logs_merges_file_and_buffer(self, z2m: Z2MClient) -> None:
        """Entries from both file and buffer are included in results."""
        file_entry = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": "error", "message": "from file"}
        buffer_entry_msg = {"level": "warn", "message": "from buffer"}

        z2m.get_logs_from_file = MagicMock(return_value=[file_entry])
        z2m._process_log_message(json.dumps(buffer_entry_msg))

        result = await analyze_logs(minutes_back=60)

        messages = [e["message"] for e in result["entries"]]
        assert "from file" in messages
        assert "from buffer" in messages
        assert result["error_count"] == 1
        assert result["warning_count"] == 1

    @pytest.mark.asyncio
    async def test_analyze_logs_deduplicates(self, z2m: Z2MClient) -> None:
        """Identical entries from file and buffer appear only once."""
        ts = datetime.now(timezone.utc).isoformat()
        entry = {"timestamp": ts, "level": "info", "message": "duplicate msg"}

        # Return the same entry from both sources
        z2m.get_logs_from_file = MagicMock(return_value=[dict(entry)])
        z2m._log_buffer.append(dict(entry))

        result = await analyze_logs(minutes_back=60)

        assert len(result["entries"]) == 1
        assert result["entries"][0]["message"] == "duplicate msg"

    @pytest.mark.asyncio
    async def test_analyze_logs_deduplicates_with_subsecond_skew(self, z2m: Z2MClient) -> None:
        """Entries with same second but different sub-second timestamps are deduped."""
        base = datetime.now(timezone.utc).replace(microsecond=0)
        ts_file = base.isoformat()
        ts_buffer = base.replace(microsecond=500000).isoformat()

        file_entry = {"timestamp": ts_file, "level": "warn", "message": "skewed msg"}
        buffer_entry = {"timestamp": ts_buffer, "level": "warn", "message": "skewed msg"}

        z2m.get_logs_from_file = MagicMock(return_value=[file_entry])
        z2m._log_buffer.append(buffer_entry)

        result = await analyze_logs(minutes_back=60)

        msgs = [e["message"] for e in result["entries"] if e["message"] == "skewed msg"]
        assert len(msgs) == 1


class TestAnalyzeDebugLogs:
    @pytest.mark.asyncio
    async def test_returns_categories(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_ROUTE_RECORD},
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_INCOMING_MSG},
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_UART_NOISE},
        ]
        z2m.get_logs_from_file = MagicMock(return_value=entries)

        result = await analyze_debug_logs(minutes_back=60)

        assert result["total_entries"] == 3
        assert result["categories"]["route_records"] == 1
        assert result["categories"]["incoming_messages"] == 1
        assert result["categories"]["uart_noise"] == 1

    @pytest.mark.asyncio
    async def test_includes_route_errors(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        debug_entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_ROUTE_RECORD},
        ]
        error_entries = [
            {"timestamp": now, "level": "error", "message": SAMPLE_DEBUG_ROUTE_ERROR},
        ]
        z2m.get_logs_from_file = MagicMock(side_effect=[debug_entries, error_entries])

        result = await analyze_debug_logs(minutes_back=60)

        assert result["categories"]["route_errors"] >= 1

    @pytest.mark.asyncio
    async def test_no_debug_entries_returns_hint(self, z2m: Z2MClient) -> None:
        z2m.get_logs_from_file = MagicMock(return_value=[])

        result = await analyze_debug_logs(minutes_back=60)

        assert "log_debug_to_mqtt_frontend" in result["note"]


class TestGetRoutingTable:
    @pytest.mark.asyncio
    async def test_returns_routing_data(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_ROUTE_RECORD},
        ]
        z2m.get_logs_from_file = MagicMock(return_value=entries)

        result = await get_routing_table(minutes_back=60)

        assert "routes" in result
        assert len(result["routes"]) >= 1

    @pytest.mark.asyncio
    async def test_filter_by_device(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_ROUTE_RECORD},
        ]
        z2m.get_logs_from_file = MagicMock(return_value=entries)

        result = await get_routing_table(device="Living Room Plug", minutes_back=60)

        assert len(result["routes"]) == 1
        assert "Living Room Plug" in result["routes"]

    @pytest.mark.asyncio
    async def test_no_debug_entries_returns_hint(self, z2m: Z2MClient) -> None:
        z2m.get_logs_from_file = MagicMock(return_value=[])

        result = await get_routing_table(minutes_back=60)

        assert "log_debug_to_mqtt_frontend" in result["note"]


class TestGetSignalHistory:
    @pytest.mark.asyncio
    async def test_returns_signal_data(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_INCOMING_MSG},
        ]
        z2m.get_logs_from_file = MagicMock(return_value=entries)

        result = await get_signal_history(minutes_back=60)

        assert "devices" in result
        assert len(result["devices"]) >= 1

    @pytest.mark.asyncio
    async def test_filter_by_device(self, z2m: Z2MClient) -> None:
        now = datetime.now(timezone.utc).isoformat()
        entries = [
            {"timestamp": now, "level": "debug", "message": SAMPLE_DEBUG_INCOMING_MSG},
        ]
        z2m.get_logs_from_file = MagicMock(return_value=entries)

        # Incoming messages are now keyed by bare friendly name
        result = await get_signal_history(device="Living Room Plug", minutes_back=60)

        assert len(result["devices"]) == 1
        assert "Living Room Plug" in result["devices"]

    @pytest.mark.asyncio
    async def test_no_debug_entries_returns_hint(self, z2m: Z2MClient) -> None:
        z2m.get_logs_from_file = MagicMock(return_value=[])

        result = await get_signal_history(minutes_back=60)

        assert "log_debug_to_mqtt_frontend" in result["note"]


# ---------------------------------------------------------------------------
# Control Tools
# ---------------------------------------------------------------------------


class TestPermitJoin:
    @pytest.mark.asyncio
    async def test_enable_permit_join(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await permit_join(enable=True, time=120)

        assert result["status"] == "ok"
        z2m.request_response.assert_called_once_with(
            request_topic="zigbee2mqtt/bridge/request/permit_join",
            response_topic="zigbee2mqtt/bridge/response/permit_join",
            payload={"value": True, "time": 120},
        )


class TestReconfigureDevice:
    @pytest.mark.asyncio
    async def test_reconfigure(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await reconfigure_device(device="Living Room Plug")

        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_reconfigure_not_found(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            await reconfigure_device(device="nonexistent")


class TestRenameDevice:
    @pytest.mark.asyncio
    async def test_rename(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await rename_device(old_name="Living Room Plug", new_name="LR Plug")

        assert result["status"] == "ok"
        z2m.request_response.assert_called_once_with(
            request_topic="zigbee2mqtt/bridge/request/device/rename",
            response_topic="zigbee2mqtt/bridge/response/device/rename",
            payload={"from": "Living Room Plug", "to": "LR Plug"},
        )

    @pytest.mark.asyncio
    async def test_rename_not_found(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            await rename_device(old_name="nonexistent", new_name="new_name")


class TestRemoveDevice:
    @pytest.mark.asyncio
    async def test_remove(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await remove_device(device="Living Room Plug", force=False)

        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_remove_force(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await remove_device(device="Living Room Plug", force=True)

        z2m.request_response.assert_called_once_with(
            request_topic="zigbee2mqtt/bridge/request/device/remove",
            response_topic="zigbee2mqtt/bridge/response/device/remove",
            payload={"id": "Living Room Plug", "force": True},
        )

    @pytest.mark.asyncio
    async def test_remove_not_found(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            await remove_device(device="nonexistent")


class TestRestartZ2M:
    @pytest.mark.asyncio
    async def test_restart(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await restart_z2m()

        assert result["status"] == "ok"


class TestSetLogLevel:
    @pytest.mark.asyncio
    async def test_set_valid_level(self, z2m: Z2MClient) -> None:
        z2m.request_response = AsyncMock(return_value={"status": "ok"})

        result = await set_log_level(level="debug")

        assert result["status"] == "ok"
        z2m.request_response.assert_called_once_with(
            request_topic="zigbee2mqtt/bridge/request/options",
            response_topic="zigbee2mqtt/bridge/response/options",
            payload={"options": {"advanced": {"log_level": "debug"}}},
        )

    @pytest.mark.asyncio
    async def test_invalid_level(self) -> None:
        with pytest.raises(ValueError, match="Invalid log level"):
            await set_log_level(level="verbose")
