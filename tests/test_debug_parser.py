"""Tests for debug log parser."""

from __future__ import annotations

from app.debug_parser import (
    IncomingMessage,
    RouteError,
    RouteRecord,
    _format_relay,
    aggregate_routes,
    aggregate_signal_stats,
    analyze_debug_entries,
    parse_incoming_message,
    parse_route_error,
    parse_route_record,
)
from tests.conftest import (
    SAMPLE_DEBUG_INCOMING_MSG,
    SAMPLE_DEBUG_ROUTE_ERROR,
    SAMPLE_DEBUG_ROUTE_RECORD,
    SAMPLE_DEBUG_ROUTE_RECORD_DIRECT,
    SAMPLE_DEBUG_UART_NOISE,
)


# ---------------------------------------------------------------------------
# Maps used across tests
# ---------------------------------------------------------------------------

IEEE_MAP = {
    "0x00158d0001234567": "Living Room Plug",
    "0x00158d0009876543": "Kitchen Sensor",
}

ADDR_INFO: dict[int, dict[str, str]] = {
    100: {"name": "Hallway Repeater", "type": "Router"},
    200: {"name": "Garage Plug", "type": "Router"},
    12345: {"name": "Living Room Plug", "type": "Router"},
    54321: {"name": "Kitchen Sensor", "type": "EndDevice"},
    9814: {"name": "master_bedroom_pir", "type": "EndDevice"},
}


# ---------------------------------------------------------------------------
# parse_route_record
# ---------------------------------------------------------------------------


class TestParseRouteRecord:
    def test_parses_relayed_route(self) -> None:
        rec = parse_route_record("2026-02-08T10:00:00Z", SAMPLE_DEBUG_ROUTE_RECORD)

        assert rec is not None
        assert rec.source == 12345
        assert rec.source_eui == "0x00158d0001234567"
        assert rec.last_hop_lqi == 180
        assert rec.last_hop_rssi == -45
        assert rec.relay_count == 2
        assert rec.relay_list == [100, 200]
        assert rec.timestamp == "2026-02-08T10:00:00Z"

    def test_parses_direct_route(self) -> None:
        rec = parse_route_record("2026-02-08T10:00:00Z", SAMPLE_DEBUG_ROUTE_RECORD_DIRECT)

        assert rec is not None
        assert rec.source == 54321
        assert rec.relay_count == 0
        assert rec.relay_list == []

    def test_returns_none_for_non_route_record(self) -> None:
        assert parse_route_record("ts", "some random log message") is None

    def test_returns_none_for_uart_noise(self) -> None:
        assert parse_route_record("ts", SAMPLE_DEBUG_UART_NOISE) is None

    def test_returns_none_for_incoming_message(self) -> None:
        assert parse_route_record("ts", SAMPLE_DEBUG_INCOMING_MSG) is None


# ---------------------------------------------------------------------------
# parse_incoming_message
# ---------------------------------------------------------------------------


class TestParseIncomingMessage:
    def test_parses_incoming_message(self) -> None:
        msg = parse_incoming_message("2026-02-08T10:00:00Z", SAMPLE_DEBUG_INCOMING_MSG)

        assert msg is not None
        assert msg.sender_short_id == 12345
        assert msg.last_hop_lqi == 155
        assert msg.last_hop_rssi == -50
        assert msg.timestamp == "2026-02-08T10:00:00Z"

    def test_returns_none_for_non_incoming(self) -> None:
        assert parse_incoming_message("ts", "random log") is None

    def test_returns_none_for_route_record(self) -> None:
        assert parse_incoming_message("ts", SAMPLE_DEBUG_ROUTE_RECORD) is None


# ---------------------------------------------------------------------------
# parse_route_error
# ---------------------------------------------------------------------------


class TestParseRouteError:
    def test_parses_route_error(self) -> None:
        err = parse_route_error("2026-02-08T10:00:00Z", SAMPLE_DEBUG_ROUTE_ERROR)

        assert err is not None
        assert err.error_type == "routeDiscoveryFailed"
        assert err.address == "0x00158d0001234567"
        assert err.timestamp == "2026-02-08T10:00:00Z"

    def test_returns_none_for_non_error(self) -> None:
        assert parse_route_error("ts", "some other log") is None

    def test_parses_different_error_type(self) -> None:
        msg = 'Received network/route error manyToOneRouteFailure for "0xaabbccdd"'
        err = parse_route_error("ts", msg)

        assert err is not None
        assert err.error_type == "manyToOneRouteFailure"
        assert err.address == "0xaabbccdd"


# ---------------------------------------------------------------------------
# _format_relay
# ---------------------------------------------------------------------------


class TestFormatRelay:
    def test_resolved_router(self) -> None:
        assert _format_relay(100, ADDR_INFO) == "Hallway Repeater [addr:100, Router]"

    def test_resolved_end_device(self) -> None:
        result = _format_relay(9814, ADDR_INFO)
        assert result == "master_bedroom_pir [addr:9814, EndDevice — likely reassigned]"

    def test_unresolved_address(self) -> None:
        assert _format_relay(88888, ADDR_INFO) == "[addr:88888]"

    def test_empty_addr_info(self) -> None:
        assert _format_relay(100, {}) == "[addr:100]"


# ---------------------------------------------------------------------------
# aggregate_routes
# ---------------------------------------------------------------------------


class TestAggregateRoutes:
    def test_single_device_single_path(self) -> None:
        records = [
            RouteRecord(
                timestamp="2026-02-08T10:00:00Z",
                source=12345,
                source_eui="0x00158d0001234567",
                last_hop_lqi=180,
                last_hop_rssi=-45,
                relay_count=2,
                relay_list=[100, 200],
            ),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        assert "Living Room Plug" in result
        dev = result["Living Room Plug"]
        assert dev["path_change_count"] == 0
        assert len(dev["observed_paths"]) == 1
        path_key = "Hallway Repeater [addr:100, Router] -> Garage Plug [addr:200, Router]"
        assert path_key in dev["observed_paths"]
        assert dev["observed_paths"][path_key] == 1
        assert dev["current_path"] == [
            "Hallway Repeater [addr:100, Router]",
            "Garage Plug [addr:200, Router]",
        ]

    def test_direct_device(self) -> None:
        records = [
            RouteRecord(
                timestamp="2026-02-08T10:00:00Z",
                source=54321,
                source_eui="0x00158d0009876543",
                last_hop_lqi=120,
                last_hop_rssi=-60,
                relay_count=0,
                relay_list=[],
            ),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        assert "Kitchen Sensor" in result
        dev = result["Kitchen Sensor"]
        assert dev["current_path"] == ["(direct)"]
        assert "(direct)" in dev["observed_paths"]

    def test_path_changes_counted(self) -> None:
        records = [
            RouteRecord("t1", 12345, "0x00158d0001234567", 180, -45, 1, [100]),
            RouteRecord("t2", 12345, "0x00158d0001234567", 170, -50, 1, [200]),
            RouteRecord("t3", 12345, "0x00158d0001234567", 180, -45, 1, [100]),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        dev = result["Living Room Plug"]
        assert dev["path_change_count"] == 2
        assert len(dev["observed_paths"]) == 2

    def test_unknown_relay_uses_raw_addr(self) -> None:
        records = [
            RouteRecord("t1", 99999, "0xunknown", 100, -70, 1, [88888]),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        assert "0xunknown" in result
        dev = result["0xunknown"]
        assert dev["current_path"] == ["[addr:88888]"]

    def test_end_device_relay_shows_reassigned(self) -> None:
        """EndDevice in relay path gets 'likely reassigned' tag."""
        records = [
            RouteRecord("t1", 12345, "0x00158d0001234567", 180, -45, 1, [9814]),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        dev = result["Living Room Plug"]
        assert dev["current_path"] == [
            "master_bedroom_pir [addr:9814, EndDevice — likely reassigned]",
        ]

    def test_summary_stats(self) -> None:
        records = [
            RouteRecord("t1", 12345, "0x00158d0001234567", 180, -45, 2, [100, 200]),
            RouteRecord("t2", 54321, "0x00158d0009876543", 120, -60, 0, []),
        ]

        result = aggregate_routes(records, ADDR_INFO, IEEE_MAP)

        assert len(result) == 2

    def test_empty_records(self) -> None:
        result = aggregate_routes([], ADDR_INFO, IEEE_MAP)
        assert result == {}


# ---------------------------------------------------------------------------
# aggregate_signal_stats
# ---------------------------------------------------------------------------


class TestAggregateSignalStats:
    def test_single_device_stats(self) -> None:
        messages = [
            IncomingMessage("t1", 12345, 150, -50),
            IncomingMessage("t2", 12345, 180, -40),
            IncomingMessage("t3", 12345, 120, -60),
        ]

        result = aggregate_signal_stats(messages, [], ADDR_INFO, IEEE_MAP)

        key = "Living Room Plug [addr:12345, Router]"
        assert key in result
        dev = result[key]
        assert dev["lqi_min"] == 120
        assert dev["lqi_max"] == 180
        assert dev["lqi_avg"] == 150
        assert dev["rssi_min"] == -60
        assert dev["rssi_max"] == -40
        assert dev["rssi_avg"] == -50
        assert dev["sample_count"] == 3

    def test_includes_route_record_signals(self) -> None:
        """Route records use IEEE-resolved names, incoming messages use formatted address."""
        messages = [
            IncomingMessage("t1", 12345, 150, -50),
        ]
        records = [
            RouteRecord("t2", 12345, "0x00158d0001234567", 180, -40, 1, [100]),
        ]

        result = aggregate_signal_stats(messages, records, ADDR_INFO, IEEE_MAP)

        # Incoming message keyed by formatted address, route record by IEEE name
        msg_key = "Living Room Plug [addr:12345, Router]"
        assert msg_key in result
        assert "Living Room Plug" in result
        assert result[msg_key]["sample_count"] == 1
        assert result["Living Room Plug"]["sample_count"] == 1

    def test_sorted_weakest_first(self) -> None:
        messages = [
            IncomingMessage("t1", 12345, 180, -30),
            IncomingMessage("t2", 54321, 50, -80),
        ]

        result = aggregate_signal_stats(messages, [], ADDR_INFO, IEEE_MAP)

        keys = list(result.keys())
        # EndDevice key comes first (weaker signal)
        assert "Kitchen Sensor" in keys[0]
        assert "Living Room Plug" in keys[1]

    def test_empty_messages(self) -> None:
        result = aggregate_signal_stats([], [], ADDR_INFO, IEEE_MAP)
        assert result == {}

    def test_unknown_device_uses_addr_format(self) -> None:
        messages = [
            IncomingMessage("t1", 99999, 100, -70),
        ]

        result = aggregate_signal_stats(messages, [], ADDR_INFO, IEEE_MAP)

        assert "[addr:99999]" in result


# ---------------------------------------------------------------------------
# analyze_debug_entries
# ---------------------------------------------------------------------------


class TestAnalyzeDebugEntries:
    def test_full_pipeline(self) -> None:
        entries = [
            {"timestamp": "2026-02-08T10:00:00Z", "level": "debug", "message": SAMPLE_DEBUG_ROUTE_RECORD},
            {"timestamp": "2026-02-08T10:00:01Z", "level": "debug", "message": SAMPLE_DEBUG_INCOMING_MSG},
            {"timestamp": "2026-02-08T10:00:02Z", "level": "debug", "message": SAMPLE_DEBUG_UART_NOISE},
            {"timestamp": "2026-02-08T10:00:03Z", "level": "error", "message": SAMPLE_DEBUG_ROUTE_ERROR},
        ]

        result = analyze_debug_entries(entries, ADDR_INFO, IEEE_MAP)

        assert result["total_entries"] == 4
        assert result["categories"]["route_records"] == 1
        assert result["categories"]["incoming_messages"] == 1
        assert result["categories"]["uart_noise"] == 1
        assert result["categories"]["route_errors"] == 1
        assert len(result["route_errors"]) == 1
        assert result["route_errors"][0]["device"] == "Living Room Plug"
        assert result["route_errors"][0]["error_type"] == "routeDiscoveryFailed"

    def test_empty_entries(self) -> None:
        result = analyze_debug_entries([], ADDR_INFO, IEEE_MAP)

        assert result["total_entries"] == 0
        assert result["categories"]["route_records"] == 0

    def test_skips_uart_noise_in_parsing(self) -> None:
        """UART noise entries are counted but not parsed as routes/messages."""
        entries = [
            {"timestamp": "t1", "level": "debug", "message": SAMPLE_DEBUG_UART_NOISE},
            {"timestamp": "t2", "level": "debug", "message": SAMPLE_DEBUG_UART_NOISE},
        ]

        result = analyze_debug_entries(entries, ADDR_INFO, IEEE_MAP)

        assert result["categories"]["uart_noise"] == 2
        assert result["categories"]["route_records"] == 0
        assert result["categories"]["incoming_messages"] == 0

    def test_weak_signal_devices(self) -> None:
        """Devices with avg LQI < 80 appear in weak_signal_devices."""
        entries = [
            {
                "timestamp": "t1",
                "level": "debug",
                "message": (
                    'zh:ember:ezsp: ezspIncomingMessageHandler: type=4 '
                    '"apsFrame":{"profileId":260,"clusterId":6} '
                    '"senderShortId":54321 '
                    '"lastHopLqi":40, "lastHopRssi":-85'
                ),
            },
        ]

        result = analyze_debug_entries(entries, ADDR_INFO, IEEE_MAP)

        assert len(result["weak_signal_devices"]) >= 1
        # Now keyed by formatted address
        weak_name = result["weak_signal_devices"][0]["device"]
        assert "Kitchen Sensor" in weak_name
        assert "addr:54321" in weak_name

    def test_routing_instability(self) -> None:
        """Devices with >= 3 path changes appear in routing_instability."""
        entries = [
            {"timestamp": "t1", "level": "debug", "message": (
                "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=12345 "
                "sourceEui=0x00158d0001234567 lastHopLqi=180 lastHopRssi=-45 "
                "relayCount=1 relayList=100"
            )},
            {"timestamp": "t2", "level": "debug", "message": (
                "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=12345 "
                "sourceEui=0x00158d0001234567 lastHopLqi=170 lastHopRssi=-50 "
                "relayCount=1 relayList=200"
            )},
            {"timestamp": "t3", "level": "debug", "message": (
                "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=12345 "
                "sourceEui=0x00158d0001234567 lastHopLqi=180 lastHopRssi=-45 "
                "relayCount=1 relayList=100"
            )},
            {"timestamp": "t4", "level": "debug", "message": (
                "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=12345 "
                "sourceEui=0x00158d0001234567 lastHopLqi=160 lastHopRssi=-55 "
                "relayCount=0 relayList="
            )},
        ]

        result = analyze_debug_entries(entries, ADDR_INFO, IEEE_MAP)

        assert len(result["routing_instability"]) >= 1
        assert result["routing_instability"][0]["device"] == "Living Room Plug"
        assert result["routing_instability"][0]["path_changes"] == 3
