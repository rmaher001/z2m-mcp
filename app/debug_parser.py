"""Parser for Z2M debug log entries (route records, incoming messages, errors)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

_RE_ROUTE_RECORD = re.compile(
    r"ezspIncomingRouteRecordHandler:\s+"
    r"source=(\d+)\s+"
    r"sourceEui=(0x[0-9a-fA-F]+)\s+"
    r"lastHopLqi=(\d+)\s+"
    r"lastHopRssi=(-?\d+)\s+"
    r"relayCount=(\d+)\s+"
    r"relayList=([\d,]*)"
)

_RE_INCOMING_MSG = re.compile(
    r"ezspIncomingMessageHandler:.*"
    r'"senderShortId":(\d+).*'
    r'"lastHopLqi":(\d+),\s*"lastHopRssi":(-?\d+)'
)

_RE_ROUTE_ERROR = re.compile(
    r"Received network/route error (\S+) for "
    r'"(0x[0-9a-fA-F]+)"'
)

_UART_PREFIX = "zh:ember:uart"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RouteRecord:
    timestamp: str
    source: int
    source_eui: str
    last_hop_lqi: int
    last_hop_rssi: int
    relay_count: int
    relay_list: list[int]


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    timestamp: str
    sender_short_id: int
    last_hop_lqi: int
    last_hop_rssi: int


@dataclass(frozen=True, slots=True)
class RouteError:
    timestamp: str
    error_type: str
    address: str


# ---------------------------------------------------------------------------
# Parse functions
# ---------------------------------------------------------------------------


def parse_route_record(ts: str, msg: str) -> RouteRecord | None:
    m = _RE_ROUTE_RECORD.search(msg)
    if not m:
        return None

    relay_str = m.group(6).strip()
    relay_list = [int(x) for x in relay_str.split(",") if x] if relay_str else []

    return RouteRecord(
        timestamp=ts,
        source=int(m.group(1)),
        source_eui=m.group(2),
        last_hop_lqi=int(m.group(3)),
        last_hop_rssi=int(m.group(4)),
        relay_count=int(m.group(5)),
        relay_list=relay_list,
    )


def parse_incoming_message(ts: str, msg: str) -> IncomingMessage | None:
    m = _RE_INCOMING_MSG.search(msg)
    if not m:
        return None

    return IncomingMessage(
        timestamp=ts,
        sender_short_id=int(m.group(1)),
        last_hop_lqi=int(m.group(2)),
        last_hop_rssi=int(m.group(3)),
    )


def parse_route_error(ts: str, msg: str) -> RouteError | None:
    m = _RE_ROUTE_ERROR.search(msg)
    if not m:
        return None

    return RouteError(
        timestamp=ts,
        error_type=m.group(1),
        address=m.group(2),
    )


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def _resolve_ieee(ieee: str, ieee_map: dict[str, str]) -> str:
    return ieee_map.get(ieee, ieee)


def _format_relay(addr: int, addr_info: dict[int, dict[str, str]]) -> str:
    """Format a relay hop address with best-effort name resolution."""
    info = addr_info.get(addr)
    if not info:
        return f"[addr:{addr}]"
    name = info["name"]
    dev_type = info["type"]
    if dev_type == "EndDevice":
        return f"{name} [addr:{addr}, {dev_type} — likely reassigned]"
    return f"{name} [addr:{addr}, {dev_type}]"


def _path_key(relay_labels: list[str]) -> str:
    if not relay_labels:
        return "(direct)"
    return " -> ".join(relay_labels)


def aggregate_routes(
    records: list[RouteRecord],
    addr_info: dict[int, dict[str, str]],
    ieee_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Aggregate route records into per-device routing summaries.

    Relay hops are resolved best-effort via addr_info (network_address ->
    {name, type}). The raw address is always included because network
    addresses are ephemeral and can be reassigned on device rejoin.
    """
    if not records:
        return {}

    # Group by source device
    by_device: dict[str, list[RouteRecord]] = {}
    for rec in records:
        name = _resolve_ieee(rec.source_eui, ieee_map)
        by_device.setdefault(name, []).append(rec)

    result: dict[str, dict[str, Any]] = {}
    for device_name, recs in by_device.items():
        observed_paths: dict[str, int] = {}
        prev_path: str | None = None
        path_changes = 0

        for rec in recs:
            formatted = [_format_relay(a, addr_info) for a in rec.relay_list]
            key = _path_key(formatted)
            observed_paths[key] = observed_paths.get(key, 0) + 1

            if prev_path is not None and key != prev_path:
                path_changes += 1
            prev_path = key

        # Current path is the last observed
        last = recs[-1]
        if last.relay_list:
            current_path = [_format_relay(a, addr_info) for a in last.relay_list]
        else:
            current_path = ["(direct)"]

        result[device_name] = {
            "current_path": current_path,
            "observed_paths": observed_paths,
            "path_change_count": path_changes,
            "total_records": len(recs),
        }

    return result


def aggregate_signal_stats(
    messages: list[IncomingMessage],
    records: list[RouteRecord],
    addr_info: dict[int, dict[str, str]],
    ieee_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Aggregate signal stats (LQI/RSSI) per device, sorted weakest first.

    Incoming messages are keyed by best-effort resolved name (with raw address).
    Route records are keyed by IEEE-resolved friendly name (permanent).
    """
    if not messages and not records:
        return {}

    # Collect (lqi, rssi) samples per device
    samples: dict[str, list[tuple[int, int]]] = {}

    for msg in messages:
        name = _format_relay(msg.sender_short_id, addr_info)
        samples.setdefault(name, []).append((msg.last_hop_lqi, msg.last_hop_rssi))

    for rec in records:
        name = _resolve_ieee(rec.source_eui, ieee_map)
        samples.setdefault(name, []).append((rec.last_hop_lqi, rec.last_hop_rssi))

    result: dict[str, dict[str, Any]] = {}
    for device_name, samps in samples.items():
        if not samps:
            continue
        lqis = [s[0] for s in samps]
        rssis = [s[1] for s in samps]

        result[device_name] = {
            "lqi_min": min(lqis),
            "lqi_max": max(lqis),
            "lqi_avg": round(sum(lqis) / len(lqis)),
            "rssi_min": min(rssis),
            "rssi_max": max(rssis),
            "rssi_avg": round(sum(rssis) / len(rssis)),
            "sample_count": len(samps),
        }

    # Sort by avg LQI ascending (weakest first)
    return dict(sorted(result.items(), key=lambda item: item[1]["lqi_avg"]))


def analyze_debug_entries(
    entries: list[dict[str, str]],
    addr_info: dict[int, dict[str, str]],
    ieee_map: dict[str, str],
) -> dict[str, Any]:
    """Full debug log analysis: categorize, parse, and aggregate."""
    route_records: list[RouteRecord] = []
    incoming_messages: list[IncomingMessage] = []
    route_errors: list[RouteError] = []
    uart_count = 0
    other_count = 0

    for entry in entries:
        msg = entry.get("message", "")
        ts = entry.get("timestamp", "")

        # Skip UART noise early
        if msg.startswith(_UART_PREFIX):
            uart_count += 1
            continue

        # Try route record
        rec = parse_route_record(ts, msg)
        if rec:
            route_records.append(rec)
            continue

        # Try incoming message
        im = parse_incoming_message(ts, msg)
        if im:
            incoming_messages.append(im)
            continue

        # Try route error
        err = parse_route_error(ts, msg)
        if err:
            route_errors.append(err)
            continue

        other_count += 1

    # Aggregate
    routes = aggregate_routes(route_records, addr_info, ieee_map)
    signals = aggregate_signal_stats(incoming_messages, route_records, addr_info, ieee_map)

    # Build error list with resolved names
    error_list = [
        {
            "timestamp": e.timestamp,
            "error_type": e.error_type,
            "address": e.address,
            "device": _resolve_ieee(e.address, ieee_map),
        }
        for e in route_errors
    ]

    # Weak signal devices: avg LQI < 80
    weak_signal = [
        {"device": name, **stats}
        for name, stats in signals.items()
        if stats["lqi_avg"] < 80
    ]

    # Routing instability: devices with >= 3 path changes
    instability = [
        {"device": name, "path_changes": data["path_change_count"], **data}
        for name, data in routes.items()
        if data["path_change_count"] >= 3
    ]

    return {
        "total_entries": len(entries),
        "categories": {
            "route_records": len(route_records),
            "incoming_messages": len(incoming_messages),
            "route_errors": len(route_errors),
            "uart_noise": uart_count,
            "other": other_count,
        },
        "route_errors": error_list,
        "weak_signal_devices": weak_signal,
        "routing_instability": instability,
    }
