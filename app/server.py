"""MCP server with Z2M diagnostic and control tools."""

from __future__ import annotations

import atexit
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP

from app.config import load_config
from app.debug_parser import (
    aggregate_routes,
    aggregate_signal_stats,
    analyze_debug_entries,
    parse_incoming_message,
    parse_route_record,
)
from app.mqtt_client import Z2MClient

logger = logging.getLogger(__name__)

_z2m: Z2MClient | None = None


def _shutdown_client() -> None:
    """Close the log file handler on process exit."""
    if _z2m is not None and _z2m._log_writer is not None:
        _z2m._log_writer.close()


atexit.register(_shutdown_client)


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Start MQTT client on server startup (singleton — survives across sessions)."""
    global _z2m
    if _z2m is None:
        config = load_config()
        _z2m = Z2MClient(config.mqtt, log_config=config.log)
        await _z2m.start()
    yield {"z2m": _z2m}


mcp = FastMCP("Z2M-MCP", host="0.0.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Diagnostic Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def get_bridge_info() -> dict[str, Any]:
    """Get Zigbee2MQTT bridge information.

    Returns Z2M version, coordinator firmware, Zigbee channel, PAN ID,
    permit_join status, and device count.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    info = z2m.get_bridge_info()
    if not info:
        raise RuntimeError("Bridge info not yet available. Z2M may not be running.")

    coordinator = info.get("coordinator", {})
    network = info.get("network", {})
    devices = z2m.get_all_devices()

    # Count by type
    type_counts: dict[str, int] = {}
    for d in devices:
        t = d.get("type", "Unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    return {
        "version": info.get("version"),
        "zigbee_herdsman": info.get("zigbee_herdsman", {}).get("version"),
        "zigbee_herdsman_converters": info.get("zigbee_herdsman_converters", {}).get("version"),
        "coordinator": {
            "type": coordinator.get("type"),
            "ieee_address": coordinator.get("ieee_address"),
            "firmware_revision": coordinator.get("meta", {}).get("revision"),
        },
        "network": {
            "channel": network.get("channel"),
            "pan_id": network.get("pan_id"),
            "extended_pan_id": network.get("extended_pan_id"),
        },
        "permit_join": info.get("permit_join", False),
        "log_level": info.get("log_level"),
        "restart_required": info.get("restart_required", False),
        "device_count": len(devices),
        "device_types": type_counts,
    }


@mcp.tool()
async def list_devices(
    device_type: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """List all Zigbee devices.

    Returns lean format: friendly_name, type, model, vendor, power_source,
    and last state info (LQI, last_seen) when available.

    Args:
        device_type: Filter by type: Router, EndDevice, or Coordinator.
        limit: Maximum number of devices to return.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    devices = z2m.get_all_devices()

    if device_type:
        devices = [d for d in devices if d.get("type") == device_type]

    total = len(devices)
    devices = devices[:limit]

    result = []
    for d in devices:
        definition = d.get("definition") or {}
        state = d.get("state", {})
        entry: dict[str, Any] = {
            "friendly_name": d.get("friendly_name"),
            "ieee_address": d.get("ieee_address"),
            "type": d.get("type"),
            "model": definition.get("model"),
            "vendor": definition.get("vendor"),
            "power_source": d.get("power_source"),
            "supported": d.get("supported"),
        }
        if "linkquality" in state:
            entry["lqi"] = state["linkquality"]
        if "last_seen" in state:
            entry["last_seen"] = state["last_seen"]
        result.append(entry)

    return {
        "devices": result,
        "count": len(result),
        "total": total,
        "truncated": total > limit,
    }


@mcp.tool()
async def get_device_info(
    device: str,
    detailed: bool = False,
) -> dict[str, Any]:
    """Get detailed info for a specific Zigbee device.

    Args:
        device: Device friendly_name or IEEE address.
        detailed: If True, include full endpoint/cluster data.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    d = z2m.get_device(device)
    if not d:
        raise ValueError(f"Device '{device}' not found")

    definition = d.get("definition") or {}
    state = d.get("state", {})

    result: dict[str, Any] = {
        "friendly_name": d.get("friendly_name"),
        "ieee_address": d.get("ieee_address"),
        "type": d.get("type"),
        "network_address": d.get("network_address"),
        "model": definition.get("model"),
        "vendor": definition.get("vendor"),
        "description": definition.get("description"),
        "manufacturer": d.get("manufacturer"),
        "model_id": d.get("model_id"),
        "power_source": d.get("power_source"),
        "supported": d.get("supported"),
        "disabled": d.get("disabled"),
        "interview_completed": d.get("interview_completed"),
        "interviewing": d.get("interviewing"),
    }

    if state:
        result["state"] = state

    if detailed:
        result["endpoints"] = d.get("endpoints")

    return result


@mcp.tool()
async def get_network_map() -> dict[str, Any]:
    """Get the Zigbee network topology map.

    Returns structured topology showing coordinator, routers, and end devices
    with link quality (LQI) on connections. This request may take 10-30 seconds.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    response = await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/networkmap",
        response_topic="zigbee2mqtt/bridge/response/networkmap",
        payload={"type": "raw", "routes": True},
        timeout=60.0,
    )

    data = response.get("data", {})
    nodes = data.get("value", {}).get("nodes", data.get("nodes", []))
    links = data.get("value", {}).get("links", data.get("links", []))

    # Build structured output
    structured_nodes = []
    for node in nodes:
        structured_nodes.append({
            "ieee_address": node.get("ieeeAddr"),
            "friendly_name": node.get("friendlyName"),
            "type": node.get("type"),
            "network_address": node.get("networkAddress"),
            "model": node.get("definition", {}).get("model") if node.get("definition") else None,
        })

    structured_links = []
    for link in links:
        structured_links.append({
            "source": link.get("source", {}).get("ieeeAddr"),
            "target": link.get("target", {}).get("ieeeAddr"),
            "lqi": link.get("linkquality"),
            "depth": link.get("depth"),
            "relationship": link.get("relationship"),
        })

    return {
        "nodes": structured_nodes,
        "links": structured_links,
        "node_count": len(structured_nodes),
        "link_count": len(structured_links),
    }


@mcp.tool()
async def get_device_health(device: str) -> dict[str, Any]:
    """Get health summary for a specific device.

    Shows last_seen age, link quality, availability status, and battery level.

    Args:
        device: Device friendly_name or IEEE address.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    d = z2m.get_device(device)
    if not d:
        raise ValueError(f"Device '{device}' not found")

    state = d.get("state", {})
    definition = d.get("definition") or {}
    now = datetime.now(timezone.utc)

    health: dict[str, Any] = {
        "friendly_name": d.get("friendly_name"),
        "ieee_address": d.get("ieee_address"),
        "type": d.get("type"),
        "model": definition.get("model"),
        "power_source": d.get("power_source"),
        "interview_completed": d.get("interview_completed"),
    }

    # LQI
    lqi = state.get("linkquality")
    if lqi is not None:
        health["lqi"] = lqi
        if lqi >= 100:
            health["lqi_status"] = "good"
        elif lqi >= 50:
            health["lqi_status"] = "fair"
        else:
            health["lqi_status"] = "poor"

    # Last seen
    last_seen = state.get("last_seen")
    if last_seen:
        health["last_seen"] = last_seen
        try:
            if isinstance(last_seen, (int, float)):
                ls_dt = datetime.fromtimestamp(last_seen / 1000, tz=timezone.utc)
            else:
                ls_dt = datetime.fromisoformat(str(last_seen).replace("Z", "+00:00"))
            age_seconds = (now - ls_dt).total_seconds()
            health["last_seen_age_minutes"] = round(age_seconds / 60, 1)
            if age_seconds < 3600:
                health["availability"] = "online"
            elif age_seconds < 86400:
                health["availability"] = "stale"
            else:
                health["availability"] = "offline"
        except (ValueError, TypeError):
            health["availability"] = "unknown"

    # Battery
    battery = state.get("battery")
    if battery is not None:
        health["battery"] = battery
        if battery >= 50:
            health["battery_status"] = "good"
        elif battery >= 20:
            health["battery_status"] = "low"
        else:
            health["battery_status"] = "critical"

    return health


@mcp.tool()
async def list_weak_devices(
    lqi_threshold: int = 50,
    stale_hours: float = 6.0,
    limit: int = 50,
) -> dict[str, Any]:
    """List devices with weak signal or stale last_seen.

    Finds devices with LQI below threshold or that haven't reported
    within stale_hours. Useful for identifying network problems.

    Args:
        lqi_threshold: LQI values below this are flagged (default: 50).
        stale_hours: Hours since last report to flag as stale (default: 6).
        limit: Maximum number of results.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    devices = z2m.get_all_devices()
    now = datetime.now(timezone.utc)
    weak: list[dict[str, Any]] = []

    for d in devices:
        if d.get("type") == "Coordinator":
            continue

        state = d.get("state", {})
        definition = d.get("definition") or {}
        issues: list[str] = []

        lqi = state.get("linkquality")
        if lqi is not None and lqi < lqi_threshold:
            issues.append(f"low_lqi ({lqi})")

        last_seen = state.get("last_seen")
        age_hours = None
        if last_seen:
            try:
                if isinstance(last_seen, (int, float)):
                    ls_dt = datetime.fromtimestamp(last_seen / 1000, tz=timezone.utc)
                else:
                    ls_dt = datetime.fromisoformat(str(last_seen).replace("Z", "+00:00"))
                age_hours = (now - ls_dt).total_seconds() / 3600
                if age_hours > stale_hours:
                    issues.append(f"stale ({age_hours:.1f}h)")
            except (ValueError, TypeError):
                pass

        if issues:
            weak.append({
                "friendly_name": d.get("friendly_name"),
                "ieee_address": d.get("ieee_address"),
                "type": d.get("type"),
                "model": definition.get("model"),
                "lqi": lqi,
                "last_seen_hours_ago": round(age_hours, 1) if age_hours else None,
                "issues": issues,
            })

    weak.sort(key=lambda x: x.get("lqi") or 999)
    total = len(weak)
    weak = weak[:limit]

    return {
        "devices": weak,
        "count": len(weak),
        "total": total,
        "truncated": total > limit,
        "thresholds": {
            "lqi": lqi_threshold,
            "stale_hours": stale_hours,
        },
    }


@mcp.tool()
async def analyze_logs(
    minutes_back: int = 60,
    level: str | None = None,
) -> dict[str, Any]:
    """Analyze Zigbee2MQTT logs for errors and issues.

    Returns recent log entries from the in-memory buffer (last 1000 messages).
    Full history is persisted to a JSONL file for offline analysis.

    Args:
        minutes_back: Number of minutes of logs to analyze (default: 60).
        level: Filter by level: error, warn, info, or debug.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    file_entries = z2m.get_logs_from_file(minutes_back=minutes_back, level=level)
    buffer_entries = z2m.get_logs(minutes_back=minutes_back, level=level)

    # Deduplicate — collector file and in-session buffer may both capture
    # the same MQTT log message with slightly different timestamps.
    # Truncate timestamp to the second for dedup to handle clock skew.
    seen: set[tuple[str, str, str]] = set()
    entries: list[dict[str, str]] = []
    for entry in file_entries + buffer_entries:
        ts = entry.get("timestamp", "")[:19]  # "YYYY-MM-DDTHH:MM:SS"
        key = (ts, entry.get("level", ""), entry.get("message", ""))
        if key not in seen:
            seen.add(key)
            entries.append(entry)

    entries.sort(key=lambda e: e.get("timestamp", ""))

    error_count = sum(1 for e in entries if e.get("level") == "error")
    warning_count = sum(1 for e in entries if e.get("level") == "warn")

    return {
        "error_count": error_count,
        "warning_count": warning_count,
        "entries": entries,
        "total_in_buffer": z2m.get_log_buffer_size(),
        "log_file": z2m.get_log_file_path(),
        "note": (
            f"Showing {len(entries)} entries from last {minutes_back} minutes "
            f"(merged from persistent log file and in-session buffer). "
            f"In-memory buffer holds {z2m.get_log_buffer_size()} of the last "
            f"1000 messages."
        ),
    }


_NO_DEBUG_HINT = (
    "No debug log entries found. To enable debug logging in Z2M, set "
    "'log_debug_to_mqtt_frontend: true' in the Z2M advanced configuration, "
    "then set the log level to debug."
)


@mcp.tool()
async def analyze_debug_logs(minutes_back: int = 60) -> dict[str, Any]:
    """Analyze Z2M debug logs for network health insights.

    Parses debug-level log entries to provide message category counts,
    route errors with resolved device names, weak signal devices,
    and routing instability indicators.

    Requires Z2M debug logging enabled (log_debug_to_mqtt_frontend: true).

    Args:
        minutes_back: Number of minutes of debug logs to analyze (default: 60).
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    ieee_map = z2m.build_ieee_map()

    debug_entries = z2m.get_logs_from_file(minutes_back=minutes_back, level="debug")

    # Also fetch non-debug entries for route errors (logged at error/warn level)
    all_entries = z2m.get_logs_from_file(minutes_back=minutes_back)
    non_debug = [e for e in all_entries if e.get("level") != "debug"]

    combined = debug_entries + non_debug

    if not combined:
        return {
            "total_entries": 0,
            "categories": {
                "route_records": 0,
                "incoming_messages": 0,
                "route_errors": 0,
                "uart_noise": 0,
                "other": 0,
            },
            "route_errors": [],
            "weak_signal_devices": [],
            "routing_instability": [],
            "note": _NO_DEBUG_HINT,
        }

    result = analyze_debug_entries(combined, ieee_map)
    result["minutes_back"] = minutes_back
    return result


@mcp.tool()
async def get_routing_table(
    device: str | None = None,
    minutes_back: int = 60,
) -> dict[str, Any]:
    """Get per-device routing paths from debug log route records.

    Shows current relay path through the mesh (resolved to friendly names),
    all observed paths with frequency counts, and path change count as
    an instability indicator.

    Requires Z2M debug logging enabled (log_debug_to_mqtt_frontend: true).

    Args:
        device: Optional device friendly_name to filter results.
        minutes_back: Number of minutes of debug logs to analyze (default: 60).
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    ieee_map = z2m.build_ieee_map()

    entries = z2m.get_logs_from_file(minutes_back=minutes_back, level="debug")

    if not entries:
        return {
            "routes": {},
            "note": _NO_DEBUG_HINT,
        }

    records = []
    for entry in entries:
        rec = parse_route_record(entry.get("timestamp", ""), entry.get("message", ""))
        if rec:
            records.append(rec)

    routes = aggregate_routes(records, ieee_map)

    if device:
        routes = {k: v for k, v in routes.items() if k == device}

    # Summary stats
    direct_count = sum(1 for r in routes.values() if r["current_path"] == ["(direct)"])
    relayed_count = len(routes) - direct_count
    all_relays: dict[str, int] = {}
    for data in routes.values():
        for path_key, count in data["observed_paths"].items():
            if path_key != "(direct)":
                for relay in path_key.split(" -> "):
                    all_relays[relay] = all_relays.get(relay, 0) + count

    top_routers = sorted(all_relays.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "routes": routes,
        "device_count": len(routes),
        "direct_devices": direct_count,
        "relayed_devices": relayed_count,
        "top_routers": [{"name": name, "relay_count": cnt} for name, cnt in top_routers],
        "minutes_back": minutes_back,
    }


@mcp.tool()
async def get_signal_history(
    device: str | None = None,
    minutes_back: int = 60,
) -> dict[str, Any]:
    """Get per-device LQI/RSSI signal history from debug logs.

    Shows min/max/avg LQI and RSSI per device, sample count, sorted
    weakest-first. Combines data from incoming message handlers and
    route records.

    Requires Z2M debug logging enabled (log_debug_to_mqtt_frontend: true).

    Args:
        device: Optional device friendly_name to filter results.
        minutes_back: Number of minutes of debug logs to analyze (default: 60).
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    ieee_map = z2m.build_ieee_map()

    entries = z2m.get_logs_from_file(minutes_back=minutes_back, level="debug")

    if not entries:
        return {
            "devices": {},
            "note": _NO_DEBUG_HINT,
        }

    messages = []
    records = []
    for entry in entries:
        ts = entry.get("timestamp", "")
        msg = entry.get("message", "")

        rec = parse_route_record(ts, msg)
        if rec:
            records.append(rec)
            continue

        im = parse_incoming_message(ts, msg)
        if im:
            messages.append(im)

    signals = aggregate_signal_stats(messages, records, ieee_map)

    if device:
        signals = {k: v for k, v in signals.items() if k == device}

    return {
        "devices": signals,
        "device_count": len(signals),
        "minutes_back": minutes_back,
    }


# ---------------------------------------------------------------------------
# Control Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def permit_join(
    enable: bool = True,
    time: int = 120,
) -> dict[str, Any]:
    """Enable or disable Zigbee pairing mode.

    Args:
        enable: True to enable pairing, False to disable.
        time: Timeout in seconds for pairing mode (default: 120).
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/permit_join",
        response_topic="zigbee2mqtt/bridge/response/permit_join",
        payload={"value": enable, "time": time},
    )


@mcp.tool()
async def reconfigure_device(device: str) -> dict[str, Any]:
    """Force a device re-interview and reconfiguration.

    Useful when a device is misbehaving or after firmware update.

    Args:
        device: Device friendly_name or IEEE address.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    # Verify device exists
    d = z2m.get_device(device)
    if not d:
        raise ValueError(f"Device '{device}' not found")

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/device/configure",
        response_topic="zigbee2mqtt/bridge/response/device/configure",
        payload={"id": device},
    )


@mcp.tool()
async def rename_device(
    old_name: str,
    new_name: str,
) -> dict[str, Any]:
    """Rename a Zigbee device.

    Args:
        old_name: Current device friendly_name or IEEE address.
        new_name: New friendly name for the device.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    # Verify device exists
    d = z2m.get_device(old_name)
    if not d:
        raise ValueError(f"Device '{old_name}' not found")

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/device/rename",
        response_topic="zigbee2mqtt/bridge/response/device/rename",
        payload={"from": old_name, "to": new_name},
    )


@mcp.tool()
async def remove_device(
    device: str,
    force: bool = False,
) -> dict[str, Any]:
    """Remove a device from the Zigbee network.

    Args:
        device: Device friendly_name or IEEE address.
        force: If True, force-remove even if device is unresponsive.
    """
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    # Verify device exists
    d = z2m.get_device(device)
    if not d:
        raise ValueError(f"Device '{device}' not found")

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/device/remove",
        response_topic="zigbee2mqtt/bridge/response/device/remove",
        payload={"id": device, "force": force},
    )


@mcp.tool()
async def restart_z2m() -> dict[str, Any]:
    """Restart the Zigbee2MQTT service."""
    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/restart",
        response_topic="zigbee2mqtt/bridge/response/restart",
        payload={},
    )


@mcp.tool()
async def set_log_level(level: str) -> dict[str, Any]:
    """Change the Zigbee2MQTT log level.

    Args:
        level: Log level: debug, info, warn, or error.
    """
    valid_levels = ("debug", "info", "warn", "error")
    if level not in valid_levels:
        raise ValueError(f"Invalid log level '{level}'. Must be one of: {', '.join(valid_levels)}")

    ctx = mcp.get_context()
    z2m: Z2MClient = ctx.request_context.lifespan_context["z2m"]

    return await z2m.request_response(
        request_topic="zigbee2mqtt/bridge/request/options",
        response_topic="zigbee2mqtt/bridge/response/options",
        payload={"options": {"advanced": {"log_level": level}}},
    )
