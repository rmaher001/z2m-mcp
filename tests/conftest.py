"""Shared test fixtures."""

from __future__ import annotations

import pytest

from app.config import AppConfig, LogConfig, MQTTConfig


@pytest.fixture
def mqtt_config() -> MQTTConfig:
    return MQTTConfig(
        host="localhost",
        port=1883,
        username="test_user",
        password="test_pass",
    )


@pytest.fixture
def log_config(tmp_path) -> LogConfig:
    return LogConfig(
        dir=str(tmp_path / "logs"),
        max_size_mb=10,
        backup_count=3,
        retention_days=7,
        max_total_mb=100,
    )


@pytest.fixture
def app_config(mqtt_config: MQTTConfig, log_config: LogConfig) -> AppConfig:
    return AppConfig(
        mqtt=mqtt_config,
        log=log_config,
        timezone="America/Los_Angeles",
    )


SAMPLE_BRIDGE_INFO = {
    "commit": "abc123",
    "config": {
        "advanced": {"channel": 20, "pan_id": 6754},
        "permit_join": False,
    },
    "coordinator": {
        "ieee_address": "0x00124b002345abcd",
        "meta": {"revision": 20240710, "transportrev": 2},
        "type": "zStack30x",
    },
    "log_level": "info",
    "network": {
        "channel": 20,
        "extended_pan_id": "0xdddddddddddddddd",
        "pan_id": 6754,
    },
    "permit_join": False,
    "restart_required": False,
    "version": "2.1.1-1",
    "zigbee_herdsman": {"version": "3.0.0"},
    "zigbee_herdsman_converters": {"version": "21.0.0"},
}


SAMPLE_DEVICE_ROUTER = {
    "definition": {
        "description": "Smart plug",
        "model": "SP 224",
        "vendor": "Innr",
    },
    "disabled": False,
    "endpoints": {
        "1": {
            "bindings": [],
            "clusters": {"input": ["genOnOff"], "output": []},
        }
    },
    "friendly_name": "Living Room Plug",
    "ieee_address": "0x00158d0001234567",
    "interview_completed": True,
    "interviewing": False,
    "manufacturer": "Innr",
    "model_id": "SP 224",
    "network_address": 12345,
    "power_source": "Mains (single phase)",
    "supported": True,
    "type": "Router",
}


SAMPLE_DEVICE_END_DEVICE = {
    "definition": {
        "description": "Temperature & humidity sensor",
        "model": "SNZB-02",
        "vendor": "SONOFF",
    },
    "disabled": False,
    "endpoints": {
        "1": {
            "bindings": [],
            "clusters": {"input": ["msTemperatureMeasurement"], "output": []},
        }
    },
    "friendly_name": "Kitchen Sensor",
    "ieee_address": "0x00158d0009876543",
    "interview_completed": True,
    "interviewing": False,
    "manufacturer": "SONOFF",
    "model_id": "SNZB-02",
    "network_address": 54321,
    "power_source": "Battery",
    "supported": True,
    "type": "EndDevice",
}


SAMPLE_DEVICE_COORDINATOR = {
    "definition": None,
    "disabled": False,
    "endpoints": {
        "1": {
            "bindings": [],
            "clusters": {"input": [], "output": []},
        }
    },
    "friendly_name": "Coordinator",
    "ieee_address": "0x00124b002345abcd",
    "interview_completed": True,
    "interviewing": False,
    "manufacturer": None,
    "model_id": None,
    "network_address": 0,
    "power_source": "Mains (single phase)",
    "supported": False,
    "type": "Coordinator",
}


SAMPLE_DEVICES_LIST = [
    SAMPLE_DEVICE_COORDINATOR,
    SAMPLE_DEVICE_ROUTER,
    SAMPLE_DEVICE_END_DEVICE,
]


# ---------------------------------------------------------------------------
# Debug log samples (from Z2M with log_debug_to_mqtt_frontend: true)
# ---------------------------------------------------------------------------

SAMPLE_DEBUG_ROUTE_RECORD = (
    "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=12345 "
    "sourceEui=0x00158d0001234567 lastHopLqi=180 lastHopRssi=-45 "
    "relayCount=2 relayList=100,200"
)

SAMPLE_DEBUG_ROUTE_RECORD_DIRECT = (
    "zh:ember:ezsp: ezspIncomingRouteRecordHandler: source=54321 "
    "sourceEui=0x00158d0009876543 lastHopLqi=120 lastHopRssi=-60 "
    "relayCount=0 relayList="
)

SAMPLE_DEBUG_INCOMING_MSG = (
    'zh:ember:ezsp: ezspIncomingMessageHandler: type=4 '
    '"apsFrame":{"profileId":260,"clusterId":6,"sourceEndpoint":1,'
    '"destinationEndpoint":1,"options":0,"groupId":0} '
    '"senderShortId":12345 '
    '"lastHopLqi":155, "lastHopRssi":-50'
)

SAMPLE_DEBUG_ROUTE_ERROR = (
    "Received network/route error routeDiscoveryFailed for "
    '"0x00158d0001234567"'
)

SAMPLE_DEBUG_UART_NOISE = (
    "zh:ember:uart: --> [readFrame] Frame(10): "
    "feff0053ab89002180"
)
