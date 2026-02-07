"""Configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MQTTConfig:
    """MQTT broker connection configuration."""

    host: str
    port: int
    username: str
    password: str


@dataclass(frozen=True)
class LogConfig:
    """Z2M log capture and persistence configuration."""

    dir: str
    max_size_mb: int
    backup_count: int


@dataclass(frozen=True)
class AppConfig:
    """Application configuration."""

    mqtt: MQTTConfig
    log: LogConfig
    timezone: str


def load_config() -> AppConfig:
    """Load configuration from environment variables.

    Raises:
        ValueError: If required environment variables are missing.
    """
    missing = []
    for var in ("MQTT_HOST", "MQTT_USERNAME", "MQTT_PASSWORD"):
        if not os.environ.get(var):
            missing.append(var)

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    port_str = os.environ.get("MQTT_PORT", "1883")
    try:
        port = int(port_str)
    except ValueError:
        raise ValueError(f"MQTT_PORT must be a valid integer, got '{port_str}'")
    if not 1 <= port <= 65535:
        raise ValueError(f"MQTT_PORT must be between 1 and 65535, got {port}")

    mqtt = MQTTConfig(
        host=os.environ["MQTT_HOST"],
        port=port,
        username=os.environ["MQTT_USERNAME"],
        password=os.environ["MQTT_PASSWORD"],
    )

    max_size_str = os.environ.get("LOG_MAX_SIZE_MB", "10")
    try:
        max_size_mb = int(max_size_str)
    except ValueError:
        raise ValueError(f"LOG_MAX_SIZE_MB must be a valid integer, got '{max_size_str}'")

    backup_count_str = os.environ.get("LOG_BACKUP_COUNT", "3")
    try:
        backup_count = int(backup_count_str)
    except ValueError:
        raise ValueError(f"LOG_BACKUP_COUNT must be a valid integer, got '{backup_count_str}'")

    log = LogConfig(
        dir=os.environ.get("LOG_DIR", "/data/logs"),
        max_size_mb=max_size_mb,
        backup_count=backup_count,
    )

    timezone = os.environ.get("TZ", "UTC")

    return AppConfig(mqtt=mqtt, log=log, timezone=timezone)
