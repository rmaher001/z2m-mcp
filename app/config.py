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
    retention_days: int
    max_total_mb: int


@dataclass(frozen=True)
class AppConfig:
    """Application configuration."""

    mqtt: MQTTConfig
    log: LogConfig
    timezone: str
    transport: str
    auth_token: str | None


VALID_TRANSPORTS = ("stdio", "sse")
MIN_AUTH_TOKEN_LENGTH = 32


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

    retention_days_str = os.environ.get("LOG_RETENTION_DAYS", "7")
    try:
        retention_days = int(retention_days_str)
    except ValueError:
        raise ValueError(f"LOG_RETENTION_DAYS must be a valid integer, got '{retention_days_str}'")
    if retention_days < 0:
        raise ValueError(f"LOG_RETENTION_DAYS must be >= 0, got {retention_days}")

    max_total_mb_str = os.environ.get("LOG_MAX_TOTAL_MB", "100")
    try:
        max_total_mb = int(max_total_mb_str)
    except ValueError:
        raise ValueError(f"LOG_MAX_TOTAL_MB must be a valid integer, got '{max_total_mb_str}'")
    if max_total_mb < 0:
        raise ValueError(f"LOG_MAX_TOTAL_MB must be >= 0, got {max_total_mb}")

    log = LogConfig(
        dir=os.environ.get("LOG_DIR", "/data/logs"),
        max_size_mb=max_size_mb,
        backup_count=backup_count,
        retention_days=retention_days,
        max_total_mb=max_total_mb,
    )

    timezone = os.environ.get("TZ", "UTC")

    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport not in VALID_TRANSPORTS:
        raise ValueError(
            f"MCP_TRANSPORT must be one of {VALID_TRANSPORTS}, got '{transport}'"
        )

    auth_token = os.environ.get("MCP_AUTH_TOKEN") or None
    if transport == "sse":
        if not auth_token:
            raise ValueError("MCP_AUTH_TOKEN is required when MCP_TRANSPORT=sse")
        if len(auth_token) < MIN_AUTH_TOKEN_LENGTH:
            raise ValueError(
                f"MCP_AUTH_TOKEN must be at least {MIN_AUTH_TOKEN_LENGTH} characters"
            )

    return AppConfig(
        mqtt=mqtt,
        log=log,
        timezone=timezone,
        transport=transport,
        auth_token=auth_token,
    )
