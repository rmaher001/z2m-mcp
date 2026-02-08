"""Tests for configuration loading."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from app.config import load_config


VALID_ENV = {
    "MQTT_HOST": "mqtt.example.com",
    "MQTT_USERNAME": "user",
    "MQTT_PASSWORD": "pass",
}


class TestLoadConfig:
    def test_loads_with_defaults(self) -> None:
        with patch.dict(os.environ, VALID_ENV, clear=True):
            config = load_config()

        assert config.mqtt.host == "mqtt.example.com"
        assert config.mqtt.port == 1883
        assert config.mqtt.username == "user"
        assert config.mqtt.password == "pass"
        assert config.log.dir == "/data/logs"
        assert config.log.max_size_mb == 10
        assert config.log.backup_count == 3
        assert config.log.retention_days == 7
        assert config.log.max_total_mb == 100
        assert config.timezone == "UTC"

    def test_loads_custom_values(self) -> None:
        env = {
            **VALID_ENV,
            "MQTT_HOST": "10.0.0.1",
            "MQTT_PORT": "8883",
            "LOG_DIR": "/tmp/z2m-logs",
            "LOG_MAX_SIZE_MB": "25",
            "LOG_BACKUP_COUNT": "5",
            "LOG_RETENTION_DAYS": "14",
            "LOG_MAX_TOTAL_MB": "200",
            "TZ": "America/Los_Angeles",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config()

        assert config.mqtt.host == "10.0.0.1"
        assert config.mqtt.port == 8883
        assert config.log.dir == "/tmp/z2m-logs"
        assert config.log.max_size_mb == 25
        assert config.log.backup_count == 5
        assert config.log.retention_days == 14
        assert config.log.max_total_mb == 200
        assert config.timezone == "America/Los_Angeles"

    def test_missing_mqtt_host_raises(self) -> None:
        env = {"MQTT_USERNAME": "user", "MQTT_PASSWORD": "pass"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MQTT_HOST"):
                load_config()

    def test_missing_mqtt_username_raises(self) -> None:
        env = {"MQTT_HOST": "x", "MQTT_PASSWORD": "pass"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MQTT_USERNAME"):
                load_config()

    def test_missing_mqtt_password_raises(self) -> None:
        env = {"MQTT_HOST": "x", "MQTT_USERNAME": "user"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MQTT_PASSWORD"):
                load_config()

    def test_all_missing_vars_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="MQTT_HOST.*MQTT_USERNAME.*MQTT_PASSWORD"):
                load_config()

    def test_invalid_mqtt_port_non_numeric(self) -> None:
        env = {**VALID_ENV, "MQTT_PORT": "abc"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MQTT_PORT must be a valid integer"):
                load_config()

    def test_invalid_mqtt_port_out_of_range(self) -> None:
        env = {**VALID_ENV, "MQTT_PORT": "99999"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MQTT_PORT must be between"):
                load_config()

    def test_invalid_log_max_size(self) -> None:
        env = {**VALID_ENV, "LOG_MAX_SIZE_MB": "abc"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_MAX_SIZE_MB must be a valid integer"):
                load_config()

    def test_invalid_log_backup_count(self) -> None:
        env = {**VALID_ENV, "LOG_BACKUP_COUNT": "xyz"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_BACKUP_COUNT must be a valid integer"):
                load_config()

    def test_invalid_log_retention_days(self) -> None:
        env = {**VALID_ENV, "LOG_RETENTION_DAYS": "abc"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_RETENTION_DAYS must be a valid integer"):
                load_config()

    def test_invalid_log_max_total_mb(self) -> None:
        env = {**VALID_ENV, "LOG_MAX_TOTAL_MB": "abc"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_MAX_TOTAL_MB must be a valid integer"):
                load_config()

    def test_negative_log_retention_days(self) -> None:
        env = {**VALID_ENV, "LOG_RETENTION_DAYS": "-1"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_RETENTION_DAYS must be >= 0"):
                load_config()

    def test_negative_log_max_total_mb(self) -> None:
        env = {**VALID_ENV, "LOG_MAX_TOTAL_MB": "-5"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="LOG_MAX_TOTAL_MB must be >= 0"):
                load_config()

    def test_config_is_frozen(self) -> None:
        with patch.dict(os.environ, VALID_ENV, clear=True):
            config = load_config()

        with pytest.raises(AttributeError):
            config.mqtt.host = "other"  # type: ignore[misc]
