"""
Tests for Rithmic configuration helpers.
"""

import os
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientConfig as PackageDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig as PackageExecClientConfig
from nautilus_trader.adapters.rithmic.config import get_rithmic_profiles_from_env
from nautilus_trader.adapters.rithmic.config import parse_rithmic_env


def _assert_binding_environment(actual, expected) -> None:
    assert repr(actual) == repr(expected)


def test_public_generic_config_exports_are_binding_classes():
    assert PackageDataClientConfig is RithmicDataClientConfig
    assert PackageExecClientConfig is RithmicExecClientConfig


class TestParseRithmicEnv:
    def test_parse_demo_aliases(self):
        assert parse_rithmic_env("demo") == RithmicEnv.DEMO
        assert parse_rithmic_env("DEMO") == RithmicEnv.DEMO
        assert parse_rithmic_env("paper") == RithmicEnv.DEMO
        assert parse_rithmic_env(None) == RithmicEnv.DEMO

    def test_parse_live_aliases(self):
        assert parse_rithmic_env("live") == RithmicEnv.LIVE
        assert parse_rithmic_env("LIVE") == RithmicEnv.LIVE
        assert parse_rithmic_env("prod") == RithmicEnv.LIVE
        assert parse_rithmic_env("production") == RithmicEnv.LIVE

    def test_parse_test_alias(self):
        assert parse_rithmic_env("test") == RithmicEnv.TEST
        assert parse_rithmic_env("TEST") == RithmicEnv.TEST

    def test_parse_invalid_value(self):
        with pytest.raises(ValueError, match="expected demo, live, or test"):
            parse_rithmic_env("invalid")


class TestRithmicDataClientConfig:
    def test_create_binding_config(self):
        config = RithmicDataClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="test_system",
        )

        _assert_binding_environment(config.environment, RithmicEnv.DEMO)
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.system_name == "test_system"
        assert config.app_name == ""
        assert config.app_version == "1.0"
        assert config.fcm_id is None
        assert config.ib_id is None
        assert config.server is None
        assert config.alt_server is None
        assert config.enable_history is False

    def test_binding_config_from_env(self):
        env_vars = {
            "RITHMIC_ENV": "demo",
            "RITHMIC_USERNAME": "env_user",
            "RITHMIC_PASSWORD": "env_pass",
            "RITHMIC_SYSTEM_NAME": "env_system",
            "RITHMIC_APP_NAME": "OwnApp",
            "RITHMIC_SERVER": "Chicago",
            "RITHMIC_ALT_SERVER": "Sydney",
            "RITHMIC_ENABLE_HISTORY": "true",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = RithmicDataClientConfig.from_env()
            _assert_binding_environment(config.environment, RithmicEnv.DEMO)
            assert config.username == "env_user"
            assert config.password == "env_pass"
            assert config.system_name == "env_system"
            assert config.app_name == "OwnApp"
            assert config.server == "Chicago"
            assert config.alt_server == "Sydney"
            assert config.enable_history is True

    def test_binding_config_from_env_profile(self):
        env_vars = {
            "RITHMIC_APEX_ENV": "live",
            "RITHMIC_APEX_USERNAME": "profile_user",
            "RITHMIC_APEX_PASSWORD": "profile_pass",
            "RITHMIC_APEX_SYSTEM_NAME": "Apex",
            "RITHMIC_APEX_APP_NAME": "ProfileApp",
            "RITHMIC_APEX_SERVER": "Frankfurt",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = RithmicDataClientConfig.from_env("Apex")
            _assert_binding_environment(config.environment, RithmicEnv.LIVE)
            assert config.username == "profile_user"
            assert config.password == "profile_pass"
            assert config.system_name == "Apex"
            assert config.app_name == "ProfileApp"
            assert config.server == "Frankfurt"


class TestRithmicExecClientConfig:
    def test_create_binding_config(self):
        config = RithmicExecClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="test_system",
            account_id="ACCOUNT123",
        )

        _assert_binding_environment(config.environment, RithmicEnv.DEMO)
        assert config.account_id == "ACCOUNT123"
        assert config.server is None
        assert config.alt_server is None
        assert config.execution_replay_lookback_secs == 86_400

    def test_binding_config_from_env(self):
        env_vars = {
            "RITHMIC_ENV": "demo",
            "RITHMIC_USERNAME": "env_user",
            "RITHMIC_PASSWORD": "env_pass",
            "RITHMIC_SYSTEM_NAME": "env_system",
            "RITHMIC_ACCOUNT_ID": "ENV_ACCOUNT",
            "RITHMIC_APP_NAME": "OwnApp",
            "RITHMIC_SERVER": "Chicago",
            "RITHMIC_ALT_SERVER": "Sydney",
            "RITHMIC_EXECUTION_REPLAY_LOOKBACK_SECS": "7200",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = RithmicExecClientConfig.from_env()
            _assert_binding_environment(config.environment, RithmicEnv.DEMO)
            assert config.account_id == "ENV_ACCOUNT"
            assert config.app_name == "OwnApp"
            assert config.server == "Chicago"
            assert config.alt_server == "Sydney"
            assert config.execution_replay_lookback_secs == 7200


def test_get_rithmic_profiles_from_env():
    env_vars = {
        "RITHMIC_PROFILES": "Apex, Paper,APEX,, paper-live ",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        assert get_rithmic_profiles_from_env() == ["Apex", "Paper", "paper-live"]
