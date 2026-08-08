"""
Tests for Rithmic configuration helpers.
"""

import os
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientConfig as PackageDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicEnvironment
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig as PackageExecClientConfig
from nautilus_trader.adapters.rithmic.config import get_rithmic_adapter_account_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_data_client_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_exec_client_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_profiles_from_env
from nautilus_trader.adapters.rithmic.config import load_rithmic_env_file
from nautilus_trader.adapters.rithmic.config import normalize_rithmic_client_component
from nautilus_trader.adapters.rithmic.config import parse_rithmic_env


def _assert_binding_environment(actual, expected) -> None:
    assert actual == expected


def test_public_generic_config_exports_are_binding_classes():
    assert PackageDataClientConfig is RithmicDataClientConfig
    assert PackageExecClientConfig is RithmicExecClientConfig


def test_deprecated_environment_name_is_a_compatible_alias():
    assert RithmicEnvironment is RithmicEnv
    config = RithmicDataClientConfig(
        environment=RithmicEnvironment.DEMO,
        username="test_user",
        password="test_pass",
        system_name="test_system",
        app_name="TestApp",
    )
    assert config.environment == RithmicEnv.DEMO


def test_checked_identity_helpers_match_factory_ids():
    assert get_rithmic_data_client_id("Rithmic Paper Trading") == "RITHMIC_PAPER_TRADING"
    assert get_rithmic_exec_client_id("Apex", "PA-123456") == "APEX_PA_123456"
    assert get_rithmic_adapter_account_id("Apex", "PA-123456") == "RITHMIC-APEX_PA_123456-PA-123456"


def test_identity_normalization_is_ascii_only():
    assert normalize_rithmic_client_component("ÅPEX") == "PEX"
    with pytest.raises(ValueError, match="cannot be empty"):
        normalize_rithmic_client_component("東京")


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
            app_name="TestApp",
        )

        _assert_binding_environment(config.environment, RithmicEnv.DEMO)
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.system_name == "test_system"
        assert config.app_name == "TestApp"
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
            app_name="TestApp",
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

    def test_invalid_trader_id_returns_value_error(self):
        with pytest.raises(ValueError, match="hyphen"):
            RithmicExecClientConfig(
                environment=RithmicEnv.DEMO,
                username="test_user",
                password="test_pass",
                system_name="test_system",
                account_id="ACCOUNT123",
                app_name="TestApp",
                trader_id="INVALID",
            )

    def test_from_env_accepts_account_and_trader_overrides_without_identity_env(self):
        env_vars = {
            "RITHMIC_USERNAME": "test_user",
            "RITHMIC_PASSWORD": "test_pass",
            "RITHMIC_SYSTEM_NAME": "test_system",
            "RITHMIC_APP_NAME": "TestApp",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = RithmicExecClientConfig.from_env(
                account_id="ACCOUNT123",
                trader_id="TESTER-001",
            )

        assert config.account_id == "ACCOUNT123"
        assert config.trader_id == "TESTER-001"


def test_get_rithmic_profiles_from_env():
    env_vars = {
        "RITHMIC_PROFILES": "Apex, Paper,APEX,, paper-live ",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        assert get_rithmic_profiles_from_env() == ["Apex", "Paper", "paper-live"]


def test_get_rithmic_profiles_from_adapter_dotenv_cache(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("RITHMIC_PROFILES=CachedOne,CachedTwo\n")

    with patch.dict(os.environ, {}, clear=True):
        assert load_rithmic_env_file(str(env_file)) == 1
        assert "RITHMIC_PROFILES" not in os.environ
        assert get_rithmic_profiles_from_env() == ["CachedOne", "CachedTwo"]
