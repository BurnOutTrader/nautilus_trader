"""
Tests for the Rithmic v2 PyO3 factories and configs via a LiveNode build.
"""

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientFactory
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientFactory


class TestRithmicV2Factories:
    def test_instantiate_data_client_factory(self):
        factory = RithmicDataClientFactory()
        assert factory.name() == "RITHMIC"

    def test_instantiate_exec_client_factory(self):
        factory = RithmicExecClientFactory()
        assert factory.name() == "RITHMIC"

    def test_data_config_from_env_fields(self):
        config = RithmicDataClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="Apex",
        )
        assert config.system_name == "Apex"

    def test_exec_config_fields(self):
        config = RithmicExecClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="Apex",
            account_id="PA-123456",
            trader_id="TRADER-001",
        )
        assert config.account_id == "PA-123456"
        assert config.system_name == "Apex"
