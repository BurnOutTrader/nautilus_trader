"""
Tests for the Rithmic v2 PyO3 factories and LiveNode wiring.
"""

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientFactory
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientFactory


class TestRithmicV2Factories:
    def test_instantiate_data_client_factory(self):
        factory = RithmicDataClientFactory()
        assert isinstance(factory, RithmicDataClientFactory)

    def test_instantiate_exec_client_factory(self):
        factory = RithmicExecClientFactory()
        assert isinstance(factory, RithmicExecClientFactory)

    def test_data_client_factory_name(self):
        assert RithmicDataClientFactory().name() == "RITHMIC"

    def test_exec_client_factory_name(self):
        assert RithmicExecClientFactory().name() == "RITHMIC"

    def test_data_config_constructs_with_env_fields(self):
        config = RithmicDataClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="Apex",
        )

        assert config.system_name == "Apex"
        assert config.username == "test_user"

    def test_exec_config_constructs_with_account(self):
        config = RithmicExecClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="Apex",
            account_id="PA-123456",
        )

        assert config.account_id == "PA-123456"
