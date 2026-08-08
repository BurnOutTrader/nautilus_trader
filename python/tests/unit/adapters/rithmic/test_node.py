"""
Tests for the Rithmic v2 PyO3 factories and configs via a LiveNode build.
"""

from nautilus_trader.adapters.rithmic import RithmicDataClientConfig
from nautilus_trader.adapters.rithmic import RithmicDataClientFactory
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecClientConfig
from nautilus_trader.adapters.rithmic import RithmicExecClientFactory
from nautilus_trader.adapters.rithmic.config import get_rithmic_adapter_account_id
from nautilus_trader.adapters.rithmic.config import get_rithmic_exec_client_id
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.live import LiveRiskEngineConfig
from nautilus_trader.model import TraderId


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
            app_name="TestApp",
        )
        assert config.system_name == "Apex"

    def test_exec_config_fields(self):
        config = RithmicExecClientConfig(
            environment=RithmicEnv.DEMO,
            username="test_user",
            password="test_pass",
            system_name="Apex",
            account_id="PA-123456",
            app_name="TestApp",
            trader_id="TRADER-001",
        )
        assert config.account_id == "PA-123456"
        assert config.system_name == "Apex"

    def test_live_node_builds_two_named_exec_clients_for_distinct_accounts(self):
        trader_id = TraderId.from_str("TESTER-001")
        first_account = "PA-123456"
        second_account = "PA-654321"
        first_name = get_rithmic_exec_client_id("Apex", first_account)
        second_name = get_rithmic_exec_client_id("Apex", second_account)

        node = (
            LiveNode.builder("RITHMIC-MULTI-EXEC-PYTEST-001", trader_id, Environment.LIVE)
            .with_risk_engine_config(LiveRiskEngineConfig(bypass=True))
            .add_exec_client(
                first_name,
                RithmicExecClientFactory(),
                RithmicExecClientConfig(
                    environment=RithmicEnv.DEMO,
                    username="test_user",
                    password="test_pass",
                    system_name="Apex",
                    account_id=first_account,
                    app_name="TestApp",
                    trader_id="TESTER-001",
                ),
            )
            .add_exec_client(
                second_name,
                RithmicExecClientFactory(),
                RithmicExecClientConfig(
                    environment=RithmicEnv.DEMO,
                    username="test_user",
                    password="test_pass",
                    system_name="Apex",
                    account_id=second_account,
                    app_name="TestApp",
                    trader_id="TESTER-001",
                ),
            )
            .build()
        )

        assert node.trader_id == trader_id
        assert first_name != second_name
        assert get_rithmic_adapter_account_id("Apex", first_account) != (
            get_rithmic_adapter_account_id("Apex", second_account)
        )
