"""
Tests for Rithmic execution client (Gateway Python bindings).
"""

import os

import pytest

from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicExecutionClient
from nautilus_trader.adapters.rithmic import RithmicGateway
from nautilus_trader.adapters.rithmic.config import to_binding_environment


def _test_gateway() -> RithmicGateway:
    environment = to_binding_environment(RithmicEnv.DEMO)
    return RithmicGateway(
        environment=environment,
        username="test_user",
        password="test_pass",
        system_name="Apex",
        fcm_id="test_fcm",
        ib_id="test_ib",
        account_id="test_account",
        app_name="TestApp",
    )


class TestPyRithmicExecutionClient:
    """
    Tests for PyRithmicExecutionClient (Gateway Python bindings).
    """

    def test_execution_client_instantiation(self):
        """
        Test that RithmicExecutionClient can be instantiated with a gateway.
        """
        gateway = _test_gateway()

        client = RithmicExecutionClient(gateway, "test_account")
        assert client is not None

    def test_execution_client_rejects_unsupported_native_bracket_state_path(self, tmp_path):
        """
        Test that the raw binding fails fast for unsupported persisted bracket state.
        """
        gateway = _test_gateway()

        with pytest.raises(ValueError, match="native_bracket_state_path"):
            RithmicExecutionClient(
                gateway,
                "test_account",
                native_bracket_state_path=str(tmp_path / "rithmic-native-brackets.json"),
            )

    def test_execution_client_is_connected_property(self):
        gateway = _test_gateway()
        client = RithmicExecutionClient(gateway, "test_account")
        assert client.is_connected is False

    def test_execution_client_account_id_property(self):
        gateway = _test_gateway()
        client = RithmicExecutionClient(gateway, "test_account")
        assert client.account_id == "test_account"

    def test_execution_client_orders_count_initially_zero(self):
        gateway = _test_gateway()
        client = RithmicExecutionClient(gateway, "test_account")
        assert client.orders_count == 0

    def test_execution_client_open_orders_empty(self):
        gateway = _test_gateway()
        client = RithmicExecutionClient(gateway, "test_account")
        assert client.open_orders() == []

    @pytest.fixture
    def live_credentials(self):
        return bool(
            os.environ.get("RITHMIC_USERNAME") and os.environ.get("RITHMIC_APP_NAME"),
        )

    @pytest.mark.asyncio
    async def test_execution_client_connected_after_gateway_connect(self, live_credentials):
        if not live_credentials:
            pytest.skip("Live credentials not available")

        gateway = RithmicGateway.from_env()
        await gateway.connect()

        try:
            account_id = os.environ.get("RITHMIC_ACCOUNT_ID", "")
            client = RithmicExecutionClient(gateway, account_id)
            assert client.is_connected is True
        finally:
            await gateway.disconnect()

    @pytest.mark.asyncio
    async def test_list_accounts_after_connect(self, live_credentials):
        if not live_credentials:
            pytest.skip("Live credentials not available")

        gateway = RithmicGateway.from_env()
        await gateway.connect()

        try:
            accounts = await gateway.list_accounts()
            assert isinstance(accounts, list)
        finally:
            await gateway.disconnect()

    @pytest.mark.asyncio
    async def test_positions_after_connect(self, live_credentials):
        if not live_credentials:
            pytest.skip("Live credentials not available")

        gateway = RithmicGateway.from_env()
        await gateway.connect()

        try:
            account_id = os.environ.get("RITHMIC_ACCOUNT_ID", "")
            client = RithmicExecutionClient(gateway, account_id)
            positions = await client.show_brackets()
            assert isinstance(positions, list)
        finally:
            await gateway.disconnect()
