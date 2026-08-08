"""
Tests for the PyO3 Rithmic data client bindings.
"""

import pytest

from nautilus_trader.adapters.rithmic import RithmicDataClient
from nautilus_trader.adapters.rithmic import RithmicEndOfDayPrices
from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicGateway
from nautilus_trader.adapters.rithmic import RithmicIndicatorPrices
from nautilus_trader.adapters.rithmic import RithmicMinuteVolumeProfileBar
from nautilus_trader.adapters.rithmic import RithmicOpenInterest
from nautilus_trader.adapters.rithmic import RithmicOrderPriceLimits
from nautilus_trader.adapters.rithmic import RithmicQuoteStatistics
from nautilus_trader.adapters.rithmic import RithmicSymbolMarginRate
from nautilus_trader.adapters.rithmic import RithmicTradeStatistics
from nautilus_trader.adapters.rithmic import RithmicVolumeAtPrice
from nautilus_trader.adapters.rithmic.config import to_binding_environment


def _test_client() -> RithmicDataClient:
    gateway = RithmicGateway(
        environment=to_binding_environment(RithmicEnv.DEMO),
        username="test_user",
        password="test_pass",
        system_name="Apex",
        fcm_id="",
        ib_id="",
        account_id="",
        app_name="TestApp",
    )
    return RithmicDataClient(gateway)


def test_all_emitted_custom_data_types_are_importable():
    custom_types = (
        RithmicTradeStatistics,
        RithmicQuoteStatistics,
        RithmicIndicatorPrices,
        RithmicOpenInterest,
        RithmicEndOfDayPrices,
        RithmicOrderPriceLimits,
        RithmicSymbolMarginRate,
        RithmicVolumeAtPrice,
        RithmicMinuteVolumeProfileBar,
    )

    assert all(value.__module__ == "nautilus_trader.adapters.rithmic" for value in custom_types)


def test_gateway_rejects_empty_app_name_at_construction():
    with pytest.raises(ValueError, match="app_name cannot be empty"):
        RithmicGateway(
            environment=to_binding_environment(RithmicEnv.DEMO),
            username="test_user",
            password="test_pass",
            system_name="Apex",
            fcm_id="",
            ib_id="",
            account_id="",
            app_name="",
            enable_order=False,
            enable_pnl=False,
        )


class TestPyRithmicDataClient:
    def test_instantiation(self):
        client = _test_client()
        assert client is not None

    def test_is_connected_false_when_gateway_not_connected(self):
        client = _test_client()
        assert client.is_connected is False

    def test_subscription_counts_initially_zero(self):
        client = _test_client()
        assert client.subscription_count == 0
        assert client.bar_subscription_count == 0
        assert client.book_subscription_count == 0

    def test_subscriptions_initially_empty(self):
        client = _test_client()
        assert client.subscriptions() == []
        assert client.bar_subscriptions() == []

    def test_not_subscribed_initially(self):
        client = _test_client()
        assert client.is_subscribed("MNQM6", "CME") is False
        assert client.is_subscribed_bars("MNQM6", "CME", "MinuteBar", 1) is False

    def test_set_and_clear_data_callback(self):
        client = _test_client()

        def callback(event):
            pass

        client.set_data_callback(callback)
        client.clear_data_callback()
