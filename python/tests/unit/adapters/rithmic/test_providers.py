"""
Tests for Rithmic instrument symbol/product helpers and the PyO3 provider.
"""

from nautilus_trader.adapters.rithmic.providers import RITHMIC_VENUE
from nautilus_trader.adapters.rithmic.providers import candidate_exchanges_for_symbol
from nautilus_trader.adapters.rithmic.providers import front_month_products_for_exchange
from nautilus_trader.adapters.rithmic.providers import normalize_rithmic_symbol
from nautilus_trader.adapters.rithmic.providers import resolve_exchange_hint
from nautilus_trader.adapters.rithmic.providers import split_exchange_from_symbol
from nautilus_trader.adapters.rithmic.providers import supported_product_for_symbol


class TestSymbolHelpers:
    def test_normalize_rithmic_symbol_strips_exchange_suffix(self):
        assert normalize_rithmic_symbol("MNQM6.CME") == "MNQM6"
        assert normalize_rithmic_symbol("MNQM6") == "MNQM6"

    def test_split_exchange_from_symbol(self):
        assert split_exchange_from_symbol("MNQM6.CME") == ("MNQM6", "CME")
        assert split_exchange_from_symbol("MNQM6") == ("MNQM6", None)

    def test_supported_product_for_symbol(self):
        assert supported_product_for_symbol("MNQM6") == "MNQ"
        assert supported_product_for_symbol("ESU5") == "ES"
        assert supported_product_for_symbol("UNKNOWN_ROOT") is None

    def test_candidate_exchanges_for_symbol(self):
        exchanges = candidate_exchanges_for_symbol("MNQ")
        assert "CME" in exchanges

        preferred = candidate_exchanges_for_symbol("MNQ", preferred_exchange="CME")
        assert preferred[0] == "CME"

    def test_front_month_products_for_exchange(self):
        products = front_month_products_for_exchange("CME")
        assert "MNQ" in products

    def test_resolve_exchange_hint_from_filters(self):
        assert resolve_exchange_hint("MNQM6", {"exchange": "CME"}) == "CME"
        assert resolve_exchange_hint("MNQM6.CME", {}) == "CME"

    def test_rithmic_venue_constant(self):
        assert RITHMIC_VENUE.value == "RITHMIC"
