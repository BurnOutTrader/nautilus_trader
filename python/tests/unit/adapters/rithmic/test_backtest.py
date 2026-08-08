from pathlib import Path

import pytest

import nautilus_trader.adapters.rithmic.backtest as rithmic_backtest
from nautilus_trader.adapters.rithmic import build_external_bar_type
from nautilus_trader.adapters.rithmic import normalize_rithmic_bar_spec
from nautilus_trader.adapters.rithmic import resolve_catalog_backtest_window
from nautilus_trader.adapters.rithmic import resolve_catalog_instrument_id
from nautilus_trader.adapters.rithmic import resolve_download_instrument_id
from nautilus_trader.adapters.rithmic import resolve_front_month_instrument_id
from nautilus_trader.model import AggressorSide
from nautilus_trader.model import AssetClass
from nautilus_trader.model import Bar
from nautilus_trader.model import BarType
from nautilus_trader.model import Currency
from nautilus_trader.model import FuturesContract
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import Symbol
from nautilus_trader.model import TradeId
from nautilus_trader.model import TradeTick
from nautilus_trader.persistence import ParquetDataCatalog


def _make_rithmic_instrument() -> FuturesContract:
    return FuturesContract(
        instrument_id=InstrumentId.from_str("MNQM6.CME.RITHMIC"),
        raw_symbol=Symbol("MNQM6"),
        underlying="MNQ",
        asset_class=AssetClass.INDEX,
        currency=Currency.from_str("USD"),
        price_precision=2,
        price_increment=Price.from_str("0.25"),
        multiplier=Quantity.from_int(1),
        lot_size=Quantity.from_int(1),
        activation_ns=1_700_000_000_000_000_000,
        expiration_ns=1_740_000_000_000_000_000,
        ts_event=0,
        ts_init=0,
    )


def _write_catalog_fixture(tmp_path: Path) -> tuple[ParquetDataCatalog, InstrumentId, BarType]:
    catalog_path = tmp_path / "catalog"
    catalog_path.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(catalog_path))
    instrument = _make_rithmic_instrument()
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    bars = [
        Bar(
            bar_type=bar_type,
            open=Price.from_str("20000.00"),
            high=Price.from_str("20010.00"),
            low=Price.from_str("19990.00"),
            close=Price.from_str("20005.00"),
            volume=Quantity.from_int(10),
            ts_event=1_710_000_000_000_000_000,
            ts_init=1_710_000_000_000_000_000,
        ),
        Bar(
            bar_type=bar_type,
            open=Price.from_str("20005.00"),
            high=Price.from_str("20015.00"),
            low=Price.from_str("20000.00"),
            close=Price.from_str("20012.00"),
            volume=Quantity.from_int(12),
            ts_event=1_710_000_060_000_000_000,
            ts_init=1_710_000_060_000_000_000,
        ),
    ]

    catalog.write_instruments([instrument])
    catalog.write_bars(bars)
    return catalog, instrument.id, bar_type


def _make_trade_ticks(instrument_id: InstrumentId) -> list[TradeTick]:
    return [
        TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str("20001.00"),
            size=Quantity.from_int(2),
            aggressor_side=AggressorSide.SELLER,
            trade_id=TradeId("1"),
            ts_event=1_710_000_000_000_000_000,
            ts_init=1_710_000_000_000_000_000,
        ),
        TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str("20002.00"),
            size=Quantity.from_int(3),
            aggressor_side=AggressorSide.SELLER,
            trade_id=TradeId("2"),
            ts_event=1_710_000_001_000_000_000,
            ts_init=1_710_000_001_000_000_000,
        ),
    ]


class TestRithmicBacktestHelpers:
    def test_normalize_rithmic_bar_spec_strips_external_suffix(self):
        assert normalize_rithmic_bar_spec("1-MINUTE-LAST-EXTERNAL") == "1-MINUTE-LAST"

    def test_build_external_bar_type_rejects_internal_specs(self):
        instrument_id = InstrumentId.from_str("MNQM6.CME.RITHMIC")

        with pytest.raises(ValueError, match="external"):
            build_external_bar_type(instrument_id, "1-MINUTE-LAST-INTERNAL")

    def test_resolve_catalog_instrument_id_uses_only_catalog_instrument(self, tmp_path):
        catalog, instrument_id, _ = _write_catalog_fixture(tmp_path)

        resolved = resolve_catalog_instrument_id(catalog)

        assert resolved == instrument_id

    def test_resolve_catalog_instrument_id_preserves_explicit_legacy_catalog_id(self, tmp_path):
        catalog, _, _ = _write_catalog_fixture(tmp_path)
        catalog_id = InstrumentId.from_str("MNQM6.RITHMIC")

        resolved = resolve_catalog_instrument_id(catalog, instrument_id=catalog_id)

        assert resolved == catalog_id

    def test_resolve_catalog_backtest_window_uses_bar_event_times(self, tmp_path):
        catalog, _, bar_type = _write_catalog_fixture(tmp_path)

        start_time, end_time = resolve_catalog_backtest_window(catalog, bar_type=bar_type)

        assert start_time == "2024-03-09T16:00:00+00:00"
        assert end_time == "2024-03-09T16:01:00+00:00"

    def test_resolve_download_instrument_id_allows_unique_root_without_exchange(self, monkeypatch):
        expected = InstrumentId.from_str("MNQM6.CME.RITHMIC")

        monkeypatch.setattr(
            rithmic_backtest,
            "resolve_front_month_instrument_id",
            lambda profile, product_code, exchange=None: expected,
        )

        resolved = resolve_download_instrument_id(
            profile=None,
            instrument_id=None,
            product_code="MNQ",
            exchange=None,
        )

        assert resolved == expected

    def test_canonical_live_id_uses_explicit_exchange_for_legacy_input(self):
        resolved = rithmic_backtest.canonical_rithmic_instrument_id(
            "MNQM6.RITHMIC",
            exchange="CME",
        )

        assert resolved == InstrumentId.from_str("MNQM6.CME.RITHMIC")

    def test_canonical_live_id_rejects_non_rithmic_venue(self):
        with pytest.raises(ValueError, match="Expected a Rithmic instrument ID"):
            rithmic_backtest.canonical_rithmic_instrument_id("ES.CME")

    def test_canonical_live_id_rejects_conflicting_exchange(self):
        with pytest.raises(ValueError, match="conflicts with encoded exchange"):
            rithmic_backtest.canonical_rithmic_instrument_id(
                "MNQM6.CME.RITHMIC",
                exchange="CBOT",
            )

    def test_canonical_live_id_normalizes_symbol_and_exchange_case(self):
        resolved = rithmic_backtest.canonical_rithmic_instrument_id("mnqm6.cme.rithmic")

        assert resolved == InstrumentId.from_str("MNQM6.CME.RITHMIC")

    def test_resolved_contract_metadata_reads_exchange_from_canonical_id(self):
        metadata = rithmic_backtest._resolved_contract_metadata(_make_rithmic_instrument())

        assert metadata.instrument_id == InstrumentId.from_str("MNQM6.CME.RITHMIC")
        assert metadata.symbol == "MNQM6"
        assert metadata.exchange == "CME"

    def test_resolve_front_month_instrument_id_requires_exchange_for_ambiguous_root(self):
        with pytest.raises(ValueError, match="MYM"):
            resolve_front_month_instrument_id(
                profile=None,
                product_code="MYM",
                exchange=None,
            )

    def test_download_bars_to_catalog_writes_direct_exact_contract(self, tmp_path, monkeypatch):
        instrument = _make_rithmic_instrument()
        bar_type = build_external_bar_type(instrument.id, "1-MINUTE-LAST")
        bars = [
            Bar(
                bar_type=bar_type,
                open=Price.from_str("20000.00"),
                high=Price.from_str("20010.00"),
                low=Price.from_str("19990.00"),
                close=Price.from_str("20005.00"),
                volume=Quantity.from_int(10),
                ts_event=1_710_000_000_000_000_000,
                ts_init=1_710_000_000_000_000_000,
            ),
        ]
        fake_session = object()

        async def fake_open(profile):
            return fake_session

        async def fake_close(session):
            assert session is fake_session

        async def fake_resolve(*, session, instrument_id, product_code, exchange):
            assert session is fake_session
            assert instrument_id == "MNQM6.CME.RITHMIC"
            assert product_code is None
            assert exchange is None
            return rithmic_backtest._ResolvedDownloadContract(
                instrument=instrument,
                instrument_id=instrument.id,
                symbol="MNQM6",
                exchange="CME",
            )

        async def fake_request_bars(*, session, resolved, bar_type, start, end, limit):
            assert session is fake_session
            assert resolved.instrument_id == instrument.id
            assert limit == 0
            return bars

        monkeypatch.setattr(rithmic_backtest, "_open_historical_session", fake_open)
        monkeypatch.setattr(rithmic_backtest, "_close_historical_session", fake_close)
        monkeypatch.setattr(rithmic_backtest, "_resolve_download_contract_async", fake_resolve)
        monkeypatch.setattr(rithmic_backtest, "_request_historical_bars_async", fake_request_bars)

        result = rithmic_backtest.download_bars_to_catalog(
            profile=None,
            catalog_path=tmp_path / "bars-catalog",
            instrument_id="MNQM6.CME.RITHMIC",
            exchange=None,
            bar_spec="1-MINUTE-LAST",
            start_time="2024-03-09T16:00:00Z",
            end_time="2024-03-09T16:01:00Z",
        )

        catalog = ParquetDataCatalog(str(result.catalog_path))

        assert result.instrument_id == instrument.id
        assert result.instrument_count == 1
        assert result.bar_count == 1
        assert len(catalog.instruments(instrument_ids=[instrument.id.value])) == 1
        assert len(catalog.query_bars(identifiers=[str(bar_type)])) == 1

    def test_download_trade_ticks_to_catalog_writes_direct_exact_contract(
        self,
        tmp_path,
        monkeypatch,
    ):
        instrument = _make_rithmic_instrument()
        ticks = _make_trade_ticks(instrument.id)
        fake_session = object()

        async def fake_open(profile):
            return fake_session

        async def fake_close(session):
            assert session is fake_session

        async def fake_resolve(*, session, instrument_id, product_code, exchange):
            assert session is fake_session
            assert instrument_id == "MNQM6.CME.RITHMIC"
            assert product_code is None
            assert exchange is None
            return rithmic_backtest._ResolvedDownloadContract(
                instrument=instrument,
                instrument_id=instrument.id,
                symbol="MNQM6",
                exchange="CME",
            )

        async def fake_request_ticks(*, session, resolved, start, end, limit):
            assert session is fake_session
            assert resolved.instrument_id == instrument.id
            assert limit == 0
            return ticks

        monkeypatch.setattr(rithmic_backtest, "_open_historical_session", fake_open)
        monkeypatch.setattr(rithmic_backtest, "_close_historical_session", fake_close)
        monkeypatch.setattr(rithmic_backtest, "_resolve_download_contract_async", fake_resolve)
        monkeypatch.setattr(
            rithmic_backtest,
            "_request_historical_trade_ticks_async",
            fake_request_ticks,
        )

        result = rithmic_backtest.download_trade_ticks_to_catalog(
            profile=None,
            catalog_path=tmp_path / "ticks-catalog",
            instrument_id="MNQM6.CME.RITHMIC",
            exchange=None,
            start_time="2024-03-09T16:00:00Z",
            end_time="2024-03-09T16:01:00Z",
        )

        catalog = ParquetDataCatalog(str(result.catalog_path))

        assert result.instrument_id == instrument.id
        assert result.instrument_count == 1
        assert result.tick_count == 2
        assert len(catalog.instruments(instrument_ids=[instrument.id.value])) == 1
        assert len(catalog.query_trade_ticks(identifiers=[instrument.id.value])) == 2
