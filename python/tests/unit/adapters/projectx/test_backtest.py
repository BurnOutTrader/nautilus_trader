# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
#
#  Licensed under the GNU Lesser General Public License Version 3.0 or later.
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from nautilus_trader.adapters.projectx import backtest as projectx_backtest
from nautilus_trader.adapters.projectx import build_external_bar_type
from nautilus_trader.model import InstrumentId


def _bars_payload():
    from nautilus_trader.model import Bar
    from nautilus_trader.model import BarType
    from nautilus_trader.model import Price
    from nautilus_trader.model import Quantity

    bar_type = BarType.from_str("MNQM26.PROJECTX-1-MINUTE-LAST-EXTERNAL")
    return [
        Bar(
            bar_type=bar_type,
            open=Price.from_str("20000.00"),
            high=Price.from_str("20010.00"),
            low=Price.from_str("19990.00"),
            close=Price.from_str("20005.00"),
            volume=Quantity.from_int(10),
            ts_event=1_742_947_200_000_000_000,
            ts_init=1_742_947_200_000_000_000,
        ),
        Bar(
            bar_type=bar_type,
            open=Price.from_str("20005.00"),
            high=Price.from_str("20015.00"),
            low=Price.from_str("20000.00"),
            close=Price.from_str("20012.00"),
            volume=Quantity.from_int(12),
            ts_event=1_742_947_260_000_000_000,
            ts_init=1_742_947_260_000_000_000,
        ),
    ]


class _FakeInstrument:
    id = InstrumentId.from_str("MNQM26.PROJECTX")
    info = {"projectx_contract_id": "CON.F.US.MNQ.M26"}


class _FakeHttpClient:
    last_instance: _FakeHttpClient | None = None

    def __init__(self) -> None:
        type(self).last_instance = self
        self.started = False
        self.stopped = False
        self.live_calls: list[bool] = []

    @classmethod
    def from_config(cls, _config) -> _FakeHttpClient:
        return cls()

    async def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    async def available_instruments(self, live, active_only, product_root):
        self.live_calls.append(live)
        return [_FakeInstrument()]

    async def retrieve_bars(
        self,
        contract_id,
        bar_type,
        start_time,
        end_time,
        unit,
        unit_number,
        limit,
        live,
    ):
        self.live_calls.append(live)
        return _bars_payload()


class _FakeCatalog:
    def __init__(self, path: str) -> None:
        self.path = path

    def write_data(self, _data) -> None:
        pass

    def bars(self, **_kwargs) -> list[object]:
        return [object(), object()]


def test_download_bars_to_catalog_uses_live_history_fallback_flag(monkeypatch, tmp_path):
    monkeypatch.setattr(projectx_backtest, "load_projectx_env", lambda: None)
    monkeypatch.setenv("PROJECTX_USERNAME", "test-user")
    monkeypatch.setenv("PROJECTX_API_KEY", "test-api-key")
    monkeypatch.setattr(projectx_backtest, "ProjectXHttpClient", _FakeHttpClient)
    monkeypatch.setattr(projectx_backtest, "ParquetDataCatalog", _FakeCatalog)

    result = projectx_backtest.download_bars_to_catalog(
        catalog_path=tmp_path,
        instrument_id=InstrumentId.from_str("MNQM26.PROJECTX"),
        bar_spec="1-MINUTE-LAST",
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T01:00:00Z",
        limit=500,
        market_data_live=True,
        allow_live_history_fallback=True,
    )

    fake_client = _FakeHttpClient.last_instance
    assert fake_client is not None
    assert fake_client.started is True
    assert fake_client.stopped is True
    assert fake_client.live_calls == [True, True]
    assert result.bar_count == 2
    assert result.instrument_count == 1
    assert result.bar_count == 2


def test_download_bars_to_catalog_defaults_to_no_live_history_fallback(monkeypatch, tmp_path):
    monkeypatch.setattr(projectx_backtest, "load_projectx_env", lambda: None)
    monkeypatch.setenv("PROJECTX_USERNAME", "test-user")
    monkeypatch.setenv("PROJECTX_API_KEY", "test-api-key")
    monkeypatch.setattr(projectx_backtest, "ProjectXHttpClient", _FakeHttpClient)
    monkeypatch.setattr(projectx_backtest, "ParquetDataCatalog", _FakeCatalog)

    projectx_backtest.download_bars_to_catalog(
        catalog_path=tmp_path,
        instrument_id="MNQM26.PROJECTX",
        bar_spec="1-MINUTE-LAST",
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T01:00:00Z",
    )

    fake_client = _FakeHttpClient.last_instance
    assert fake_client is not None
    assert fake_client.live_calls == [False, False]


def test_build_external_bar_type_normalizes_spec():
    instrument_id = InstrumentId.from_str("MNQM26.PROJECTX")

    assert str(build_external_bar_type(instrument_id, "1-MINUTE-LAST-EXTERNAL")) == (
        "MNQM26.PROJECTX-1-MINUTE-LAST-EXTERNAL"
    )
