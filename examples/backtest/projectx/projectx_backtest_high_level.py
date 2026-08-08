#!/usr/bin/env python3
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

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model import Bar
from nautilus_trader.model import BarType
from nautilus_trader.model import InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")
    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the parent directory for the ProjectX example catalog, "
            "for example /tmp/nautilus-data/examples/projectx. "
            "This script reads bars from <NAUTILUS_PATH>/catalog.",
        )
    return Path(nautilus_path).expanduser().resolve()


CATALOG_PATH = _require_example_root() / "catalog"
INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
BAR_SPEC = "1-MINUTE-LAST"
TRADE_SIZE = Decimal(1)
FAST_EMA = 10
SLOW_EMA = 20
STARTING_BALANCE = "100000 USD"
BACKTEST_START = None
BACKTEST_END = None


def _resolve_time_range(catalog: ParquetDataCatalog, bar_type: BarType) -> tuple[str, str]:
    bars = catalog.bars(bar_types=[str(bar_type)])
    if not bars:
        raise RuntimeError(
            f"No bars found for {bar_type} in catalog {CATALOG_PATH}. "
            "Run examples/backtest/projectx/projectx_download_bars.py first.",
        )

    start_time = BACKTEST_START
    end_time = BACKTEST_END
    if start_time and end_time:
        return start_time, end_time

    first_bar = pd.Timestamp(bars[0].ts_init, unit="ns", tz="UTC")
    last_bar = pd.Timestamp(bars[-1].ts_init, unit="ns", tz="UTC")
    return first_bar.isoformat(), last_bar.isoformat()


def main() -> None:
    catalog = ParquetDataCatalog(str(CATALOG_PATH))
    instrument_id = INSTRUMENT_ID
    bar_type = BarType.from_str(f"{instrument_id}-{BAR_SPEC}-EXTERNAL")
    instruments = catalog.instruments(instrument_ids=[instrument_id.value])
    if not instruments:
        raise RuntimeError(
            f"Instrument {instrument_id} not found in catalog {CATALOG_PATH}. "
            "Run examples/backtest/projectx/projectx_download_bars.py first.",
        )

    start_time, end_time = _resolve_time_range(catalog, bar_type)

    venue_configs = [
        BacktestVenueConfig(
            name="PROJECTX",
            oms_type="NETTING",
            account_type="CASH",
            base_currency="USD",
            starting_balances=[STARTING_BALANCE],
        ),
    ]

    data_configs = [
        BacktestDataConfig(
            catalog_path=str(CATALOG_PATH),
            data_cls=Bar,
            instrument_id=instrument_id,
            bar_spec=BAR_SPEC,
            start_time=start_time,
            end_time=end_time,
        ),
    ]

    strategies = [
        ImportableStrategyConfig(
            strategy_path="nautilus_trader.examples.strategies.ema_cross:EMACross",
            config_path="nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
            config={
                "instrument_id": instrument_id,
                "bar_type": str(bar_type),
                "fast_ema_period": FAST_EMA,
                "slow_ema_period": SLOW_EMA,
                "trade_size": TRADE_SIZE,
            },
        ),
    ]

    config = BacktestRunConfig(
        engine=BacktestEngineConfig(strategies=strategies),
        data=data_configs,
        venues=venue_configs,
    )

    node = BacktestNode(configs=[config])
    results = node.run()

    print(f"Catalog path: {CATALOG_PATH}")
    print(f"Instrument ID: {instrument_id}")
    print(f"Bar type: {bar_type}")
    print(f"Backtest range: {start_time} -> {end_time}")
    print(results)


if __name__ == "__main__":
    main()
