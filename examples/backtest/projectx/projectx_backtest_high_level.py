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
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
from nautilus_trader.backtest import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestNode,
    BacktestRunConfig,
    BacktestVenueConfig,
)
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model import (
    AccountType,
    BarType,
    BookType,
    Currency,
    InstrumentId,
    OmsType,
)
from nautilus_trader.persistence import ParquetDataCatalog

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


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


def _resolve_time_range(
    catalog: ParquetDataCatalog, bar_type: BarType
) -> tuple[int, int]:
    bars = catalog.query_bars(identifiers=[str(bar_type)])
    if not bars:
        raise RuntimeError(
            f"No bars found for {bar_type} in catalog {CATALOG_PATH}. "
            "Run examples/backtest/projectx/projectx_download_bars.py first.",
        )

    start_time = (
        int(pd.Timestamp(BACKTEST_START).value)
        if BACKTEST_START is not None
        else int(bars[0].ts_init)
    )
    end_time = (
        int(pd.Timestamp(BACKTEST_END).value)
        if BACKTEST_END is not None
        else int(bars[-1].ts_init)
    )
    if start_time > end_time:
        raise ValueError(f"Backtest start {start_time} is after end {end_time}")
    return start_time, end_time


def _build_run_config(
    *,
    instrument_id: InstrumentId,
    bar_type: BarType,
    start_time: int,
    end_time: int,
) -> tuple[BacktestRunConfig, list[ImportableStrategyConfig]]:
    venue_config = BacktestVenueConfig(
        name="PROJECTX",
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        book_type=BookType.L1_MBP,
        base_currency=Currency.from_str("USD"),
        starting_balances=[STARTING_BALANCE],
        bar_execution=True,
    )
    data_config = BacktestDataConfig(
        catalog_path=str(CATALOG_PATH),
        data_type="Bar",
        instrument_id=instrument_id,
        bar_spec=bar_type.spec,
        start_time=start_time,
        end_time=end_time,
    )
    strategies = [
        ImportableStrategyConfig(
            strategy_path=(
                "examples.live.projectx.projectx_ema_cross:ProjectXEMACrossStrategy"
            ),
            config_path=(
                "examples.live.projectx.projectx_ema_cross:ProjectXEMACrossStrategyConfig"
            ),
            config={
                "instrument_id": instrument_id,
                "bar_type": str(bar_type),
                "fast_ema_period": FAST_EMA,
                "slow_ema_period": SLOW_EMA,
                "trade_size": TRADE_SIZE,
                "request_bars": False,
                "cleanup_on_stop": False,
            },
        ),
    ]
    return (
        BacktestRunConfig(
            engine=BacktestEngineConfig(),
            data=[data_config],
            venues=[venue_config],
        ),
        strategies,
    )


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

    config, strategies = _build_run_config(
        instrument_id=instrument_id,
        bar_type=bar_type,
        start_time=start_time,
        end_time=end_time,
    )

    node = BacktestNode(configs=[config])
    node.build()
    for strategy in strategies:
        node.add_strategy_from_config(config.id, strategy)
    results = node.run()

    print(f"Catalog path: {CATALOG_PATH}")
    print(f"Instrument ID: {instrument_id}")
    print(f"Bar type: {bar_type}")
    print(
        "Backtest range: "
        f"{pd.Timestamp(start_time, unit='ns', tz='UTC').isoformat()} -> "
        f"{pd.Timestamp(end_time, unit='ns', tz='UTC').isoformat()}"
    )
    print(results)


if __name__ == "__main__":
    main()
