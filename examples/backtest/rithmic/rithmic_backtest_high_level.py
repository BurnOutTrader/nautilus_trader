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

"""
If the catalog is empty, bootstrap instrument definitions first with
`crates/adapters/rithmic/examples/write_instruments_catalog.rs`, then load bar
data with `examples/backtest/rithmic/rithmic_download_bars.py` or
`crates/adapters/rithmic/examples/download_bars.rs`. When you use the catalog
writer for historical/backtest work, set `RITHMIC_TRADEABLE_ONLY=0`. The
catalog writer only supports the adapter's hard-coded Rithmic futures list.

"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

from nautilus_trader.adapters.rithmic import build_external_bar_type
from nautilus_trader.adapters.rithmic import normalize_rithmic_bar_spec
from nautilus_trader.adapters.rithmic import resolve_catalog_backtest_window
from nautilus_trader.adapters.rithmic import resolve_catalog_instrument_id
from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model import Bar
from nautilus_trader.persistence import ParquetDataCatalog


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")

    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the parent directory for the Rithmic example catalog, "
            "for example /tmp/nautilus-data/examples/rithmic. "
            "This script reads bars from <NAUTILUS_PATH>/catalog.",
        )
    return Path(nautilus_path).expanduser().resolve()


CATALOG_PATH = _require_example_root() / "catalog"
INSTRUMENT_ID = "MNQM6.RITHMIC"
BAR_SPEC = normalize_rithmic_bar_spec("1-MINUTE-LAST")
TRADE_SIZE = Decimal(1)
FAST_EMA = 10
SLOW_EMA = 20
STARTING_BALANCE = "100000 USD"
BACKTEST_START = None
BACKTEST_END = None


def main() -> None:
    catalog = ParquetDataCatalog(str(CATALOG_PATH))
    resolved_instrument_id = resolve_catalog_instrument_id(
        catalog,
        instrument_id=INSTRUMENT_ID,
        product_code=None,
        exchange=None,
    )
    instrument = catalog.instruments(instrument_ids=[resolved_instrument_id.value])

    if not instrument:
        raise RuntimeError(
            f"Instrument {resolved_instrument_id} not found in catalog {CATALOG_PATH}. "
            "Run the instrument catalog writer first if needed, then load bars with "
            "examples/backtest/rithmic/rithmic_download_bars.py.",
        )
    instrument = instrument[0]

    bar_type = build_external_bar_type(resolved_instrument_id, BAR_SPEC)
    start_time, end_time = resolve_catalog_backtest_window(
        catalog,
        bar_type=bar_type,
        start_time=BACKTEST_START,
        end_time=BACKTEST_END,
    )

    base_currency = getattr(
        getattr(instrument, "quote_currency", None) or getattr(instrument, "currency", None),
        "code",
        None,
    )

    if base_currency is None:
        raise RuntimeError(f"Could not determine a base currency for {resolved_instrument_id}")

    strategies = [
        ImportableStrategyConfig(
            strategy_path="nautilus_trader.examples.strategies.ema_cross:EMACross",
            config_path="nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
            config={
                "instrument_id": resolved_instrument_id,
                "bar_type": str(bar_type),
                "fast_ema_period": FAST_EMA,
                "slow_ema_period": SLOW_EMA,
                "trade_size": TRADE_SIZE,
                "request_bars": False,
                "subscribe_trade_ticks": False,
                "subscribe_quote_ticks": False,
            },
        ),
    ]

    config = BacktestRunConfig(
        engine=BacktestEngineConfig(strategies=strategies),
        data=[
            BacktestDataConfig(
                catalog_path=str(CATALOG_PATH),
                data_cls=Bar,
                instrument_id=resolved_instrument_id,
                bar_spec=BAR_SPEC,
                start_time=start_time,
                end_time=end_time,
            ),
        ],
        venues=[
            BacktestVenueConfig(
                name="RITHMIC",
                oms_type="NETTING",
                account_type="MARGIN",
                base_currency=base_currency,
                starting_balances=[STARTING_BALANCE],
            ),
        ],
    )

    node = BacktestNode(configs=[config])
    results = node.run()

    print(f"Catalog path: {CATALOG_PATH}")
    print(f"Instrument ID: {resolved_instrument_id}")
    print(f"Bar type: {bar_type}")
    print(f"Backtest range: {start_time} -> {end_time}")
    print(results)


if __name__ == "__main__":
    main()
