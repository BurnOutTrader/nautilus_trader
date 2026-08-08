#!/usr/bin/env python3
# mypy: disable-error-code="attr-defined"
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
Run this workflow to:
1. Resolves the current MNQ front month on CME by default
2. Starts a data-only Rithmic LiveNode
3. Subscribes to book deltas over the PyO3 strategy API
4. Prints either raw deltas, a readable managed book state, or both

Note:
    The current PyO3 strategy surface does not expose the legacy raw
    ``OrderBookDepth10`` callback. ``STREAM_MODE="depth"`` is therefore treated
    as the readable managed-book mode built from incoming deltas.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

_REPO_ROOT = Path(__file__).resolve().parents[3]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nautilus_trader._libnautilus.common import Environment, LogColor
from nautilus_trader._libnautilus.model import BookType, ClientId, OrderBook, StrategyId
from nautilus_trader._libnautilus.model import InstrumentId as PyInstrumentId
from nautilus_trader._libnautilus.trading import Strategy, StrategyConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId

from examples.live.rithmic.rithmic_live_node_helpers import (
    TRADER_ID,
    RithmicDataClientFactory,
    build_data_client_config,
    build_data_client_id,
    load_rithmic_env_file,
    resolve_instrument_id,
    schedule_stop,
)

if TYPE_CHECKING:
    from nautilus_trader.config import ImportableStrategyConfig
else:
    from nautilus_trader._libnautilus.trading import ImportableStrategyConfig


load_rithmic_env_file()

PROFILE = None
PRODUCT_CODE = "MNQ"
EXCHANGE = "CME"
INSTRUMENT_ID: InstrumentId | None = None
STREAM_MODE = "book"  # "book", "depth", "deltas", or "both"
BOOK_TYPE = BookType.L2_MBP
LEVELS_TO_PRINT = 10
BOOK_PRINT_EVERY_N_UPDATES = 1
RUN_SECONDS = 0

_MODULE_PATH = "examples.live.rithmic.rithmic_orderbook_probe"
_STRATEGY_PATH = f"{_MODULE_PATH}:RithmicOrderBookProbeStrategy"
_CONFIG_PATH = f"{_MODULE_PATH}:RithmicOrderBookProbeStrategyConfig"


def normalize_stream_mode(value: str) -> str:
    mode = value.strip().lower()

    if mode == "depth":
        return "book"

    if mode not in {"book", "deltas", "both"}:
        raise ValueError("STREAM_MODE must be one of: book, depth, deltas, both")
    return mode


def _coerce_instrument_id(value: str | PyInstrumentId) -> PyInstrumentId:
    if isinstance(value, PyInstrumentId):
        return value
    return PyInstrumentId.from_str(value)


def _coerce_client_id(value: str | ClientId) -> ClientId:
    if isinstance(value, ClientId):
        return value
    return ClientId(value)


def _coerce_strategy_id(value: str | StrategyId) -> StrategyId:
    if isinstance(value, StrategyId):
        return value
    return StrategyId(value)


def _coerce_book_type(value: str | BookType) -> BookType:
    if isinstance(value, BookType):
        return value
    return BookType(value)


class RithmicOrderBookProbeStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | PyInstrumentId = "MNQM6.CME.RITHMIC",
        data_client_id: str | ClientId = "RITHMIC",
        strategy_id: str | StrategyId = "RITHMIC-ORDERBOOK-001",
        stream_mode: str = "book",
        book_type: str | BookType = BookType.L2_MBP,
        levels_to_print: int = 10,
        book_print_every_n_updates: int = 1,
        log_events: bool = True,
        log_commands: bool = False,
    ):
        parsed_instrument_id = _coerce_instrument_id(instrument_id)
        parsed_strategy_id = _coerce_strategy_id(strategy_id)

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=False,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.data_client_id = _coerce_client_id(data_client_id)
        config.stream_mode = normalize_stream_mode(stream_mode)
        config.book_type = _coerce_book_type(book_type)
        config.levels_to_print = int(levels_to_print)
        config.book_print_every_n_updates = int(book_print_every_n_updates)
        return config


class RithmicOrderBookProbeStrategy(Strategy):
    def __init__(self, config: RithmicOrderBookProbeStrategyConfig):
        super().__init__(config)
        self.config = config
        self._book = OrderBook(config.instrument_id, config.book_type)
        self._delta_batches = 0

    def on_start(self):
        self.request_instrument(
            instrument_id=self.config.instrument_id,
            client_id=self.config.data_client_id,
        )
        self.subscribe_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=self.config.book_type,
            depth=self.config.levels_to_print,
            client_id=self.config.data_client_id,
        )

    def on_stop(self):
        self.unsubscribe_book_deltas(
            instrument_id=self.config.instrument_id,
            client_id=self.config.data_client_id,
        )

    def on_instrument(self, instrument):
        if instrument.id != self.config.instrument_id:
            return
        self.log.info(f"Instrument ready: {instrument.id}", LogColor.CYAN)

    def on_book_deltas(self, deltas):
        self._delta_batches += 1

        if self.config.stream_mode in {"deltas", "both"}:
            self.log.info(
                f"OrderBookDeltas update #{self._delta_batches}: {deltas!r}",
                LogColor.CYAN,
            )

        if self.config.stream_mode in {"book", "both"}:
            self._book.apply_deltas(deltas)

            if self._delta_batches % self.config.book_print_every_n_updates == 0:
                self.log.info(
                    "Managed book update "
                    f"#{self._delta_batches} deltas={len(deltas.deltas)} ts_event={deltas.ts_event}\n"
                    f"{self._book.pprint(self.config.levels_to_print)}",
                    LogColor.CYAN,
                )


def main() -> None:
    profile = PROFILE
    normalized_stream_mode = normalize_stream_mode(STREAM_MODE)
    run_seconds = int(RUN_SECONDS)
    levels_to_print = int(LEVELS_TO_PRINT)
    book_print_every_n_updates = int(BOOK_PRINT_EVERY_N_UPDATES)

    if levels_to_print <= 0:
        raise ValueError("LEVELS_TO_PRINT must be positive")

    if book_print_every_n_updates <= 0:
        raise ValueError("BOOK_PRINT_EVERY_N_UPDATES must be positive")

    if run_seconds < 0:
        raise ValueError("RUN_SECONDS cannot be negative")

    instrument_id = resolve_instrument_id(
        profile, INSTRUMENT_ID, PRODUCT_CODE, EXCHANGE
    )
    data_client_id = build_data_client_id(profile)

    node = (
        LiveNode.builder("TESTER-001", TRADER_ID, Environment.LIVE)
        .with_reconciliation(False)
        .with_timeout_connection(20)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(5)
        .add_data_client(
            data_client_id,
            RithmicDataClientFactory(),
            build_data_client_config(profile),
        )
        .build()
    )

    node.add_strategy_from_config(
        ImportableStrategyConfig(
            strategy_path=_STRATEGY_PATH,
            config_path=_CONFIG_PATH,
            config={
                "instrument_id": instrument_id.value,
                "data_client_id": data_client_id,
                "strategy_id": "RITHMIC-ORDERBOOK-001",
                "stream_mode": STREAM_MODE,
                "book_type": BOOK_TYPE,
                "levels_to_print": levels_to_print,
                "book_print_every_n_updates": book_print_every_n_updates,
                "log_events": True,
                "log_commands": False,
            },
        ),
    )

    print(f"Resolved instrument: {instrument_id}")
    print(f"Data client ID: {data_client_id}")
    print(f"Stream mode: {normalized_stream_mode}")
    print(f"Levels to print: {levels_to_print}")

    if STREAM_MODE.strip().lower() == "depth":
        print(
            "Raw depth callbacks are not exposed on PyO3 strategies; using managed book output."
        )
    print("Press CTRL+C to stop.")
    print()

    timer = None
    try:
        timer = schedule_stop(node, run_seconds)
        node.run()
    finally:
        if timer is not None:
            timer.cancel()


if __name__ == "__main__":
    main()
