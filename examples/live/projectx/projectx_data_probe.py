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

import threading
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from nautilus_trader._libnautilus.model import BookType
from nautilus_trader._libnautilus.model import ClientId
from nautilus_trader._libnautilus.model import InstrumentId
from nautilus_trader._libnautilus.model import StrategyId
from nautilus_trader._libnautilus.model import TimeInForce
from nautilus_trader._libnautilus.trading import Strategy
from nautilus_trader._libnautilus.trading import StrategyConfig


@dataclass
class _ProbeState:
    instrument_id: str
    market_data_live: bool
    instrument_ready: threading.Event = field(default_factory=threading.Event)
    data_seen: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    started_at_unix: float | None = None
    stopped_at_unix: float | None = None
    last_event_at_unix: float | None = None
    instrument_count: int = 0
    quote_count: int = 0
    trade_count: int = 0
    book_delta_count: int = 0
    lifecycle: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


_STATE_LOCK = threading.Lock()
_PROBE_STATES: dict[str, _ProbeState] = {}


def register_probe_state(
    *,
    key: str,
    instrument_id: str,
    market_data_live: bool,
) -> None:
    with _STATE_LOCK:
        _PROBE_STATES[key] = _ProbeState(
            instrument_id=instrument_id,
            market_data_live=market_data_live,
        )


def clear_probe_state(key: str) -> None:
    with _STATE_LOCK:
        _PROBE_STATES.pop(key, None)


def wait_for_probe_instrument(key: str, timeout: float) -> bool:
    return _require_probe_state(key).instrument_ready.wait(timeout=timeout)


def wait_for_probe_data(key: str, timeout: float) -> bool:
    return _require_probe_state(key).data_seen.wait(timeout=timeout)


def wait_for_probe_stop(key: str, timeout: float) -> bool:
    return _require_probe_state(key).stopped.wait(timeout=timeout)


def snapshot_probe_state(key: str) -> dict[str, Any]:
    state = _require_probe_state(key)
    with state.lock:
        return {
            "instrument_id": state.instrument_id,
            "market_data_live": state.market_data_live,
            "instrument_ready": state.instrument_ready.is_set(),
            "data_seen": state.data_seen.is_set(),
            "stopped": state.stopped.is_set(),
            "started_at_unix": state.started_at_unix,
            "stopped_at_unix": state.stopped_at_unix,
            "last_event_at_unix": state.last_event_at_unix,
            "instrument_count": state.instrument_count,
            "quote_count": state.quote_count,
            "trade_count": state.trade_count,
            "book_delta_count": state.book_delta_count,
            "lifecycle": list(state.lifecycle),
            "errors": list(state.errors),
        }


def _require_probe_state(key: str) -> _ProbeState:
    with _STATE_LOCK:
        state = _PROBE_STATES.get(key)

    if state is None:
        raise RuntimeError(f"Unknown ProjectX probe state key: {key}")
    return state


def _append_lifecycle(key: str, entry: str) -> None:
    state = _require_probe_state(key)
    with state.lock:
        state.lifecycle.append(entry)
        state.last_event_at_unix = time.time()


def _append_error(key: str, error: str) -> None:
    state = _require_probe_state(key)
    with state.lock:
        state.errors.append(error)
        state.last_event_at_unix = time.time()


def _mark_started(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.started_at_unix = now
        state.lifecycle.append("start")
        state.last_event_at_unix = now


def _mark_stopped(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.stopped_at_unix = now
        state.lifecycle.append("stop")
        state.last_event_at_unix = now
        state.stopped.set()


def _mark_instrument(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.instrument_count += 1
        state.last_event_at_unix = now
        state.instrument_ready.set()


def _mark_quote(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.quote_count += 1
        state.last_event_at_unix = now
        state.data_seen.set()


def _mark_trade(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.trade_count += 1
        state.last_event_at_unix = now
        state.data_seen.set()


def _mark_book_deltas(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.book_delta_count += 1
        state.last_event_at_unix = now
        state.data_seen.set()


def _coerce_instrument_id(value: str | InstrumentId) -> InstrumentId:
    if isinstance(value, InstrumentId):
        return value
    return InstrumentId.from_str(value)


def _coerce_client_id(value: str | ClientId) -> ClientId:
    if isinstance(value, ClientId):
        return value
    return ClientId(value)


def _coerce_strategy_id(value: str | StrategyId) -> StrategyId:
    if isinstance(value, StrategyId):
        return value
    return StrategyId(value)


class ProjectXDataProbeStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | InstrumentId = "MESM26.PROJECTX",
        client_id: str | ClientId = "PROJECTX",
        strategy_id: str | StrategyId = "PROJECTX-DATA-PROBE-001",
        state_key: str = "projectx-data-probe",
        subscribe_quotes: bool = True,
        subscribe_trades: bool = True,
        subscribe_depth: bool = True,
        depth_levels: int = 10,
        unsubscribe_on_stop: bool = True,
        log_data: bool = False,
        log_events: bool = True,
        log_commands: bool = False,
    ):
        parsed_instrument_id = _coerce_instrument_id(instrument_id)
        parsed_client_id = _coerce_client_id(client_id)
        parsed_strategy_id = _coerce_strategy_id(strategy_id)

        config = super().__new__(
            cls,
            strategy_id=parsed_strategy_id,
            external_order_claims=[parsed_instrument_id],
            manage_stop=False,
            market_exit_time_in_force=TimeInForce.IOC,
            market_exit_reduce_only=True,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.client_id = parsed_client_id
        config.state_key = state_key
        config.subscribe_quotes = subscribe_quotes
        config.subscribe_trades = subscribe_trades
        config.subscribe_depth = subscribe_depth
        config.depth_levels = depth_levels
        config.unsubscribe_on_stop = unsubscribe_on_stop
        config.log_data = log_data
        return config


class ProjectXDataProbeStrategy(Strategy):
    def __init__(self, config: ProjectXDataProbeStrategyConfig):
        super().__init__(config)
        self.config = config

    def on_start(self):
        _mark_started(self.config.state_key)
        instrument = self.cache.instrument(self.config.instrument_id)

        if instrument is not None:
            self.on_instrument(instrument)

        self.request_instrument(
            instrument_id=self.config.instrument_id,
            client_id=self.config.client_id,
        )

        if self.config.subscribe_quotes:
            self.subscribe_quotes(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        if self.config.subscribe_trades:
            self.subscribe_trades(
                instrument_id=self.config.instrument_id,
                client_id=self.config.client_id,
            )

        if self.config.subscribe_depth:
            self.subscribe_book_deltas(
                instrument_id=self.config.instrument_id,
                book_type=BookType.L2_MBP,
                depth=self.config.depth_levels,
                client_id=self.config.client_id,
                managed=True,
            )

    def on_stop(self):
        if self.config.unsubscribe_on_stop:
            if self.config.subscribe_quotes:
                self.unsubscribe_quotes(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.client_id,
                )

            if self.config.subscribe_trades:
                self.unsubscribe_trades(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.client_id,
                )

            if self.config.subscribe_depth:
                self.unsubscribe_book_deltas(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.client_id,
                )

        _mark_stopped(self.config.state_key)

    def on_instrument(self, instrument):
        _mark_instrument(self.config.state_key)

        if self.config.log_data:
            self._info(f"Instrument: {instrument}")

    def on_quote(self, tick):
        _mark_quote(self.config.state_key)

        if self.config.log_data:
            self._info(f"Quote: {tick}")

    def on_trade(self, tick):
        _mark_trade(self.config.state_key)

        if self.config.log_data:
            self._info(f"Trade: {tick}")

    def on_book_deltas(self, deltas):
        _mark_book_deltas(self.config.state_key)

        if self.config.log_data:
            self._info(f"Book deltas: {deltas}")

    def on_reset(self):
        _append_lifecycle(self.config.state_key, "reset")

    def on_resume(self):
        _append_lifecycle(self.config.state_key, "resume")

    def on_degrade(self):
        _append_lifecycle(self.config.state_key, "degrade")

    def on_fault(self):
        _append_error(self.config.state_key, "fault")
