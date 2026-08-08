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

from nautilus_trader._libnautilus.model import Bar
from nautilus_trader._libnautilus.model import BarType
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
    bar_count: int = 0
    historical_bar_count: int = 0
    lifecycle: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


_STATE_LOCK = threading.Lock()
_PROBE_STATES: dict[str, _ProbeState] = {}


def register_probe_state(*, key: str, instrument_id: str) -> None:
    with _STATE_LOCK:
        _PROBE_STATES[key] = _ProbeState(instrument_id=instrument_id)


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
            "bar_count": state.bar_count,
            "historical_bar_count": state.historical_bar_count,
            "lifecycle": list(state.lifecycle),
            "errors": list(state.errors),
        }


def _require_probe_state(key: str) -> _ProbeState:
    with _STATE_LOCK:
        state = _PROBE_STATES.get(key)

    if state is None:
        raise RuntimeError(f"Unknown Rithmic probe state key: {key}")
    return state


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


def _mark_bar(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.bar_count += 1
        state.last_event_at_unix = now
        state.data_seen.set()


def _mark_historical_bar(key: str) -> None:
    state = _require_probe_state(key)
    now = time.time()
    with state.lock:
        state.historical_bar_count += 1
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


def _coerce_bar_type(value: str | BarType | None) -> BarType | None:
    if value is None:
        return None

    if isinstance(value, BarType):
        return value
    return BarType.from_str(value)


class RithmicDataProbeStrategyConfig(StrategyConfig):
    def __new__(
        cls,
        instrument_id: str | InstrumentId = "MNQM6.RITHMIC",
        data_client_id: str | ClientId = "RITHMIC",
        strategy_id: str | StrategyId = "RITHMIC-DATA-PROBE-001",
        state_key: str = "rithmic-data-probe",
        bar_type: str | BarType | None = None,
        subscribe_quotes: bool = True,
        subscribe_trades: bool = True,
        subscribe_depth: bool = True,
        subscribe_bars: bool = False,
        request_bars: bool = False,
        historical_lookback_minutes: int = 30,
        depth_levels: int = 10,
        unsubscribe_on_stop: bool = True,
        log_data: bool = False,
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
            market_exit_time_in_force=TimeInForce.IOC,
            market_exit_reduce_only=True,
            log_events=log_events,
            log_commands=log_commands,
        )
        config.instrument_id = parsed_instrument_id
        config.data_client_id = _coerce_client_id(data_client_id)
        config.state_key = state_key
        config.bar_type = _coerce_bar_type(bar_type)
        config.subscribe_quotes = bool(subscribe_quotes)
        config.subscribe_trades = bool(subscribe_trades)
        config.subscribe_depth = bool(subscribe_depth)
        config.subscribe_bars = bool(subscribe_bars)
        config.request_bars = bool(request_bars)
        config.historical_lookback_minutes = max(int(historical_lookback_minutes), 0)
        config.depth_levels = int(depth_levels)
        config.unsubscribe_on_stop = bool(unsubscribe_on_stop)
        config.log_data = bool(log_data)
        return config


class RithmicDataProbeStrategy(Strategy):
    def __init__(self, config: RithmicDataProbeStrategyConfig):
        super().__init__(config)
        self.config = config

    def on_start(self):
        _mark_started(self.config.state_key)
        instrument = self.cache.instrument(self.config.instrument_id)

        if instrument is not None:
            self.on_instrument(instrument)

        self.request_instrument(
            instrument_id=self.config.instrument_id,
            client_id=self.config.data_client_id,
        )

        if self.config.subscribe_quotes:
            self.subscribe_quotes(
                instrument_id=self.config.instrument_id,
                client_id=self.config.data_client_id,
            )

        if self.config.subscribe_trades:
            self.subscribe_trades(
                instrument_id=self.config.instrument_id,
                client_id=self.config.data_client_id,
            )

        if self.config.subscribe_depth:
            self.subscribe_book_deltas(
                instrument_id=self.config.instrument_id,
                book_type=BookType.L2_MBP,
                depth=self.config.depth_levels,
                client_id=self.config.data_client_id,
            )

        if self.config.subscribe_bars and self.config.bar_type is not None:
            if self.config.request_bars:
                end_ns = self.clock.timestamp_ns()
                lookback_ns = int(self.config.historical_lookback_minutes) * 60 * 1_000_000_000
                self.request_bars(
                    bar_type=self.config.bar_type,
                    start=end_ns - lookback_ns if lookback_ns > 0 else None,
                    end=end_ns if lookback_ns > 0 else None,
                    client_id=self.config.data_client_id,
                )
            self.subscribe_bars(
                bar_type=self.config.bar_type,
                client_id=self.config.data_client_id,
            )

    def on_stop(self):
        if self.config.unsubscribe_on_stop:
            if self.config.subscribe_quotes:
                self.unsubscribe_quotes(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.data_client_id,
                )

            if self.config.subscribe_trades:
                self.unsubscribe_trades(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.data_client_id,
                )

            if self.config.subscribe_depth:
                self.unsubscribe_book_deltas(
                    instrument_id=self.config.instrument_id,
                    client_id=self.config.data_client_id,
                )

            if self.config.subscribe_bars and self.config.bar_type is not None:
                self.unsubscribe_bars(
                    bar_type=self.config.bar_type,
                    client_id=self.config.data_client_id,
                )

        _mark_stopped(self.config.state_key)

    def on_instrument(self, instrument):
        _mark_instrument(self.config.state_key)

        if self.config.log_data:
            self.log.info(f"Instrument: {instrument}")

    def on_quote(self, tick):
        _mark_quote(self.config.state_key)

        if self.config.log_data:
            self.log.info(f"Quote: {tick}")

    def on_trade(self, tick):
        _mark_trade(self.config.state_key)

        if self.config.log_data:
            self.log.info(f"Trade: {tick}")

    def on_book_deltas(self, deltas):
        _mark_book_deltas(self.config.state_key)

        if self.config.log_data:
            self.log.info(f"Book deltas: {deltas}")

    def on_bar(self, bar: Bar):
        if self.config.bar_type is not None and bar.bar_type != self.config.bar_type:
            return
        _mark_bar(self.config.state_key)

        if self.config.log_data:
            self.log.info(f"Bar: {bar}")

    def on_historical_data(self, data):
        if (
            isinstance(data, Bar)
            and self.config.bar_type is not None
            and data.bar_type == self.config.bar_type
        ):
            _mark_historical_bar(self.config.state_key)

            if self.config.log_data:
                self.log.info(f"Historical bar: {data}")

    def on_historical_bars(self, bars):
        if self.config.bar_type is None:
            return

        for bar in bars:
            if bar.bar_type != self.config.bar_type:
                continue
            _mark_historical_bar(self.config.state_key)

            if self.config.log_data:
                self.log.info(f"Historical bar: {bar}")

    def on_reset(self):
        state = _require_probe_state(self.config.state_key)
        with state.lock:
            state.lifecycle.append("reset")

    def on_resume(self):
        state = _require_probe_state(self.config.state_key)
        with state.lock:
            state.lifecycle.append("resume")

    def on_fault(self):
        _append_error(self.config.state_key, "fault")
