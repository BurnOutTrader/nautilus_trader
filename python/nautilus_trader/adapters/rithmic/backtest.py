# mypy: disable-error-code="dict-item"
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

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader._libnautilus.rithmic import RithmicDataClient as BindingRithmicDataClient
from nautilus_trader._libnautilus.rithmic import RithmicDataClientConfig
from nautilus_trader._libnautilus.rithmic import RithmicGateway
from nautilus_trader._libnautilus.rithmic import (
    RithmicInstrumentProvider as BindingRithmicInstrumentProvider,
)
from nautilus_trader.adapters.rithmic.config import load_rithmic_env_file
from nautilus_trader.adapters.rithmic.config import to_binding_environment
from nautilus_trader.adapters.rithmic.constants import RITHMIC
from nautilus_trader.adapters.rithmic.providers import candidate_exchanges_for_symbol
from nautilus_trader.adapters.rithmic.providers import normalize_rithmic_symbol
from nautilus_trader.adapters.rithmic.providers import supported_product_for_symbol
from nautilus_trader.model import BarAggregation
from nautilus_trader.model import BarType
from nautilus_trader.model import InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog


@dataclass(frozen=True)
class RithmicCatalogDownloadResult:
    catalog_path: Path
    instrument_id: InstrumentId
    bar_type: BarType
    start_time: str
    end_time: str
    instrument_count: int
    bar_count: int


@dataclass(frozen=True)
class RithmicTradeTickDownloadResult:
    catalog_path: Path
    instrument_id: InstrumentId
    start_time: str
    end_time: str
    instrument_count: int
    tick_count: int


@dataclass(frozen=True)
class _ResolvedDownloadContract:
    instrument: Any
    instrument_id: InstrumentId
    symbol: str
    exchange: str


@dataclass(frozen=True)
class _HistoricalSession:
    gateway: RithmicGateway
    provider: BindingRithmicInstrumentProvider
    client: BindingRithmicDataClient


def normalize_rithmic_bar_spec(bar_spec: str) -> str:
    normalized = bar_spec.strip()

    if not normalized:
        raise ValueError("Rithmic bar spec cannot be empty")

    if normalized.endswith("-INTERNAL"):
        raise ValueError("Rithmic historical downloads require an external bar specification")
    normalized = normalized.removesuffix("-EXTERNAL")
    return normalized


def build_external_bar_type(
    instrument_id: InstrumentId,
    bar_spec: str,
) -> BarType:
    normalized_spec = normalize_rithmic_bar_spec(bar_spec)
    return BarType.from_str(f"{instrument_id}-{normalized_spec}-EXTERNAL")


def _build_gateway(
    config: RithmicDataClientConfig,
    *,
    enable_history: bool,
) -> RithmicGateway:
    return RithmicGateway(
        environment=to_binding_environment(config.environment),
        username=config.username,
        password=config.password,
        system_name=config.system_name,
        app_name=config.app_name,
        app_version=config.app_version,
        fcm_id=config.fcm_id or "",
        ib_id=config.ib_id or "",
        account_id="",
        server=config.server,
        alt_server=config.alt_server,
        enable_ticker=True,
        enable_order=False,
        enable_pnl=False,
        enable_history=enable_history,
    )


async def resolve_front_month_instrument_id_async(
    profile: str | None,
    product_code: str,
    exchange: str | None = None,
) -> InstrumentId:
    load_rithmic_env_file()

    normalized_product = normalize_rithmic_symbol(product_code).upper()
    product = supported_product_for_symbol(normalized_product)

    if product is None or normalized_product != product:
        raise ValueError(f"Unsupported Rithmic product root {product_code!r}")

    exchange_candidates = candidate_exchanges_for_symbol(
        normalized_product,
        preferred_exchange=exchange,
    )

    if not exchange_candidates:
        raise ValueError(f"Unable to determine an exchange for {product_code!r}")

    if exchange is None and len(exchange_candidates) > 1:
        raise ValueError(
            f"Exchange is required for ambiguous Rithmic root {product_code!r}; "
            f"candidates={exchange_candidates}",
        )

    config = RithmicDataClientConfig.from_env(profile)
    gateway = _build_gateway(config, enable_history=False)
    provider = BindingRithmicInstrumentProvider(gateway)

    await gateway.connect()
    try:
        last_error: Exception | None = None
        contract = None

        for candidate_exchange in exchange_candidates:
            try:
                contract = await provider.load_front_month_async(product, candidate_exchange)
                break
            except Exception as exc:
                last_error = exc

        if contract is None:
            raise RuntimeError(
                f"Unable to resolve front month for {product}",
            ) from last_error
    finally:
        await gateway.disconnect()

    contract_id = getattr(contract, "id", None)

    if contract_id is not None:
        return canonical_rithmic_instrument_id(contract_id)

    symbol = getattr(contract, "symbol", None)

    if symbol is None:
        raise RuntimeError("Front-month lookup returned no symbol")

    return InstrumentId.from_str(f"{symbol}.{RITHMIC}")


def resolve_front_month_instrument_id(
    profile: str | None,
    product_code: str,
    exchange: str | None = None,
) -> InstrumentId:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            resolve_front_month_instrument_id_async(profile, product_code, exchange),
        )

    raise RuntimeError(
        "resolve_front_month_instrument_id() cannot be used inside a running event loop; "
        "use resolve_front_month_instrument_id_async() instead",
    )


def canonical_rithmic_instrument_id(instrument_id: str | InstrumentId) -> InstrumentId:
    resolved = (
        instrument_id
        if isinstance(instrument_id, InstrumentId)
        else InstrumentId.from_str(str(instrument_id))
    )
    symbol = normalize_rithmic_symbol(resolved.symbol.value)
    return InstrumentId.from_str(f"{symbol}.{RITHMIC}")


def resolve_download_instrument_id(
    *,
    profile: str | None,
    instrument_id: str | InstrumentId | None,
    product_code: str | None,
    exchange: str | None = None,
) -> InstrumentId:
    if instrument_id is not None:
        resolved = canonical_rithmic_instrument_id(instrument_id)

        if resolved.venue.value != RITHMIC:
            raise ValueError(f"Expected a Rithmic instrument ID, received {resolved}")

        symbol = normalize_rithmic_symbol(resolved.symbol.value)
        product = supported_product_for_symbol(symbol)

        if product is not None and symbol.upper() == product:
            raise ValueError(
                "Resolve the current front month first, then use the exact contract "
                "instrument ID such as `MNQM6.RITHMIC`.",
            )
        return resolved

    if not product_code:
        raise ValueError(
            "Set `instrument_id` to a direct Rithmic instrument ID such as "
            "`MNQM6.RITHMIC`, or provide `product_code` plus `exchange` "
            "to resolve the current front month.",
        )

    return resolve_front_month_instrument_id(profile, product_code, exchange)


def _run_sync(coro: Any, helper_name: str):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    raise RuntimeError(
        f"{helper_name}() cannot be used inside a running event loop; "
        f"use an async adapter flow or call {helper_name}() from synchronous code",
    )


async def _open_historical_session(profile: str | None) -> _HistoricalSession:
    config = RithmicDataClientConfig.from_env(profile)
    gateway = _build_gateway(config, enable_history=True)
    provider = BindingRithmicInstrumentProvider(gateway)
    client = BindingRithmicDataClient(gateway)

    await gateway.connect()
    await client.start_event_loop()

    return _HistoricalSession(
        gateway=gateway,
        provider=provider,
        client=client,
    )


async def _close_historical_session(session: _HistoricalSession) -> None:
    session.client.stop_event_loop()
    await session.gateway.disconnect()


def _resolved_contract_metadata(instrument: Any) -> _ResolvedDownloadContract:
    contract_id = getattr(instrument, "id", None)

    if contract_id is None:
        raise RuntimeError("Rithmic contract lookup returned no instrument ID")

    instrument_id = canonical_rithmic_instrument_id(contract_id)
    raw_symbol = getattr(getattr(instrument, "raw_symbol", None), "value", None)
    symbol = raw_symbol or instrument_id.symbol.value
    exchange = getattr(instrument, "exchange", None)

    if not isinstance(exchange, str) or not exchange:
        raise RuntimeError(f"Missing exchange on Rithmic contract {instrument_id}")

    return _ResolvedDownloadContract(
        instrument=instrument,
        instrument_id=instrument_id,
        symbol=symbol,
        exchange=exchange,
    )


async def _resolve_download_contract_async(
    *,
    session: _HistoricalSession,
    instrument_id: str | InstrumentId | None,
    product_code: str | None,
    exchange: str | None,
) -> _ResolvedDownloadContract:
    if instrument_id is not None:
        resolved_id = resolve_download_instrument_id(
            profile=None,
            instrument_id=instrument_id,
            product_code=None,
            exchange=exchange,
        )
        symbol = normalize_rithmic_symbol(resolved_id.symbol.value)
        target_exchange = exchange or ""

        instrument = await session.provider.load_instrument_async(symbol, target_exchange)

        if instrument is None:
            raise RuntimeError(f"Unable to load Rithmic instrument {resolved_id}")

        return _resolved_contract_metadata(instrument)

    if not product_code:
        raise ValueError(
            "Set `instrument_id` to a direct Rithmic instrument ID such as `MNQM6.RITHMIC`, "
            "or provide `product_code` to resolve the current front month.",
        )

    normalized_product = normalize_rithmic_symbol(product_code).upper()
    product = supported_product_for_symbol(normalized_product)

    if product is None or normalized_product != product:
        raise ValueError(f"Unsupported Rithmic product root {product_code!r}")

    exchange_candidates = candidate_exchanges_for_symbol(
        product,
        preferred_exchange=exchange,
    )

    if not exchange_candidates:
        raise ValueError(f"Unable to determine an exchange for {product!r}")

    if exchange is None and len(exchange_candidates) > 1:
        raise ValueError(
            f"Exchange is required for ambiguous Rithmic root {product!r}; "
            f"candidates={exchange_candidates}",
        )

    last_error: Exception | None = None

    for candidate_exchange in exchange_candidates:
        try:
            instrument = await session.provider.load_front_month_async(product, candidate_exchange)
            return _resolved_contract_metadata(instrument)
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Unable to resolve front month for {product}") from last_error


def _bar_request_spec(bar_type: BarType) -> tuple[str, int]:
    aggregation = bar_type.spec.aggregation
    period = int(bar_type.spec.step)

    if aggregation == BarAggregation.TICK:
        return "TickBar", period

    if aggregation == BarAggregation.SECOND:
        return "SecondBar", period

    if aggregation == BarAggregation.MINUTE:
        return "MinuteBar", period

    if aggregation == BarAggregation.DAY:
        return "DailyBar", period

    if aggregation == BarAggregation.WEEK:
        return "WeeklyBar", period

    raise NotImplementedError(
        f"Unsupported Rithmic bar aggregation for historical download: {aggregation.name}",
    )


def time_object_to_dt(value: str | datetime | int | None) -> datetime:
    """
    Normalize an ISO-8601 string, datetime, or UNIX nanoseconds value into a
    timezone-aware ``datetime``.
    """
    if value is None:
        return datetime.now(tz=UTC)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)

    if isinstance(value, str):
        normalized = value.strip()

        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"

        return datetime.fromisoformat(normalized)

    raise TypeError(f"Cannot convert time value {value!r}")


def _rfc3339_to_nanos(value: str) -> int:
    return int(time_object_to_dt(value).timestamp() * 1_000_000_000)


def _datetime_to_seconds(value: datetime | None) -> int:
    if value is None:
        return 0
    return int(value.timestamp())


async def _request_historical_bars_async(
    *,
    session: _HistoricalSession,
    resolved: _ResolvedDownloadContract,
    bar_type: BarType,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[Any]:
    bar_kind, period = _bar_request_spec(bar_type)
    price_precision = getattr(resolved.instrument, "price_precision", None)
    size_precision = getattr(resolved.instrument, "size_precision", None)
    cursor = start
    bars_by_ts: dict[int, Any] = {}

    while cursor <= end:
        responses = await session.client.request_bars(
            resolved.symbol,
            resolved.exchange,
            bar_kind,
            period,
            _datetime_to_seconds(cursor),
            _datetime_to_seconds(end),
        )

        if not responses:
            break

        page_bars = [
            raw.to_nautilus_bar(
                str(bar_type),
                price_precision=price_precision,
                size_precision=size_precision,
            )
            for raw in responses
        ]

        for bar in page_bars:
            bars_by_ts[bar.ts_event] = bar

        last_ts = max(bar.ts_event for bar in page_bars)
        last_dt = datetime.fromtimestamp(last_ts / 1_000_000_000, tz=UTC)

        if last_dt >= end:
            break

        if last_dt <= cursor:
            break

        cursor = last_dt + timedelta(seconds=1)

    bars = [bars_by_ts[key] for key in sorted(bars_by_ts)]

    if limit > 0:
        return bars[-limit:]

    return bars


async def _request_historical_trade_ticks_async(
    *,
    session: _HistoricalSession,
    resolved: _ResolvedDownloadContract,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[Any]:
    price_precision = getattr(resolved.instrument, "price_precision", None)
    size_precision = getattr(resolved.instrument, "size_precision", None)
    responses = await session.client.request_trade_ticks(
        resolved.symbol,
        resolved.exchange,
        _datetime_to_seconds(start),
        _datetime_to_seconds(end),
    )

    ticks = [
        raw.to_nautilus_trade_tick(
            resolved.instrument_id.value,
            price_precision=price_precision,
            size_precision=size_precision,
        )
        for raw in responses
    ]
    ticks.sort(key=lambda tick: (tick.ts_init, tick.ts_event))

    if limit > 0:
        return ticks[-limit:]

    return ticks


async def _download_bars_to_catalog_async(
    *,
    profile: str | None,
    catalog_path: Path,
    instrument_id: str | InstrumentId | None,
    product_code: str | None,
    exchange: str | None,
    bar_spec: str,
    start_time: str,
    end_time: str,
    limit: int,
) -> RithmicCatalogDownloadResult:
    load_rithmic_env_file()

    start = time_object_to_dt(start_time)
    end = time_object_to_dt(end_time)

    session = await _open_historical_session(profile)

    try:
        resolved = await _resolve_download_contract_async(
            session=session,
            instrument_id=instrument_id,
            product_code=product_code,
            exchange=exchange,
        )
        target_bar_type = build_external_bar_type(resolved.instrument_id, bar_spec)
        bars = await _request_historical_bars_async(
            session=session,
            resolved=resolved,
            bar_type=target_bar_type,
            start=start,
            end=end,
            limit=limit,
        )
    finally:
        await _close_historical_session(session)

    catalog = ParquetDataCatalog(str(catalog_path))
    if not catalog.instruments(instrument_ids=[resolved.instrument_id.value]):
        catalog.write_instruments([resolved.instrument])

    if bars:
        catalog.write_bars(bars)

    instruments = catalog.instruments(instrument_ids=[resolved.instrument_id.value])
    stored_bars = catalog.query_bars(
        identifiers=[str(target_bar_type)],
        start=_rfc3339_to_nanos(start_time),
        end=_rfc3339_to_nanos(end_time),
    )

    return RithmicCatalogDownloadResult(
        catalog_path=catalog_path,
        instrument_id=resolved.instrument_id,
        bar_type=target_bar_type,
        start_time=start_time,
        end_time=end_time,
        instrument_count=len(instruments),
        bar_count=len(stored_bars),
    )


async def _download_trade_ticks_to_catalog_async(
    *,
    profile: str | None,
    catalog_path: Path,
    instrument_id: str | InstrumentId | None,
    product_code: str | None,
    exchange: str | None,
    start_time: str,
    end_time: str,
    limit: int,
) -> RithmicTradeTickDownloadResult:
    load_rithmic_env_file()

    start = time_object_to_dt(start_time)
    end = time_object_to_dt(end_time)

    session = await _open_historical_session(profile)

    try:
        resolved = await _resolve_download_contract_async(
            session=session,
            instrument_id=instrument_id,
            product_code=product_code,
            exchange=exchange,
        )
        ticks = await _request_historical_trade_ticks_async(
            session=session,
            resolved=resolved,
            start=start,
            end=end,
            limit=limit,
        )
    finally:
        await _close_historical_session(session)

    catalog = ParquetDataCatalog(str(catalog_path))
    if not catalog.instruments(instrument_ids=[resolved.instrument_id.value]):
        catalog.write_instruments([resolved.instrument])

    if ticks:
        catalog.write_trade_ticks(ticks)

    instruments = catalog.instruments(instrument_ids=[resolved.instrument_id.value])
    stored_ticks = catalog.query_trade_ticks(
        identifiers=[resolved.instrument_id.value],
        start=_rfc3339_to_nanos(start_time),
        end=_rfc3339_to_nanos(end_time),
    )

    return RithmicTradeTickDownloadResult(
        catalog_path=catalog_path,
        instrument_id=resolved.instrument_id,
        start_time=start_time,
        end_time=end_time,
        instrument_count=len(instruments),
        tick_count=len(stored_ticks),
    )


def download_bars_to_catalog(
    *,
    profile: str | None,
    catalog_path: str | Path,
    instrument_id: str | InstrumentId | None = None,
    product_code: str | None = None,
    exchange: str | None = None,
    bar_spec: str,
    start_time: str,
    end_time: str,
    limit: int = 0,
) -> RithmicCatalogDownloadResult:
    """
    Download Rithmic bars into a local catalog.

    Raw Rithmic historical time-bar requests can include the current open/in-progress final bar
    when `end_time` reaches the active interval. For immutable closed-only catalogs, prefer an
    `end_time` on a completed bar boundary or post-filter the trailing bar before reuse.
    """
    target_catalog_path = Path(catalog_path).expanduser()
    target_catalog_path.mkdir(parents=True, exist_ok=True)

    return _run_sync(
        _download_bars_to_catalog_async(
            profile=profile,
            catalog_path=target_catalog_path,
            instrument_id=instrument_id,
            product_code=product_code,
            exchange=exchange,
            bar_spec=bar_spec,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        ),
        "download_bars_to_catalog",
    )


def download_trade_ticks_to_catalog(
    *,
    profile: str | None,
    catalog_path: str | Path,
    instrument_id: str | InstrumentId | None = None,
    product_code: str | None = None,
    exchange: str | None = None,
    start_time: str,
    end_time: str,
    limit: int = 0,
) -> RithmicTradeTickDownloadResult:
    target_catalog_path = Path(catalog_path).expanduser()
    target_catalog_path.mkdir(parents=True, exist_ok=True)

    return _run_sync(
        _download_trade_ticks_to_catalog_async(
            profile=profile,
            catalog_path=target_catalog_path,
            instrument_id=instrument_id,
            product_code=product_code,
            exchange=exchange,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        ),
        "download_trade_ticks_to_catalog",
    )


def resolve_catalog_instrument_id(
    catalog: ParquetDataCatalog,
    *,
    instrument_id: str | InstrumentId | None = None,
    product_code: str | None = None,
    exchange: str | None = None,
) -> InstrumentId:
    if instrument_id is not None:
        return canonical_rithmic_instrument_id(instrument_id)

    instruments = catalog.instruments()

    if not instruments:
        raise RuntimeError("The Rithmic catalog does not contain any instruments")

    if len(instruments) == 1:
        return instruments[0].id

    if product_code and exchange:
        matches = []

        for instrument in instruments:
            raw_symbol = getattr(getattr(instrument, "raw_symbol", None), "value", None)

            if raw_symbol is None:
                raw_symbol = instrument.id.symbol.value

            if (
                raw_symbol.startswith(product_code)
                and getattr(instrument, "exchange", None) == exchange
            ):
                matches.append(instrument.id)

        if len(matches) == 1:
            return matches[0]

    raise RuntimeError(
        "Could not resolve a unique Rithmic instrument from the catalog. "
        "Set `RITHMIC_INSTRUMENT_ID` explicitly to a concrete Rithmic instrument ID "
        "such as `MNQM6.RITHMIC` for the backtest run.",
    )


def resolve_catalog_backtest_window(
    catalog: ParquetDataCatalog,
    *,
    bar_type: BarType,
    start_time: str | None = None,
    end_time: str | None = None,
) -> tuple[str, str]:
    if start_time and end_time:
        return start_time, end_time

    bars = catalog.query_bars(identifiers=[str(bar_type)])

    if not bars:
        raise RuntimeError(f"No bars found in the catalog for {bar_type}")

    resolved_start = start_time or pd.Timestamp(bars[0].ts_event, unit="ns", tz="UTC").isoformat()
    resolved_end = end_time or pd.Timestamp(bars[-1].ts_event, unit="ns", tz="UTC").isoformat()
    return resolved_start, resolved_end


__all__ = [
    "RithmicCatalogDownloadResult",
    "RithmicTradeTickDownloadResult",
    "build_external_bar_type",
    "canonical_rithmic_instrument_id",
    "download_bars_to_catalog",
    "download_trade_ticks_to_catalog",
    "normalize_rithmic_bar_spec",
    "resolve_catalog_backtest_window",
    "resolve_catalog_instrument_id",
    "resolve_download_instrument_id",
    "resolve_front_month_instrument_id",
    "resolve_front_month_instrument_id_async",
]
