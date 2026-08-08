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
from collections.abc import Callable
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from nautilus_trader._libnautilus.projectx import ProjectXConfig
from nautilus_trader._libnautilus.projectx import ProjectXHttpClient
from nautilus_trader._libnautilus.projectx import load_projectx_env
from nautilus_trader.model import BarType
from nautilus_trader.model import InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog


@dataclass(frozen=True)
class ProjectXCatalogDownloadResult:
    catalog_path: Path
    instrument_id: InstrumentId
    bar_type: BarType
    start_time: str
    end_time: str
    instrument_count: int
    bar_count: int


def build_external_bar_type(instrument_id: InstrumentId, bar_spec: str) -> BarType:
    normalized_spec = bar_spec.strip()

    if not normalized_spec:
        raise ValueError("ProjectX bar spec cannot be empty")

    if normalized_spec.endswith("-INTERNAL"):
        raise ValueError("ProjectX historical downloads require an external bar specification")
    normalized_spec = normalized_spec.removesuffix("-EXTERNAL")
    return BarType.from_str(f"{instrument_id}-{normalized_spec}-EXTERNAL")


def _time_object_to_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    normalized = value.strip()

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    return datetime.fromisoformat(normalized)


def _time_object_to_unix_nanos(value: str | datetime) -> int:
    timestamp = _time_object_to_dt(value).astimezone(UTC)
    delta = timestamp - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _run_sync[T](
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    helper_name: str,
) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro_factory())

    raise RuntimeError(
        f"{helper_name}() cannot be used inside a running event loop; "
        "use download_bars_to_catalog_async() instead",
    )


def _prepare_catalog_path(catalog_path: str | Path) -> Path:
    target_catalog_path = Path(catalog_path).expanduser()
    target_catalog_path.mkdir(parents=True, exist_ok=True)
    return target_catalog_path


def _persist_catalog_download(
    catalog_path: Path,
    instrument: Any,
    bars: list[Any],
    bar_type: BarType,
    start_time: str,
    end_time: str,
) -> int:
    catalog = ParquetDataCatalog(str(catalog_path))
    catalog.write_instruments([instrument])
    if bars:
        catalog.write_bars(bars)
    return len(
        catalog.query_bars(
            identifiers=[str(bar_type)],
            start=_time_object_to_unix_nanos(start_time),
            end=_time_object_to_unix_nanos(end_time),
        ),
    )


async def _download_bars_to_catalog_async(
    *,
    catalog_path: Path,
    instrument_id: InstrumentId,
    bar_spec: str,
    start_time: str,
    end_time: str,
    limit: int,
    market_data_live: bool,
    allow_live_history_fallback: bool,
) -> ProjectXCatalogDownloadResult:
    config = ProjectXConfig(environment="TopstepX")
    client = ProjectXHttpClient.from_config(config)

    try:
        await client.start()

        contracts = await client.available_instruments(
            live=market_data_live,
            # Historical backtests must be able to resolve inactive/expired contracts. The exact
            # InstrumentId match below provides the required disambiguation without a root filter.
            active_only=False,
            product_root=None,
        )
        target_instrument = next(
            (contract for contract in contracts if contract.id == instrument_id),
            None,
        )

        if target_instrument is None:
            raise RuntimeError(f"Unable to resolve ProjectX instrument {instrument_id}")

        start = _time_object_to_dt(start_time).isoformat()
        end = _time_object_to_dt(end_time).isoformat()
        target_bar_type = build_external_bar_type(instrument_id, bar_spec)
        bars = await client.retrieve_bars(
            contract_id=target_instrument.info["projectx_contract_id"],
            bar_type=target_bar_type,
            start_time=start,
            end_time=end,
            limit=limit or 20_000,
            live=market_data_live,
            allow_live_history_fallback=allow_live_history_fallback,
        )
    finally:
        client.stop()

    bar_count = await asyncio.to_thread(
        _persist_catalog_download,
        catalog_path,
        target_instrument,
        bars,
        target_bar_type,
        start_time,
        end_time,
    )

    return ProjectXCatalogDownloadResult(
        catalog_path=catalog_path,
        instrument_id=instrument_id,
        bar_type=target_bar_type,
        start_time=start_time,
        end_time=end_time,
        instrument_count=1,
        bar_count=bar_count,
    )


async def download_bars_to_catalog_async(
    *,
    catalog_path: str | Path,
    instrument_id: str | InstrumentId,
    bar_spec: str,
    start_time: str,
    end_time: str,
    limit: int = 0,
    market_data_live: bool = False,
    allow_live_history_fallback: bool = False,
) -> ProjectXCatalogDownloadResult:
    """Download ProjectX bars into a local catalog from an async application."""
    await asyncio.to_thread(load_projectx_env)

    target_catalog_path = await asyncio.to_thread(_prepare_catalog_path, catalog_path)
    target_instrument_id = (
        instrument_id
        if isinstance(instrument_id, InstrumentId)
        else InstrumentId.from_str(str(instrument_id))
    )

    return await _download_bars_to_catalog_async(
        catalog_path=target_catalog_path,
        instrument_id=target_instrument_id,
        bar_spec=bar_spec,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        market_data_live=market_data_live,
        allow_live_history_fallback=allow_live_history_fallback,
    )


def download_bars_to_catalog(
    *,
    catalog_path: str | Path,
    instrument_id: str | InstrumentId,
    bar_spec: str,
    start_time: str,
    end_time: str,
    limit: int = 0,
    market_data_live: bool = False,
    allow_live_history_fallback: bool = False,
) -> ProjectXCatalogDownloadResult:
    """
    Download ProjectX bars into a local catalog.

    Raw ProjectX historical bar requests can include the current open/in-progress final bar when
    `end_time` reaches the active interval. For immutable closed-only catalogs, prefer an
    `end_time` on a completed bar boundary or post-filter the trailing bar before reuse.

    Live-history fallback to `live=false` is disabled by default. Enable
    `allow_live_history_fallback=True` only for helper workflows where a
    live-request rejection should explicitly retry against the sim history
    source.
    """
    return _run_sync(
        lambda: download_bars_to_catalog_async(
            catalog_path=catalog_path,
            instrument_id=instrument_id,
            bar_spec=bar_spec,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            market_data_live=market_data_live,
            allow_live_history_fallback=allow_live_history_fallback,
        ),
        "download_bars_to_catalog",
    )


__all__ = [
    "ProjectXCatalogDownloadResult",
    "build_external_bar_type",
    "download_bars_to_catalog",
    "download_bars_to_catalog_async",
]
