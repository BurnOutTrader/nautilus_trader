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
from pathlib import Path

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


def _unit_code_for_spec(bar_spec: str) -> int:
    aggregation = bar_spec.upper().split("-")[-2]

    # ProjectX API enum: 1=Second, 2=Minute, 3=Hour, 4=Day, 5=Week, 6=Month.
    match aggregation:
        case "SECOND":
            return 1
        case "MINUTE":
            return 2
        case "HOUR":
            return 3
        case "DAY":
            return 4
        case "WEEK":
            return 5
        case "MONTH":
            return 6

    raise ValueError(f"Unsupported ProjectX bar aggregation {aggregation!r}")


def _unit_number_for_spec(bar_spec: str) -> int:
    parts = bar_spec.upper().split("-")

    try:
        return int(parts[0])
    except ValueError:
        return 1


def _run_sync(coro: object, helper_name: str) -> object:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)  # type: ignore[arg-type]

    raise RuntimeError(
        f"{helper_name}() cannot be used inside a running event loop; "
        f"use an async adapter flow or call {helper_name}() from synchronous code",
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
    load_projectx_env()

    config = ProjectXConfig(environment="TopstepX")
    client = ProjectXHttpClient.from_config(config)

    try:
        await client.start()

        contracts = await client.available_instruments(
            live=market_data_live,
            active_only=True,
            product_root=instrument_id.symbol.value,
        )
        target_instrument = next(
            (contract for contract in contracts if contract.id == instrument_id),
            None,
        )

        if target_instrument is None:
            raise RuntimeError(f"Unable to resolve ProjectX instrument {instrument_id}")

        unit = _unit_code_for_spec(bar_spec)
        unit_number = _unit_number_for_spec(bar_spec)
        start = _time_object_to_dt(start_time).isoformat()
        end = _time_object_to_dt(end_time).isoformat()
        target_bar_type = build_external_bar_type(instrument_id, bar_spec)

        try:
            bars = await client.retrieve_bars(
                contract_id=target_instrument.info["projectx_contract_id"],
                bar_type=target_bar_type,
                start_time=start,
                end_time=end,
                unit=unit,
                unit_number=unit_number,
                limit=limit or 20_000,
                live=market_data_live,
            )
        except Exception:
            if not allow_live_history_fallback:
                raise

            bars = await client.retrieve_bars(
                contract_id=target_instrument.info["projectx_contract_id"],
                bar_type=target_bar_type,
                start_time=start,
                end_time=end,
                unit=unit,
                unit_number=unit_number,
                limit=limit or 20_000,
                live=False,
            )
    finally:
        client.stop()

    catalog = ParquetDataCatalog(str(catalog_path))
    catalog.write_data([target_instrument])

    if bars:
        catalog.write_data(bars)

    stored = catalog.bars(
        bar_types=[str(build_external_bar_type(instrument_id, bar_spec))],
        start=start_time,
        end=end_time,
    )

    return ProjectXCatalogDownloadResult(
        catalog_path=catalog_path,
        instrument_id=instrument_id,
        bar_type=build_external_bar_type(instrument_id, bar_spec),
        start_time=start_time,
        end_time=end_time,
        instrument_count=1,
        bar_count=len(stored),
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
    load_projectx_env()

    target_catalog_path = Path(catalog_path).expanduser()
    target_catalog_path.mkdir(parents=True, exist_ok=True)

    target_instrument_id = (
        instrument_id
        if isinstance(instrument_id, InstrumentId)
        else InstrumentId.from_str(str(instrument_id))
    )

    return _run_sync(
        _download_bars_to_catalog_async(
            catalog_path=target_catalog_path,
            instrument_id=target_instrument_id,
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
]
