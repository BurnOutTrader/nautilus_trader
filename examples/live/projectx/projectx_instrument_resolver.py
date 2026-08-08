#!/usr/bin/env python3
# mypy: disable-error-code="index"
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

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[3])
sys.path[:] = [_REPO_ROOT, *(path for path in sys.path if path != _REPO_ROOT)]

from nautilus_trader.adapters.projectx import ProjectXConfig, ProjectXHttpClient
from nautilus_trader.model import InstrumentId

from examples.live.projectx.projectx_instrument_provider import load_provider_snapshot


def _instrument_id_from_front_month(instrument: object) -> InstrumentId | None:
    instrument_id = str(getattr(instrument, "id", "") or "").strip().upper()

    if instrument_id:
        return InstrumentId.from_str(instrument_id)

    return None


def _instrument_id_from_contract(contract: dict[str, object]) -> InstrumentId | None:
    instrument_id = (
        str(contract.get("instrument_id") or contract.get("id") or "").strip().upper()
    )

    if instrument_id:
        return InstrumentId.from_str(instrument_id)

    return None


async def resolve_projectx_instrument_id(
    *,
    market_data_live: bool,
    product_root: str,
    configured_instrument_id: str | None = None,
) -> InstrumentId:
    explicit = (configured_instrument_id or "").strip()

    if explicit:
        return InstrumentId.from_str(explicit)

    provider = await load_provider_snapshot(
        live=market_data_live,
        product_root=product_root,
        active_only=True,
    )
    instrument_ids = provider.get("instrument_ids", [])

    if instrument_ids:
        return InstrumentId.from_str(instrument_ids[0])

    client = ProjectXHttpClient(ProjectXConfig())
    await client.start()
    try:
        instrument = await client.resolve_front_month_instrument(
            product_root=product_root,
            live=market_data_live,
        )
        instrument_id = _instrument_id_from_front_month(instrument)

        if instrument_id is not None:
            return instrument_id
    finally:
        client.stop()

    raise RuntimeError(
        "Unable to resolve a ProjectX instrument ID from the current contract set. "
        "Set a direct configured instrument ID in the calling example or "
        f"check market-data entitlements for root={product_root}.",
    )
