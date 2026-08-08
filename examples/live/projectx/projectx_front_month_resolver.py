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

import asyncio

from nautilus_trader.adapters.projectx import (
    ProjectXConfig,
    ProjectXHttpClient,
    load_projectx_env,
)

load_projectx_env()

PRODUCT_ROOT = "MNQ"
MARKET_DATA_LIVE = False


def _instrument_snapshot(instrument: object) -> dict[str, object]:
    info = getattr(instrument, "info", None) or {}
    instrument_id = str(getattr(instrument, "id", "") or "")
    return {
        "instrument_id": instrument_id,
        "contract_id": info.get("projectx_contract_id"),
        "symbol_id": info.get("projectx_symbol_id"),
        "name": info.get("projectx_name"),
        "active_contract": info.get("active_contract"),
    }


async def resolve_front_month_contract(
    *,
    product_root: str,
    market_data_live: bool,
) -> dict[str, object]:
    client = ProjectXHttpClient(ProjectXConfig())
    await client.start()
    try:
        instrument = await client.resolve_front_month_instrument(
            product_root=product_root,
            live=market_data_live,
        )
        return {
            "environment": "topstep",
            "market_data_live": market_data_live,
            "product_root": product_root.strip().upper(),
            "contract": _instrument_snapshot(instrument),
        }
    finally:
        client.stop()


async def main() -> None:
    result = await resolve_front_month_contract(
        product_root=PRODUCT_ROOT,
        market_data_live=MARKET_DATA_LIVE,
    )
    contract = result["contract"]
    print("ProjectX environment: topstep (pinned)")
    print(f"ProjectX market_data_live: {MARKET_DATA_LIVE}")
    print(f"ProjectX product_root: {PRODUCT_ROOT}")
    print(f"Resolved contract_id: {contract['contract_id']}")
    print(f"Resolved symbol_id: {contract['symbol_id']}")
    print(f"Resolved name: {contract['name']}")
    print(f"Resolved active_contract: {contract['active_contract']}")


if __name__ == "__main__":
    asyncio.run(main())
