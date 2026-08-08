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
import json
from pathlib import Path

from nautilus_trader.adapters.projectx import (
    ProjectXConfig,
    ProjectXHttpClient,
    load_projectx_env,
)

load_projectx_env()

MARKET_DATA_LIVE = False
ACTIVE_ONLY = True
PRODUCT_ROOT = "MNQ"
OUTPUT_PATH = Path("examples/live/projectx/projectx_instruments_snapshot.json")


async def main() -> None:
    client = ProjectXHttpClient(
        ProjectXConfig(
            user_name=None,  # Uses PROJECTX_USERNAME
            api_key=None,  # Uses PROJECTX_API_KEY
        ),
    )
    await client.start()
    try:
        instruments = await client.available_instruments(
            live=MARKET_DATA_LIVE,
            active_only=ACTIVE_ONLY,
            product_root=PRODUCT_ROOT,
        )
        rows = [
            {
                "id": str(instrument.id),
                "publicSymbol": instrument.id.symbol.value,
                "name": instrument.info.get("projectx_name", ""),
                "symbolId": instrument.info.get("projectx_symbol_id", ""),
                "tickSize": str(instrument.price_increment),
                "tickValue": instrument.info.get("tick_value"),
                "activeContract": instrument.info.get("active_contract", False),
            }
            for instrument in instruments
        ]

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {len(rows)} instrument rows to {OUTPUT_PATH}")
    finally:
        client.stop()


if __name__ == "__main__":
    asyncio.run(main())
