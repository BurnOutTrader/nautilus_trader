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

from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXHttpClient
from nautilus_trader.adapters.projectx import load_projectx_env


load_projectx_env()

PRODUCT_ROOT = "MNQ"
MARKET_DATA_LIVE = False


async def main() -> None:
    client = ProjectXHttpClient(ProjectXConfig())
    await client.start()
    try:
        instrument = await client.resolve_front_month_instrument(
            product_root=PRODUCT_ROOT,
            live=MARKET_DATA_LIVE,
        )
        print("ProjectX environment: topstep (pinned)")
        print(f"ProjectX market_data_live: {MARKET_DATA_LIVE}")
        print(f"ProjectX product_root: {PRODUCT_ROOT}")
        info = instrument.info
        print(f"Resolved contract_id: {info['projectx_contract_id']}")
        print(f"Resolved symbol_id: {info['projectx_symbol_id']}")
        print(f"Resolved name: {info['projectx_name']}")
        print(f"Resolved active_contract: {info['active_contract']}")
    finally:
        client.stop()


if __name__ == "__main__":
    asyncio.run(main())
