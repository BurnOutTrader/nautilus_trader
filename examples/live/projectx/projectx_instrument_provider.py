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

from __future__ import annotations

import asyncio

from nautilus_trader.adapters.projectx import (
    ProjectXConfig,
    ProjectXHttpClient,
    ProjectXInstrumentProvider,
    load_projectx_env,
)

load_projectx_env()

LIVE = False
PRODUCT_ROOT = "MNQ"
ACTIVE_ONLY = True


async def main() -> None:
    result = await load_provider_snapshot(
        live=LIVE,
        product_root=PRODUCT_ROOT,
        active_only=ACTIVE_ONLY,
    )
    print(
        f"Loaded {result['count']} ProjectX instruments for root={PRODUCT_ROOT} live={LIVE}"
    )

    for instrument_id in result["instrument_ids"]:
        print(instrument_id)


async def load_provider_snapshot(
    *,
    live: bool,
    product_root: str | None = None,
    active_only: bool = True,
) -> dict[str, object]:
    client = ProjectXHttpClient(
        ProjectXConfig(
            user_name=None,
            api_key=None,
            http_timeout_secs=30,
        ),
    )
    await client.start()
    try:
        provider = ProjectXInstrumentProvider(client=client, live=live)
        await provider.load_all_async(
            filters={
                "product_root": product_root,
                "active_only": active_only,
            },
        )

        loaded = provider.get_all()
        instrument_ids = [
            instrument.id.value
            for instrument in sorted(loaded, key=lambda item: item.id.value)
        ]
        return {
            "live": live,
            "product_root": (product_root or "").strip().upper() or None,
            "active_only": active_only,
            "count": len(loaded),
            "instrument_ids": instrument_ids,
        }
    finally:
        client.stop()


if __name__ == "__main__":
    asyncio.run(main())
