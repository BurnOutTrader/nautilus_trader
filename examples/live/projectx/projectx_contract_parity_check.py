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

from nautilus_trader.adapters.projectx import ProjectXConfig
from nautilus_trader.adapters.projectx import ProjectXHttpClient
from nautilus_trader.adapters.projectx import load_projectx_env


load_projectx_env()

LIVE = False
LIMIT = 0
PRODUCT_ROOT = "MNQ"
OUTPUT_PATH = ""


def _normalize_root(name: str, symbol_id: str) -> str | None:
    candidate = (name or symbol_id or "").strip().upper()

    if not candidate:
        return None

    for idx, ch in enumerate(candidate):
        if ch in "FGHJKMNQUVXZ" and idx > 0:
            return candidate[:idx]
    return None


async def main() -> None:
    result = await run_contract_parity_check(
        live=LIVE,
        product_root=PRODUCT_ROOT or None,
        limit=LIMIT,
        output_path=Path(OUTPUT_PATH).expanduser() if OUTPUT_PATH else None,  # noqa: ASYNC240
    )
    print(f"Checked {result['checked']} contracts (live={LIVE}, root={PRODUCT_ROOT or 'ALL'})")
    mismatches = result["mismatches"]

    if mismatches:
        print(f"Found {len(mismatches)} mismatches:")

        for mismatch in mismatches:
            print(f"- {mismatch}")
    else:
        print("No parity mismatches found between available and byId instruments.")

    if result.get("output_path"):
        print(f"Wrote parity summary to {result['output_path']}")


async def run_contract_parity_check(
    *,
    live: bool,
    product_root: str | None = None,
    limit: int = 0,
    output_path: Path | None = None,
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
        available = await client.available_instruments(
            live=live,
            active_only=False,
        )

        normalized_root = (product_root or "").strip().upper()

        if normalized_root:
            available = [
                inst
                for inst in available
                if _normalize_root(
                    inst.info.get("projectx_name", ""),
                    inst.info.get("projectx_symbol_id", ""),
                )
                == normalized_root
            ]

        if limit > 0:
            available = available[:limit]

        mismatches: list[str] = []
        checked = 0

        for instrument in available:
            contract_id = instrument.info.get("projectx_contract_id")

            if not contract_id:
                continue

            try:
                by_id = await client.contract_by_id(contract_id=contract_id)
            except Exception:
                mismatches.append(f"{contract_id}: missing from byId response")
                continue

            checked += 1

            for key in ("projectx_name", "projectx_symbol_id", "active_contract"):
                left = instrument.info.get(key)
                right = by_id.info.get(key)

                if left != right:
                    mismatches.append(
                        f"{contract_id}: field {key} mismatch available={left!r} byId={right!r}",
                    )

            left_tick = instrument.price_increment
            right_tick = by_id.price_increment

            if left_tick != right_tick:
                mismatches.append(
                    f"{contract_id}: field tick_size mismatch "
                    f"available={left_tick!r} byId={right_tick!r}",
                )

        result = {
            "live": live,
            "product_root": normalized_root or None,
            "checked": checked,
            "mismatches": mismatches,
        }

        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(  # noqa: ASYNC240
                json.dumps(
                    result,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            result["output_path"] = str(output_path)
        return result
    finally:
        client.stop()


if __name__ == "__main__":
    asyncio.run(main())
