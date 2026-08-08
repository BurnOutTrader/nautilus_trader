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

import asyncio
import json
import sys
import time
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[3])
sys.path[:] = [_REPO_ROOT, *(path for path in sys.path if path != _REPO_ROOT)]

from nautilus_trader.adapters.projectx import load_projectx_env
from nautilus_trader.model import InstrumentId

from examples.live.projectx.projectx_contract_parity_check import (
    run_contract_parity_check,
)
from examples.live.projectx.projectx_front_month_resolver import (
    resolve_front_month_contract,
)
from examples.live.projectx.projectx_instrument_provider import load_provider_snapshot
from examples.live.projectx.projectx_instrument_resolver import (
    _instrument_id_from_contract,
)
from examples.live.projectx.projectx_live_data_probe import run_probe

load_projectx_env()

OUTPUT_PATH = Path(
    "examples/live/projectx/projectx_validation_report.json"
).expanduser()
PRODUCT_ROOT = "MNQ"
INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
ACTIVE_ONLY = True
CONTRACT_PARITY_LIMIT = 0
RUN_SIM = True
RUN_LIVE = False
RUN_FRONT_MONTH = True
RUN_PROVIDER = True
RUN_PARITY = True
RUN_SOAK = False
SOAK_ITERATIONS = 3
SOAK_CAPTURE_SECONDS = 30.0
SOAK_SLEEP_SECONDS = 2.0
SOAK_READY_TIMEOUT_SECONDS = 60.0
SOAK_FIRST_DATA_WAIT_SECONDS = 10.0
SOAK_DEPTH_LEVELS = 10
SOAK_QUOTES = True
SOAK_TRADES = True
SOAK_DEPTH = True
SOAK_UNSUBSCRIBE_ON_STOP = True
SOAK_LOG_DATA = False


def _select_probe_instrument_id(mode_result: dict[str, object]) -> InstrumentId | None:
    provider = mode_result.get("provider")

    if isinstance(provider, dict):
        instrument_ids = provider.get("instrument_ids", [])

        if instrument_ids:
            return InstrumentId.from_str(instrument_ids[0])

    front_month = mode_result.get("front_month")

    if isinstance(front_month, dict):
        contract = front_month.get("contract")

        if isinstance(contract, dict):
            instrument_id = _instrument_id_from_contract(contract)

            if instrument_id is not None:
                return instrument_id

    return INSTRUMENT_ID


async def _run_mode_validation(live: bool) -> dict[str, object]:
    mode_name = "live" if live else "sim"
    result: dict[str, object] = {
        "mode": mode_name,
        "market_data_live": live,
    }

    if RUN_FRONT_MONTH:
        result["front_month"] = await resolve_front_month_contract(
            product_root=PRODUCT_ROOT,
            market_data_live=live,
        )

    if RUN_PROVIDER:
        result["provider"] = await load_provider_snapshot(
            live=live,
            product_root=PRODUCT_ROOT,
            active_only=ACTIVE_ONLY,
        )

    if RUN_PARITY:
        result["parity"] = await run_contract_parity_check(
            live=live,
            product_root=PRODUCT_ROOT,
            limit=CONTRACT_PARITY_LIMIT,
        )

    if RUN_SOAK:
        soak_instrument_id = _select_probe_instrument_id(result)

        if soak_instrument_id is None:
            result["reconnect_soak"] = {
                "status": "skipped",
                "reason": (
                    "No ProjectX instrument ID resolved from provider/front-month results and "
                    "no direct instrument constant was configured."
                ),
            }
        else:
            result["reconnect_soak"] = _run_reconnect_soak(
                live=live,
                instrument_id=soak_instrument_id,
            )
            result["reconnect_soak"]["instrument_id"] = soak_instrument_id.value

    return result


def _run_reconnect_soak(
    *, live: bool, instrument_id: InstrumentId
) -> dict[str, object]:
    iterations: list[dict[str, object]] = []

    for iteration in range(1, SOAK_ITERATIONS + 1):
        started_at = time.time()
        try:
            capture = run_probe(
                instrument_id=instrument_id,
                capture_seconds=SOAK_CAPTURE_SECONDS,
                market_data_live=live,
                ready_timeout_seconds=SOAK_READY_TIMEOUT_SECONDS,
                first_data_wait_seconds=SOAK_FIRST_DATA_WAIT_SECONDS,
                depth_levels=SOAK_DEPTH_LEVELS,
                capture_quotes=SOAK_QUOTES,
                capture_trades=SOAK_TRADES,
                capture_depth=SOAK_DEPTH,
                unsubscribe_on_stop=SOAK_UNSUBSCRIBE_ON_STOP,
                log_data=SOAK_LOG_DATA,
            )
            capture.setdefault("status", "ok")
        except Exception as e:  # noqa: BLE001 - soak harness records every iteration failure
            capture = {
                "status": "error",
                "error": repr(e),
                "instrument_id": instrument_id.value,
                "market_data_live": live,
            }

        capture["iteration"] = iteration
        capture["started_at_unix"] = started_at
        capture["elapsed_seconds"] = round(time.time() - started_at, 3)
        iterations.append(capture)

        if iteration < SOAK_ITERATIONS and SOAK_SLEEP_SECONDS > 0:
            time.sleep(SOAK_SLEEP_SECONDS)

    failures = [item for item in iterations if item["status"] != "ok"]
    empty_runs = [
        item["iteration"]
        for item in iterations
        if item["status"] == "ok" and not item.get("saw_data", False)
    ]
    return {
        "iterations": iterations,
        "failure_count": len(failures),
        "failed_iterations": [item["iteration"] for item in failures],
        "no_data_iterations": empty_runs,
        "soak_iterations": SOAK_ITERATIONS,
        "capture_seconds": SOAK_CAPTURE_SECONDS,
    }


async def main() -> None:
    market_modes: list[bool] = []

    if RUN_SIM:
        market_modes.append(False)

    if RUN_LIVE:
        market_modes.append(True)

    if not market_modes:
        raise ValueError(
            "Enable at least one validation mode by setting RUN_SIM or RUN_LIVE to True.",
        )

    started_at = time.time()
    report = {
        "product_root": PRODUCT_ROOT,
        "instrument_id": INSTRUMENT_ID.value if INSTRUMENT_ID is not None else None,
        "active_only": ACTIVE_ONLY,
        "run_front_month": RUN_FRONT_MONTH,
        "run_provider": RUN_PROVIDER,
        "run_parity": RUN_PARITY,
        "run_soak": RUN_SOAK,
        "results": [],
    }

    for live in market_modes:
        report["results"].append(await _run_mode_validation(live))

    report["elapsed_seconds"] = round(time.time() - started_at, 3)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote ProjectX validation report to {OUTPUT_PATH}")

    for mode_result in report["results"]:
        parity = mode_result.get("parity")
        provider = mode_result.get("provider")
        soak = mode_result.get("reconnect_soak")
        print(f"Mode: {mode_result['mode']}")

        if provider is not None:
            print(f"  Provider count: {provider['count']}")

        if parity is not None:
            print(
                f"  Parity: checked={parity['checked']} mismatches={len(parity['mismatches'])}",
            )

        if soak is not None:
            if soak.get("status") == "skipped":
                print(f"  Reconnect soak: skipped ({soak['reason']})")
            else:
                print(
                    "  Reconnect soak: "
                    f"iterations={soak['soak_iterations']} failures={soak['failure_count']}",
                )


if __name__ == "__main__":
    asyncio.run(main())
