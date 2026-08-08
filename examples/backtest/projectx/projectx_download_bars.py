#!/usr/bin/env python3
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

"""
Build a Nautilus catalog from ProjectX historical bars.

If you move `REQUEST_END` up to "now", raw ProjectX `request_bars()` calls can include the current
open/in-progress time bar. For closed-only historical catalogs, make sure the final bar satisfies
`bar.ts_event + bar_interval <= cutoff_time` before persisting it.

Live-history retry to `live=false` is explicit. Set
`ALLOW_LIVE_HISTORY_FALLBACK = True` only when you intentionally want the helper
to retry a rejected live-history request against the sim history source.
"""

from __future__ import annotations

import os
import sys
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path


def _ensure_repo_root_on_sys_path() -> None:
    repo_root = str(Path(__file__).resolve().parents[3])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


_ensure_repo_root_on_sys_path()

from nautilus_trader.adapters.projectx import download_bars_to_catalog  # noqa: E402
from nautilus_trader.adapters.projectx import load_projectx_env  # noqa: E402
from nautilus_trader.model import InstrumentId  # noqa: E402


load_projectx_env()


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")
    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the parent directory for the ProjectX example catalog, "
            "for example /tmp/nautilus-data/examples/projectx. "
            "This script writes bars to <NAUTILUS_PATH>/catalog.",
        )
    return Path(nautilus_path).expanduser().resolve()


CATALOG_PATH = _require_example_root() / "catalog"
INSTRUMENT_ID = InstrumentId.from_str("MNQM26.PROJECTX")
BAR_SPEC = "1-MINUTE-LAST"
REQUEST_LIMIT = 500
MARKET_DATA_LIVE = False
ALLOW_LIVE_HISTORY_FALLBACK = False


def _default_request_window() -> tuple[str, str]:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    current_week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0,
        minute=0,
    )
    start = current_week_start - timedelta(days=7)
    end = start + timedelta(days=4, hours=23, minutes=59)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


DEFAULT_REQUEST_START, DEFAULT_REQUEST_END = _default_request_window()
REQUEST_START = DEFAULT_REQUEST_START
REQUEST_END = DEFAULT_REQUEST_END


def main() -> None:
    result = download_bars_to_catalog(
        catalog_path=CATALOG_PATH,
        instrument_id=INSTRUMENT_ID,
        bar_spec=BAR_SPEC,
        start_time=REQUEST_START,
        end_time=REQUEST_END,
        limit=REQUEST_LIMIT,
        market_data_live=MARKET_DATA_LIVE,
        allow_live_history_fallback=ALLOW_LIVE_HISTORY_FALLBACK,
    )

    print(f"Catalog path: {result.catalog_path}")
    print(f"Instrument ID: {result.instrument_id}")
    print(f"Bar type: {result.bar_type}")
    print(f"Market data live: {MARKET_DATA_LIVE}")
    print(f"Allow live history fallback: {ALLOW_LIVE_HISTORY_FALLBACK}")
    print(f"Download window: {result.start_time} -> {result.end_time}")
    print(f"Instruments stored: {result.instrument_count}")
    print(f"Bars stored: {result.bar_count}")


if __name__ == "__main__":
    main()
