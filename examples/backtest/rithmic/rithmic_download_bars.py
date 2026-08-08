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
Use `crates/adapters/rithmic/examples/write_instruments_catalog.rs` first if
you need to pre-populate the catalog with instrument definitions. That writer
defaults to `RITHMIC_TRADEABLE_ONLY=true` for live catalog refreshes; set
`RITHMIC_TRADEABLE_ONLY=0` if you need the supported historical futures list.
The writer only covers the adapter's hard-coded supported Rithmic futures roots.
This example assumes you already know the concrete `INSTRUMENT_ID` to
download.

It uses the adapter-level `download_bars_to_catalog(...)` helper, which opens
one direct historical Rithmic session for the selected contract and writes the
result into a Nautilus catalog.

If you move `REQUEST_END` up to "now", raw Rithmic `request_bars()` calls can include the current
open/in-progress time bar. For closed-only historical catalogs, make sure the final bar satisfies
`bar.ts_event + bar_interval <= cutoff_time` before persisting it.

Edit the constants below when reusing the example.

"""

from __future__ import annotations

import os
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from nautilus_trader.adapters.rithmic import download_bars_to_catalog


def _require_example_root() -> Path:
    nautilus_path = os.environ.get("NAUTILUS_PATH")

    if not nautilus_path:
        raise RuntimeError(
            "Set NAUTILUS_PATH to the parent directory for the Rithmic example catalog, "
            "for example /tmp/nautilus-data/examples/rithmic. "
            "This script writes bars to <NAUTILUS_PATH>/catalog.",
        )
    return Path(nautilus_path).expanduser().resolve()


PROFILE = None
CATALOG_PATH = _require_example_root() / "catalog"
INSTRUMENT_ID = "MNQM6.RITHMIC"
BAR_SPEC = "1-MINUTE-LAST"
REQUEST_LIMIT = 0


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
        profile=PROFILE,
        catalog_path=CATALOG_PATH,
        instrument_id=INSTRUMENT_ID,
        product_code=None,
        bar_spec=BAR_SPEC,
        start_time=REQUEST_START,
        end_time=REQUEST_END,
        limit=REQUEST_LIMIT,
    )

    print(f"Selection: direct instrument_id={INSTRUMENT_ID}")
    print(f"Catalog path: {result.catalog_path}")
    print(f"Instrument ID: {result.instrument_id}")
    print(f"Bar type: {result.bar_type}")
    print(f"Download window: {result.start_time} -> {result.end_time}")
    print(f"Instruments stored: {result.instrument_count}")
    print(f"Bars stored: {result.bar_count}")


if __name__ == "__main__":
    main()
