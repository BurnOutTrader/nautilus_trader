# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---
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

# %% [markdown]
# ## imports

# %%
# Note: Use the jupytext python package to open this file as a notebook.

# %%
import os
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from nautilus_trader.adapters.projectx import build_external_bar_type
from nautilus_trader.adapters.projectx import download_bars_to_catalog
from nautilus_trader.adapters.projectx import load_projectx_env
from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.model import Bar
from nautilus_trader.model import InstrumentId
from nautilus_trader.persistence import ParquetDataCatalog


load_projectx_env()


# %% [markdown]
# ## parameters

# %%
# Credentials are read from:
# - PROJECTX_USERNAME
# - PROJECTX_API_KEY

nautilus_path = os.environ.get("NAUTILUS_PATH")

if not nautilus_path:
    raise RuntimeError(
        "Set NAUTILUS_PATH to the ProjectX example root, "
        "for example /tmp/nautilus-data/examples/projectx",
    )

catalog_path = Path(nautilus_path).expanduser() / "catalog"
catalog_path.mkdir(parents=True, exist_ok=True)

market_data_live = False
instrument_id = InstrumentId.from_str("MNQM26.PROJECTX")
bar_spec = "1-MINUTE-LAST"
bar_type = build_external_bar_type(instrument_id, bar_spec)


def default_request_window() -> tuple[str, str]:
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


request_start, request_end = default_request_window()
request_limit = 500


# %% [markdown]
# ## download bars into catalog

# %%
result = download_bars_to_catalog(
    catalog_path=catalog_path,
    instrument_id=instrument_id,
    bar_spec=bar_spec,
    start_time=request_start,
    end_time=request_end,
    limit=request_limit,
    market_data_live=market_data_live,
)
print(f"Instrument stored: {result.instrument_id}")
print(f"Bar type stored: {result.bar_type}")
print(f"Bars stored: {result.bar_count}")


# %% [markdown]
# ## verify parquet output

# %%
catalog = ParquetDataCatalog(str(catalog_path))
bars = catalog.bars(
    bar_types=[str(bar_type)],
    start=request_start,
    end=request_end,
)
print(f"Bars loaded back from catalog: {len(bars)}")

parquet_files = sorted((catalog_path / "data").rglob("*.parquet"))
parquet_files[:5]


# %% [markdown]
# ## backtest handoff config

# %%
backtest_data_config = BacktestDataConfig(
    catalog_path=str(catalog_path),
    data_cls=Bar,
    instrument_id=instrument_id,
    bar_spec=bar_spec,
    start_time=request_start,
    end_time=request_end,
)
