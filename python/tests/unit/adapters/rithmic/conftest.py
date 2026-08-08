import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
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

import os

import pytest

from nautilus_trader._libnautilus.rithmic import RithmicDataClientConfig
from nautilus_trader._libnautilus.rithmic import RithmicEnv
from nautilus_trader._libnautilus.rithmic import RithmicInstrumentProvider
from nautilus_trader.adapters.rithmic import RITHMIC_VENUE
from nautilus_trader.adapters.rithmic import load_rithmic_env_file
from nautilus_trader.model import AssetClass
from nautilus_trader.model import Currency
from nautilus_trader.model import FuturesContract
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import Symbol
from nautilus_trader.model import Venue


# ---------------------------------------------------------------------------
# Live-API skip guard
# Apply `@live_required` to any test that connects to Rithmic.
# ---------------------------------------------------------------------------
load_rithmic_env_file()

_LIVE_CREDS_AVAILABLE = bool(
    os.environ.get("RITHMIC_USERNAME") and os.environ.get("RITHMIC_APP_NAME"),
)

live_required = pytest.mark.skipif(
    not _LIVE_CREDS_AVAILABLE,
    reason="Live Rithmic credentials not configured (set RITHMIC_USERNAME and RITHMIC_APP_NAME)",
)


@pytest.fixture
def venue() -> Venue:
    return RITHMIC_VENUE


@pytest.fixture
def instrument():
    # Synthetic instrument required by the parent conftest protocol.
    # For tests that hit the live API, use `live_mnq` or `live_es` instead —
    # they call `load_front_month_async` so no expiry is ever hardcoded.
    return FuturesContract(
        instrument_id=InstrumentId(Symbol("MNQM6"), RITHMIC_VENUE),
        raw_symbol=Symbol("MNQM6"),
        underlying="MNQ",
        asset_class=AssetClass.INDEX,
        currency=Currency.from_str("USD"),
        price_precision=2,
        price_increment=Price.from_str("0.25"),
        multiplier=Quantity.from_int(1),
        lot_size=Quantity.from_int(1),
        activation_ns=1_700_000_000_000_000_000,
        expiration_ns=1_740_000_000_000_000_000,
        ts_event=0,
        ts_init=0,
    )


# ---------------------------------------------------------------------------
# Live-API fixtures
# These require real Rithmic credentials and a network connection.
# Always look up the front month rather than hardcoding an expiry.
# ---------------------------------------------------------------------------


@pytest.fixture
async def live_provider() -> RithmicInstrumentProvider:
    """
    Instrument provider connected via RITHMIC_* env vars.
    """
    return RithmicInstrumentProvider(RithmicDataClientConfig.from_env())


@pytest.fixture
async def live_mnq(live_provider: RithmicInstrumentProvider):
    """
    Load the current front-month MNQ contract (CME).
    """
    return await live_provider.load_front_month_async("MNQ", "CME")


@pytest.fixture
async def live_es(live_provider: RithmicInstrumentProvider):
    """
    Load the current front-month ES contract (CME).
    """
    return await live_provider.load_front_month_async("ES", "CME")


@pytest.fixture
def instrument_provider() -> RithmicInstrumentProvider:
    return RithmicInstrumentProvider(
        RithmicDataClientConfig(
            environment=RithmicEnv.DEMO,
            username="u",
            password="p",
            system_name="Apex",
            app_name="TestApp",
        ),
    )
