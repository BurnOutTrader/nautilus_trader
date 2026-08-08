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

from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.projectx import PROJECTX_VENUE
from nautilus_trader.adapters.projectx import ProjectXInstrumentProvider
from nautilus_trader.model import AssetClass
from nautilus_trader.model import Currency
from nautilus_trader.model import FuturesContract
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import Symbol
from nautilus_trader.model import Venue


@pytest.fixture
def venue() -> Venue:
    return PROJECTX_VENUE


@pytest.fixture
def instrument():
    return FuturesContract(
        instrument_id=InstrumentId(Symbol("MESM26"), PROJECTX_VENUE),
        raw_symbol=Symbol("MESM26"),
        underlying="MES",
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


@pytest.fixture
def instrument_provider() -> ProjectXInstrumentProvider:
    return ProjectXInstrumentProvider(client=MagicMock())


@pytest.fixture
def data_client():
    return None


@pytest.fixture
def exec_client():
    return None
