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

from nautilus_trader.adapters.projectx import databento_to_projectx_adapter_symbol
from nautilus_trader.adapters.projectx import (
    databento_to_projectx_adapter_symbol as module_databento_to_projectx_adapter_symbol,
)
from nautilus_trader.adapters.projectx import databento_to_projectx_contract_id
from nautilus_trader.adapters.projectx import projectx_to_databento_symbol_with_year
from nautilus_trader.adapters.projectx import projectx_to_rithmic_symbol
from nautilus_trader.adapters.projectx import rithmic_to_projectx_symbol_with_year


def test_package_exports_projectx_symbol_helpers() -> None:
    assert databento_to_projectx_adapter_symbol is module_databento_to_projectx_adapter_symbol


def test_convert_databento_symbol_to_projectx_adapter_symbol() -> None:
    assert databento_to_projectx_adapter_symbol("MNQM26") == "MNQM26"


def test_convert_databento_symbol_to_projectx_contract_id() -> None:
    assert databento_to_projectx_contract_id("MNQM26") == "CON.F.US.MNQ.M26"


def test_convert_projectx_symbol_to_rithmic_symbol() -> None:
    assert projectx_to_rithmic_symbol("CON.F.US.MNQ.M26") == "MNQM6"
    assert projectx_to_rithmic_symbol("MNQM26") == "MNQM6"


def test_convert_projectx_vendor_alias_to_databento_symbol_with_year() -> None:
    assert projectx_to_databento_symbol_with_year("MNQM6", 26) == "MNQM26"
    assert projectx_to_databento_symbol_with_year("MNQZ0", 29) == "MNQZ30"


def test_convert_rithmic_symbol_to_projectx_symbol_with_year() -> None:
    assert rithmic_to_projectx_symbol_with_year("MNQM6", 26) == "MNQM26"
