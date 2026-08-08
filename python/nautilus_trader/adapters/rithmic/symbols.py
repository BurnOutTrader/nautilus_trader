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
Symbol translation helpers for the Rithmic adapter.
"""

from __future__ import annotations

from nautilus_trader._libnautilus.rithmic import databento_to_rithmic_symbol
from nautilus_trader._libnautilus.rithmic import projectx_to_rithmic_symbol
from nautilus_trader._libnautilus.rithmic import rithmic_to_databento_symbol
from nautilus_trader._libnautilus.rithmic import rithmic_to_databento_symbol_with_year
from nautilus_trader._libnautilus.rithmic import rithmic_to_projectx_symbol
from nautilus_trader._libnautilus.rithmic import rithmic_to_projectx_symbol_with_year


__all__ = [
    "databento_to_rithmic_symbol",
    "projectx_to_rithmic_symbol",
    "rithmic_to_databento_symbol",
    "rithmic_to_databento_symbol_with_year",
    "rithmic_to_projectx_symbol",
    "rithmic_to_projectx_symbol_with_year",
]
