// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 or later.
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::common::{
    databento_to_rithmic_symbol as to_rithmic_symbol,
    projectx_to_rithmic_symbol as from_projectx_symbol,
    rithmic_to_databento_symbol as to_databento_symbol,
    rithmic_to_databento_symbol_with_year as to_databento_symbol_with_year,
    rithmic_to_projectx_symbol as to_projectx_symbol,
    rithmic_to_projectx_symbol_with_year as to_projectx_symbol_with_year,
};

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn databento_to_rithmic_symbol(symbol: &str) -> PyResult<String> {
    to_rithmic_symbol(symbol).map_err(to_pyvalue_err)
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn projectx_to_rithmic_symbol(symbol: &str) -> PyResult<String> {
    from_projectx_symbol(symbol).map_err(to_pyvalue_err)
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn rithmic_to_databento_symbol(symbol: &str) -> PyResult<String> {
    to_databento_symbol(symbol).map_err(to_pyvalue_err)
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn rithmic_to_databento_symbol_with_year(symbol: &str, reference_year: u16) -> PyResult<String> {
    to_databento_symbol_with_year(symbol, reference_year).map_err(to_pyvalue_err)
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn rithmic_to_projectx_symbol(symbol: &str) -> PyResult<String> {
    to_projectx_symbol(symbol).map_err(to_pyvalue_err)
}

#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.rithmic")]
#[pyfunction]
fn rithmic_to_projectx_symbol_with_year(symbol: &str, reference_year: u16) -> PyResult<String> {
    to_projectx_symbol_with_year(symbol, reference_year).map_err(to_pyvalue_err)
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(databento_to_rithmic_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(projectx_to_rithmic_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(rithmic_to_databento_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(rithmic_to_databento_symbol_with_year, m)?)?;
    m.add_function(wrap_pyfunction!(rithmic_to_projectx_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(rithmic_to_projectx_symbol_with_year, m)?)?;
    Ok(())
}
