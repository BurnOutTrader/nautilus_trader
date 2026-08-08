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
use serde_json::Value;

use crate::websocket::client::{ProjectXSubscription, ProjectXWsClient};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXSubscription {
    #[new]
    fn py_new(target: String, arguments: Vec<String>) -> PyResult<Self> {
        let arguments = arguments
            .into_iter()
            .map(|value| serde_json::from_str::<Value>(&value).map_err(to_pyvalue_err))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self::new(target, arguments))
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXWsClient {
    #[pyo3(name = "connect")]
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.connect().await.map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(name = "disconnect")]
    fn py_disconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.disconnect().await.map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (target, arguments = Vec::new(), track = true))]
    #[pyo3(name = "invoke")]
    fn py_invoke<'py>(
        &self,
        py: Python<'py>,
        target: String,
        arguments: Vec<String>,
        track: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let arguments = arguments
            .into_iter()
            .map(|value| serde_json::from_str::<Value>(&value).map_err(to_pyvalue_err))
            .collect::<PyResult<Vec<_>>>()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .invoke(target, arguments, track)
                .await
                .map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[getter]
    #[pyo3(name = "is_connected")]
    fn py_is_connected(&self) -> bool {
        self.is_connected()
    }

    #[getter]
    #[pyo3(name = "last_message_at_ms")]
    fn py_last_message_at_ms(&self) -> u64 {
        self.last_message_at_ms()
    }

    #[pyo3(name = "take_reconciliation_flag")]
    fn py_take_reconciliation_flag(&self) -> bool {
        Self::take_reconciliation_flag(self)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
