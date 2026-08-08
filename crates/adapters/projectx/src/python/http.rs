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

use nautilus_core::correctness::{CorrectnessResultExt, FAILED};
use nautilus_core::python::to_pyvalue_err;
use nautilus_model::{
    data::{Bar, BarType, Data},
    instruments::InstrumentAny,
    python::{data::data_to_pyobject, instruments::instrument_any_to_pyobject},
    types::{Price, Quantity},
};
use projectx_client::{BarUnit, ContractId, HistoryRequest, Timestamp};
use pyo3::{prelude::*, types::PyList};

use crate::{
    common::enums::ProjectXHub,
    config::ProjectXConfig,
    factories::{projectx_contract_to_instrument, projectx_select_front_month_contract},
    http::client::ProjectXHttpClient,
};

fn to_client_bar_unit(unit: i32) -> Result<BarUnit, anyhow::Error> {
    match unit {
        1 => Ok(BarUnit::Second),
        2 => Ok(BarUnit::Minute),
        3 => Ok(BarUnit::Hour),
        4 => Ok(BarUnit::Day),
        5 => Ok(BarUnit::Week),
        6 => Ok(BarUnit::Month),
        _ => anyhow::bail!("invalid ProjectX bar unit code: {unit}"),
    }
}

fn to_client_timestamp(value: &str) -> Result<Timestamp, anyhow::Error> {
    Timestamp::new(value).map_err(anyhow::Error::from)
}

fn to_nautilus_bar(bar_type: BarType, bar: &projectx_client::Bar) -> Option<Bar> {
    let ts_event = u64::try_from(bar.t.as_jiff().as_nanosecond())
        .ok()
        .map(nautilus_core::UnixNanos::from)?;
    let precision = [bar.o, bar.h, bar.l, bar.c]
        .into_iter()
        .map(|px| px.scale() as u8)
        .max()
        .unwrap_or(0);
    Some(Bar::new(
        bar_type,
        Price::from_decimal_dp(bar.o, precision).expect_display(FAILED),
        Price::from_decimal_dp(bar.h, precision).expect_display(FAILED),
        Price::from_decimal_dp(bar.l, precision).expect_display(FAILED),
        Price::from_decimal_dp(bar.c, precision).expect_display(FAILED),
        Quantity::new(bar.v as f64, 0),
        ts_event,
        ts_event,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_history_request(
    contract_id: &str,
    start_time: &str,
    end_time: &str,
    unit: i32,
    unit_number: i32,
    limit: i32,
    live: bool,
    include_partial_bar: bool,
) -> Result<HistoryRequest, anyhow::Error> {
    Ok(HistoryRequest::builder(
        ContractId::new(contract_id)?,
        live,
        to_client_timestamp(start_time)?,
        to_client_timestamp(end_time)?,
        to_client_bar_unit(unit)?,
    )
    .unit_number(unit_number)
    .limit(limit)
    .include_partial_bar(include_partial_bar)
    .build()?)
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ProjectXHttpClient {
    #[new]
    fn py_new(config: ProjectXConfig) -> PyResult<Self> {
        Self::from_config(config).map_err(to_pyvalue_err)
    }

    #[staticmethod]
    #[pyo3(name = "from_config")]
    fn py_from_config(config: ProjectXConfig) -> PyResult<Self> {
        Self::from_config(config).map_err(to_pyvalue_err)
    }

    #[getter]
    fn api_base_url(&self) -> String {
        self.urls().api_base.clone()
    }

    #[getter]
    fn rtc_base_url(&self) -> String {
        self.urls().rtc_base.clone()
    }

    #[pyo3(name = "create_ws_client")]
    fn py_create_ws_client(&self, hub: ProjectXHub) -> crate::websocket::client::ProjectXWsClient {
        Self::create_ws_client(self, hub)
    }

    #[pyo3(name = "stop")]
    fn py_stop(&self) {
        Self::stop(self);
    }

    #[pyo3(name = "start")]
    fn py_start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::start(&client).await.map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(name = "authenticate")]
    fn py_authenticate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::start(&client).await.map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(name = "validate")]
    fn py_validate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::start(&client).await.map_err(to_pyvalue_err)?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    #[pyo3(name = "retrieve_bars")]
    #[pyo3(signature = (contract_id, bar_type, start_time, end_time, unit, unit_number, limit = 500, live = false, include_partial_bar = false))]
    #[allow(clippy::too_many_arguments)]
    fn py_retrieve_bars<'py>(
        &self,
        py: Python<'py>,
        contract_id: String,
        bar_type: BarType,
        start_time: String,
        end_time: String,
        unit: i32,
        unit_number: i32,
        limit: i32,
        live: bool,
        include_partial_bar: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let request = build_history_request(
                &contract_id,
                &start_time,
                &end_time,
                unit,
                unit_number,
                limit,
                live,
                include_partial_bar,
            )
            .map_err(to_pyvalue_err)?;
            let bars = client
                .retrieve_bars(&request)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_bars: PyResult<Vec<_>> = bars
                    .iter()
                    .filter_map(|bar| to_nautilus_bar(bar_type, bar))
                    .map(|bar| data_to_pyobject(py, Data::Bar(bar)))
                    .collect();
                Ok(PyList::new(py, py_bars?)?.into_any().unbind())
            })
        })
    }

    #[pyo3(name = "resolve_front_month_instrument")]
    #[pyo3(signature = (product_root, live = false))]
    fn py_resolve_front_month_instrument<'py>(
        &self,
        py: Python<'py>,
        product_root: String,
        live: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contracts = client
                .available_contracts(live)
                .await
                .map_err(to_pyvalue_err)?;
            let selected = projectx_select_front_month_contract(&contracts, &product_root)
                .ok_or_else(|| {
                    to_pyvalue_err(anyhow::anyhow!(
                        "No ProjectX contract found for product root {product_root}"
                    ))
                })?;
            let instrument = projectx_contract_to_instrument(&selected).map_err(to_pyvalue_err)?;
            Python::attach(|py| instrument_any_to_pyobject(py, instrument))
        })
    }

    #[pyo3(name = "contract_by_id")]
    fn py_contract_by_id<'py>(
        &self,
        py: Python<'py>,
        contract_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contract = client
                .contract_by_id(&ContractId::new(&contract_id).map_err(to_pyvalue_err)?)
                .await
                .map_err(to_pyvalue_err)?;
            let instrument = projectx_contract_to_instrument(&contract).map_err(to_pyvalue_err)?;
            Python::attach(|py| instrument_any_to_pyobject(py, instrument))
        })
    }

    #[pyo3(name = "available_instruments")]
    #[pyo3(signature = (live = false, active_only = false, product_root = None))]
    fn py_available_instruments<'py>(
        &self,
        py: Python<'py>,
        live: bool,
        active_only: bool,
        product_root: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let contracts = client
                .available_contracts(live)
                .await
                .map_err(to_pyvalue_err)?;

            let instruments: Vec<InstrumentAny> = contracts
                .into_iter()
                .filter(|contract| !active_only || contract.active_contract)
                .filter(|contract| {
                    crate::factories::projectx_contract_matches_product_root(
                        contract,
                        product_root.as_deref(),
                    )
                })
                .map(|contract| projectx_contract_to_instrument(&contract))
                .collect::<anyhow::Result<Vec<_>>>()
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_instruments: PyResult<Vec<_>> = instruments
                    .into_iter()
                    .map(|inst| instrument_any_to_pyobject(py, inst))
                    .collect();
                Ok(PyList::new(py, py_instruments?)?.into_any().unbind())
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
