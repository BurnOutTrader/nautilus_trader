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

//! Python bindings for enums.

#[cfg(feature = "python")]
use pyo3::prelude::*;
use rithmic_rs::{OrderSide, OrderStatus, OrderType, TimeInForce};

use crate::common::enums::ConnectionState;

#[cfg(feature = "python")]
#[pyclass(name = "OrderSide", from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderSide {
    inner: OrderSide,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderSide {
    #[classattr]
    const BUY: Self = Self {
        inner: OrderSide::Buy,
    };

    #[classattr]
    const SELL: Self = Self {
        inner: OrderSide::Sell,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("OrderSide.{}", self.inner)
    }
}

impl From<PyOrderSide> for OrderSide {
    fn from(py_side: PyOrderSide) -> Self {
        py_side.inner
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "OrderType", from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderType {
    inner: OrderType,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderType {
    #[classattr]
    const MARKET: Self = Self {
        inner: OrderType::Market,
    };

    #[classattr]
    const LIMIT: Self = Self {
        inner: OrderType::Limit,
    };

    #[classattr]
    const STOP_MARKET: Self = Self {
        inner: OrderType::StopMarket,
    };

    #[classattr]
    const STOP_LIMIT: Self = Self {
        inner: OrderType::StopLimit,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("OrderType.{}", self.inner)
    }
}

impl From<PyOrderType> for OrderType {
    fn from(py_type: PyOrderType) -> Self {
        py_type.inner
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "TimeInForce", from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyTimeInForce {
    inner: TimeInForce,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyTimeInForce {
    #[classattr]
    const DAY: Self = Self {
        inner: TimeInForce::Day,
    };

    #[classattr]
    const GTC: Self = Self {
        inner: TimeInForce::Gtc,
    };

    #[classattr]
    const IOC: Self = Self {
        inner: TimeInForce::Ioc,
    };

    #[classattr]
    const FOK: Self = Self {
        inner: TimeInForce::Fok,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("TimeInForce.{}", self.inner)
    }
}

impl From<PyTimeInForce> for TimeInForce {
    fn from(py_tif: PyTimeInForce) -> Self {
        py_tif.inner
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "OrderStatus", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyOrderStatus {
    inner: OrderStatus,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyOrderStatus {
    #[classattr]
    const PENDING: Self = Self {
        inner: OrderStatus::Pending,
    };

    #[classattr]
    const OPEN: Self = Self {
        inner: OrderStatus::Open,
    };

    #[classattr]
    const PARTIAL: Self = Self {
        inner: OrderStatus::Partial,
    };

    #[classattr]
    const COMPLETE: Self = Self {
        inner: OrderStatus::Complete,
    };

    #[classattr]
    const CANCELLED: Self = Self {
        inner: OrderStatus::Cancelled,
    };

    #[classattr]
    const REJECTED: Self = Self {
        inner: OrderStatus::Rejected,
    };

    #[classattr]
    const EXPIRED: Self = Self {
        inner: OrderStatus::Expired,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("OrderStatus.{}", self.inner)
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "ConnectionState", skip_from_py_object)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")]
#[derive(Clone)]
pub(crate) struct PyConnectionState {
    inner: ConnectionState,
}

#[cfg(feature = "python")]
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyConnectionState {
    #[classattr]
    const DISCONNECTED: Self = Self {
        inner: ConnectionState::Disconnected,
    };

    #[classattr]
    const CONNECTING: Self = Self {
        inner: ConnectionState::Connecting,
    };

    #[classattr]
    const CONNECTED: Self = Self {
        inner: ConnectionState::Connected,
    };

    #[classattr]
    const RECONNECTING: Self = Self {
        inner: ConnectionState::Reconnecting,
    };

    #[classattr]
    const ERROR: Self = Self {
        inner: ConnectionState::Error,
    };

    #[pyo3(name = "__repr__")]
    fn py_repr(&self) -> String {
        format!("ConnectionState.{:?}", self.inner)
    }
}

#[cfg(feature = "python")]
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOrderSide>()?;
    m.add_class::<PyOrderType>()?;
    m.add_class::<PyTimeInForce>()?;
    m.add_class::<PyOrderStatus>()?;
    m.add_class::<PyConnectionState>()?;
    Ok(())
}
