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

//! Python enum projections for Rithmic types.

#[cfg(feature = "python")]
use pyo3::prelude::*;
use rithmic_rs::{OrderSide, OrderType, TimeInForce};

use crate::common::enums::ConnectionState;

#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "OrderSide",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyOrderSide {
    Buy,
    Sell,
}

impl From<PyOrderSide> for OrderSide {
    fn from(value: PyOrderSide) -> Self {
        match value {
            PyOrderSide::Buy => Self::Buy,
            PyOrderSide::Sell => Self::Sell,
        }
    }
}

#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "OrderType",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyOrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
}

impl From<PyOrderType> for OrderType {
    fn from(value: PyOrderType) -> Self {
        match value {
            PyOrderType::Market => Self::Market,
            PyOrderType::Limit => Self::Limit,
            PyOrderType::StopMarket => Self::StopMarket,
            PyOrderType::StopLimit => Self::StopLimit,
        }
    }
}

#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "TimeInForce",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyTimeInForce {
    Day,
    Gtc,
    Ioc,
    Fok,
}

impl From<PyTimeInForce> for TimeInForce {
    fn from(value: PyTimeInForce) -> Self {
        match value {
            PyTimeInForce::Day => Self::Day,
            PyTimeInForce::Gtc => Self::Gtc,
            PyTimeInForce::Ioc => Self::Ioc,
            PyTimeInForce::Fok => Self::Fok,
        }
    }
}

#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "OrderStatus",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyOrderStatus {
    Pending,
    Open,
    Partial,
    Complete,
    Cancelled,
    Rejected,
    Expired,
}

#[cfg(feature = "python")]
#[pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")]
#[pyclass(
    name = "ConnectionState",
    module = "nautilus_trader.adapters.rithmic",
    frozen,
    eq,
    eq_int,
    from_py_object,
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Error,
}

impl From<ConnectionState> for PyConnectionState {
    fn from(value: ConnectionState) -> Self {
        match value {
            ConnectionState::Disconnected => Self::Disconnected,
            ConnectionState::Connecting => Self::Connecting,
            ConnectionState::Connected => Self::Connected,
            ConnectionState::Reconnecting => Self::Reconnecting,
            ConnectionState::Error => Self::Error,
        }
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
