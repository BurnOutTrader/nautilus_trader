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

//! Rithmic-specific custom market data types.

use std::{any::Any, sync::Arc};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{HasTsInit, custom::CustomDataTrait},
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};

pub const TRADE_STATISTICS_TYPE_NAME: &str = "RithmicTradeStatistics";
pub const QUOTE_STATISTICS_TYPE_NAME: &str = "RithmicQuoteStatistics";
pub const INDICATOR_PRICES_TYPE_NAME: &str = "RithmicIndicatorPrices";
pub const OPEN_INTEREST_TYPE_NAME: &str = "RithmicOpenInterest";
pub const END_OF_DAY_PRICES_TYPE_NAME: &str = "RithmicEndOfDayPrices";
pub const ORDER_PRICE_LIMITS_TYPE_NAME: &str = "RithmicOrderPriceLimits";
pub const SYMBOL_MARGIN_RATE_TYPE_NAME: &str = "RithmicSymbolMarginRate";
pub const VOLUME_AT_PRICE_TYPE_NAME: &str = "RithmicVolumeAtPrice";

macro_rules! impl_rithmic_custom_data {
    ($ty:ty, $name:expr) => {
        impl HasTsInit for $ty {
            fn ts_init(&self) -> UnixNanos {
                self.ts_init
            }
        }

        impl CustomDataTrait for $ty {
            fn type_name(&self) -> &'static str {
                $name
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn ts_event(&self) -> UnixNanos {
                self.ts_event
            }

            fn to_json(&self) -> anyhow::Result<String> {
                Ok(serde_json::to_string(self)?)
            }

            fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
                Arc::new(self.clone())
            }

            fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
                other
                    .as_any()
                    .downcast_ref::<Self>()
                    .is_some_and(|o| self == o)
            }

            #[cfg(feature = "python")]
            fn to_pyobject(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                nautilus_model::data::custom::clone_pyclass_to_pyobject(self, py)
            }

            fn type_name_static() -> &'static str
            where
                Self: Sized,
            {
                $name
            }

            fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>>
            where
                Self: Sized,
            {
                let parsed: Self = serde_json::from_value(value)?;
                Ok(Arc::new(parsed))
            }
        }
    };
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicTradeStatistics {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicTradeStatistics, TRADE_STATISTICS_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicQuoteStatistics {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub highest_bid_price: Option<f64>,
    pub lowest_ask_price: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicQuoteStatistics, QUOTE_STATISTICS_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicIndicatorPrices {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub opening_indicator: Option<f64>,
    pub closing_indicator: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicIndicatorPrices, INDICATOR_PRICES_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicOpenInterest {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub should_clear: bool,
    pub open_interest: Option<u64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicOpenInterest, OPEN_INTEREST_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicEndOfDayPrices {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub close_price: Option<f64>,
    pub close_date: Option<String>,
    pub adjusted_close_price: Option<f64>,
    pub settlement_price: Option<f64>,
    pub settlement_date: Option<String>,
    pub settlement_price_type: Option<String>,
    pub projected_settlement_price: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicEndOfDayPrices, END_OF_DAY_PRICES_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicOrderPriceLimits {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub high_price_limit: Option<f64>,
    pub low_price_limit: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicOrderPriceLimits, ORDER_PRICE_LIMITS_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicSymbolMarginRate {
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub margin_rate: Option<f64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicSymbolMarginRate, SYMBOL_MARGIN_RATE_TYPE_NAME);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RithmicVolumeAtPrice {
    pub instrument_id: InstrumentId,
    pub trade_price: Vec<f64>,
    pub volume_at_price: Vec<i32>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl_rithmic_custom_data!(RithmicVolumeAtPrice, VOLUME_AT_PRICE_TYPE_NAME);

#[derive(Clone, Debug, PartialEq)]
pub enum RithmicCustomData {
    TradeStatistics(RithmicTradeStatistics),
    QuoteStatistics(RithmicQuoteStatistics),
    IndicatorPrices(RithmicIndicatorPrices),
    OpenInterest(RithmicOpenInterest),
    EndOfDayPrices(RithmicEndOfDayPrices),
    OrderPriceLimits(RithmicOrderPriceLimits),
    SymbolMarginRate(RithmicSymbolMarginRate),
}
