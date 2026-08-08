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

//! Rithmic volume profile bar — custom data type.
//!
//! [`RithmicMinuteVolumeProfileBar`] wraps the response from
//! `RithmicHistoryPlantHandle::load_volume_profile_minute_bars`. It carries
//! standard OHLCV fields plus parallel per-price-level bid/ask volume arrays
//! and a computed Point-of-Control (POC).
//!
//! # Usage
//!
//! Request historical volume profile bars from an actor using:
//!
//! ```rust,ignore
//! use nautilus_model::data::DataType;
//! use rithmic_nt::data::volume_profile::VOLUME_PROFILE_TYPE_NAME;
//!
//! // Optional metadata: {"period": 1} sets bar period in minutes (default 1).
//! let data_type = DataType::new(
//!     VOLUME_PROFILE_TYPE_NAME,
//!     None,
//!     Some("ESM5.CME.RITHMIC".to_string()),
//! );
//! actor.request_data(client_id, data_type, Some(start), Some(end), None, None);
//! ```

use std::{any::Any, sync::Arc};

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomDataTrait, HasTsInit},
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};

/// Type name used to identify this custom data type in NautilusTrader's
/// `DataType` registry and JSON serialization envelope.
pub const VOLUME_PROFILE_TYPE_NAME: &str = "RithmicMinuteVolumeProfileBar";

/// A Rithmic volume-profile minute bar.
///
/// Returned by `request_data` when the `DataType` type-name is
/// `"RithmicMinuteVolumeProfileBar"` and the identifier is an
/// `InstrumentId` string (e.g. `"ESM5.CME.RITHMIC"`).
///
/// The optional `period` metadata key (integer) sets the bar period in
/// minutes — defaults to `1` if not supplied.
///
/// # Price levels
///
/// `profile_price`, `profile_bid_volume`, and `profile_ask_volume` are
/// parallel arrays: `profile_price[i]` is the price level and
/// `profile_bid_volume[i]` / `profile_ask_volume[i]` are the bid and ask
/// volumes at that level.
///
/// `poc_price` is derived from these arrays at parse time as the price with
/// the highest total (bid + ask) volume.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", frozen, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RithmicMinuteVolumeProfileBar {
    /// Instrument identifier (e.g. `ESM5.CME.RITHMIC`).
    pub instrument_id: InstrumentId,
    /// Bar open price.
    pub open_price: f64,
    /// Bar high price.
    pub high_price: f64,
    /// Bar low price.
    pub low_price: f64,
    /// Bar close price.
    pub close_price: f64,
    /// Total volume traded in the bar.
    pub volume: u64,
    /// Bid-side volume.
    pub bid_volume: u64,
    /// Ask-side volume.
    pub ask_volume: u64,
    /// Number of trades.
    pub num_trades: u64,
    /// Point of Control: price level with the highest combined (bid + ask) volume.
    /// `None` if `profile_price` is empty.
    pub poc_price: Option<f64>,
    /// Price levels in the volume profile (parallel with the volume arrays below).
    pub profile_price: Vec<f64>,
    /// Bid volume at each price level (parallel with `profile_price`).
    pub profile_bid_volume: Vec<i32>,
    /// Ask volume at each price level (parallel with `profile_price`).
    pub profile_ask_volume: Vec<i32>,
    /// Event timestamp — bar close time in Unix nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp in Unix nanoseconds.
    pub ts_init: UnixNanos,
}

impl RithmicMinuteVolumeProfileBar {
    /// Computes the Point of Control (POC) from volume profile arrays.
    ///
    /// Returns the price level with the highest combined (bid + ask) volume,
    /// or `None` if the arrays are empty.
    pub fn compute_poc(
        profile_price: &[f64],
        profile_bid_volume: &[i32],
        profile_ask_volume: &[i32],
    ) -> Option<f64> {
        profile_price
            .iter()
            .zip(profile_bid_volume.iter().zip(profile_ask_volume.iter()))
            .max_by_key(|entry| {
                let (_, (bid, ask)) = *entry;
                i64::from(*bid) + i64::from(*ask)
            })
            .map(|(price, _)| *price)
    }
}

impl HasTsInit for RithmicMinuteVolumeProfileBar {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for RithmicMinuteVolumeProfileBar {
    fn type_name(&self) -> &'static str {
        VOLUME_PROFILE_TYPE_NAME
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
        VOLUME_PROFILE_TYPE_NAME
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>>
    where
        Self: Sized,
    {
        let parsed: Self = serde_json::from_value(value)?;
        Ok(Arc::new(parsed))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    fn make_bar(
        profile_price: Vec<f64>,
        bid: Vec<i32>,
        ask: Vec<i32>,
    ) -> RithmicMinuteVolumeProfileBar {
        RithmicMinuteVolumeProfileBar {
            instrument_id: InstrumentId::from("ESM5.CME.RITHMIC"),
            open_price: 5000.0,
            high_price: 5010.0,
            low_price: 4990.0,
            close_price: 5005.0,
            volume: 1000,
            bid_volume: 500,
            ask_volume: 500,
            num_trades: 200,
            poc_price: RithmicMinuteVolumeProfileBar::compute_poc(&profile_price, &bid, &ask),
            profile_price,
            profile_bid_volume: bid,
            profile_ask_volume: ask,
            ts_event: UnixNanos::from(1_000_000_000u64),
            ts_init: UnixNanos::from(1_000_000_000u64),
        }
    }

    #[rstest]
    fn test_compute_poc_basic() {
        let prices = vec![5000.0, 5001.0, 5002.0];
        let bid = vec![10, 50, 5];
        let ask = vec![10, 40, 5];
        // 5001.0 has bid+ask = 90, highest
        let poc = RithmicMinuteVolumeProfileBar::compute_poc(&prices, &bid, &ask);
        assert_eq!(poc, Some(5001.0));
    }

    #[rstest]
    fn test_compute_poc_empty() {
        let poc = RithmicMinuteVolumeProfileBar::compute_poc(&[], &[], &[]);
        assert_eq!(poc, None);
    }

    #[rstest]
    fn test_compute_poc_widens_venue_volume_before_adding() {
        let prices = vec![5000.0, 5001.0];
        let bid = vec![i32::MAX, i32::MAX];
        let ask = vec![i32::MAX - 1, i32::MAX];

        let poc = RithmicMinuteVolumeProfileBar::compute_poc(&prices, &bid, &ask);

        assert_eq!(poc, Some(5001.0));
    }

    #[rstest]
    fn test_json_roundtrip() {
        let bar = make_bar(vec![5000.0, 5001.0], vec![100, 200], vec![50, 150]);
        let json = bar.to_json().unwrap();
        let parsed: RithmicMinuteVolumeProfileBar = serde_json::from_str(&json).unwrap();
        assert_eq!(bar, parsed);
    }

    #[rstest]
    fn test_custom_data_trait_type_name() {
        let bar = make_bar(vec![], vec![], vec![]);
        assert_eq!(bar.type_name(), VOLUME_PROFILE_TYPE_NAME);
        assert_eq!(
            RithmicMinuteVolumeProfileBar::type_name_static(),
            VOLUME_PROFILE_TYPE_NAME
        );
    }

    #[rstest]
    fn test_from_json() {
        let bar = make_bar(vec![5000.0], vec![10], vec![20]);
        let json = bar.to_json().unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let arc = RithmicMinuteVolumeProfileBar::from_json(value).unwrap();
        let restored = arc
            .as_any()
            .downcast_ref::<RithmicMinuteVolumeProfileBar>()
            .unwrap();
        assert_eq!(&bar, restored);
    }
}
