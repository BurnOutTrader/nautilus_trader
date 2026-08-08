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

//! Python property projections for Rithmic custom market data.

use nautilus_model::identifiers::InstrumentId;

use crate::data::{
    RithmicEndOfDayPrices, RithmicIndicatorPrices, RithmicMinuteVolumeProfileBar,
    RithmicOpenInterest, RithmicOrderPriceLimits, RithmicQuoteStatistics, RithmicSymbolMarginRate,
    RithmicTradeStatistics, RithmicVolumeAtPrice,
};

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicTradeStatistics {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(open_price)]
    fn py_open_price(&self) -> Option<f64> {
        self.open_price
    }

    #[getter(high_price)]
    fn py_high_price(&self) -> Option<f64> {
        self.high_price
    }

    #[getter(low_price)]
    fn py_low_price(&self) -> Option<f64> {
        self.low_price
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicQuoteStatistics {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(highest_bid_price)]
    fn py_highest_bid_price(&self) -> Option<f64> {
        self.highest_bid_price
    }

    #[getter(lowest_ask_price)]
    fn py_lowest_ask_price(&self) -> Option<f64> {
        self.lowest_ask_price
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicIndicatorPrices {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(opening_indicator)]
    fn py_opening_indicator(&self) -> Option<f64> {
        self.opening_indicator
    }

    #[getter(closing_indicator)]
    fn py_closing_indicator(&self) -> Option<f64> {
        self.closing_indicator
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicOpenInterest {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(should_clear)]
    fn py_should_clear(&self) -> bool {
        self.should_clear
    }

    #[getter(open_interest)]
    fn py_open_interest(&self) -> Option<u64> {
        self.open_interest
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicEndOfDayPrices {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(close_price)]
    fn py_close_price(&self) -> Option<f64> {
        self.close_price
    }

    #[getter(close_date)]
    fn py_close_date(&self) -> Option<String> {
        self.close_date.clone()
    }

    #[getter(adjusted_close_price)]
    fn py_adjusted_close_price(&self) -> Option<f64> {
        self.adjusted_close_price
    }

    #[getter(settlement_price)]
    fn py_settlement_price(&self) -> Option<f64> {
        self.settlement_price
    }

    #[getter(settlement_date)]
    fn py_settlement_date(&self) -> Option<String> {
        self.settlement_date.clone()
    }

    #[getter(settlement_price_type)]
    fn py_settlement_price_type(&self) -> Option<String> {
        self.settlement_price_type.clone()
    }

    #[getter(projected_settlement_price)]
    fn py_projected_settlement_price(&self) -> Option<f64> {
        self.projected_settlement_price
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicOrderPriceLimits {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(high_price_limit)]
    fn py_high_price_limit(&self) -> Option<f64> {
        self.high_price_limit
    }

    #[getter(low_price_limit)]
    fn py_low_price_limit(&self) -> Option<f64> {
        self.low_price_limit
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicSymbolMarginRate {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(is_snapshot)]
    fn py_is_snapshot(&self) -> bool {
        self.is_snapshot
    }

    #[getter(margin_rate)]
    fn py_margin_rate(&self) -> Option<f64> {
        self.margin_rate
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicVolumeAtPrice {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(trade_price)]
    fn py_trade_price(&self) -> Vec<f64> {
        self.trade_price.clone()
    }

    #[getter(volume_at_price)]
    fn py_volume_at_price(&self) -> Vec<i32> {
        self.volume_at_price.clone()
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicMinuteVolumeProfileBar {
    #[getter(instrument_id)]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter(open_price)]
    fn py_open_price(&self) -> f64 {
        self.open_price
    }

    #[getter(high_price)]
    fn py_high_price(&self) -> f64 {
        self.high_price
    }

    #[getter(low_price)]
    fn py_low_price(&self) -> f64 {
        self.low_price
    }

    #[getter(close_price)]
    fn py_close_price(&self) -> f64 {
        self.close_price
    }

    #[getter(volume)]
    fn py_volume(&self) -> u64 {
        self.volume
    }

    #[getter(bid_volume)]
    fn py_bid_volume(&self) -> u64 {
        self.bid_volume
    }

    #[getter(ask_volume)]
    fn py_ask_volume(&self) -> u64 {
        self.ask_volume
    }

    #[getter(num_trades)]
    fn py_num_trades(&self) -> u64 {
        self.num_trades
    }

    #[getter(poc_price)]
    fn py_poc_price(&self) -> Option<f64> {
        self.poc_price
    }

    #[getter(profile_price)]
    fn py_profile_price(&self) -> Vec<f64> {
        self.profile_price.clone()
    }

    #[getter(profile_bid_volume)]
    fn py_profile_bid_volume(&self) -> Vec<i32> {
        self.profile_bid_volume.clone()
    }

    #[getter(profile_ask_volume)]
    fn py_profile_ask_volume(&self) -> Vec<i32> {
        self.profile_ask_volume.clone()
    }

    #[getter(ts_event)]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter(ts_init)]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}
