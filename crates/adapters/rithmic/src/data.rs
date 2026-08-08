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

//! Rithmic data client for market data streaming.
//!
//! This module provides the data client that connects to Rithmic's
//! ticker plant for streaming quotes, trades, and market depth.

mod client;
pub mod custom;
pub mod live;
pub mod volume_profile;

pub(crate) use client::ExtraMarketDataKind;
pub use client::{
    BookDelta, MarketDataEvent, QuoteTick, RithmicBarType, RithmicDataClient, TimeBar, TradeTick,
};
pub use custom::{
    END_OF_DAY_PRICES_TYPE_NAME, INDICATOR_PRICES_TYPE_NAME, OPEN_INTEREST_TYPE_NAME,
    ORDER_PRICE_LIMITS_TYPE_NAME, QUOTE_STATISTICS_TYPE_NAME, RithmicCustomData,
    RithmicEndOfDayPrices, RithmicIndicatorPrices, RithmicOpenInterest, RithmicOrderPriceLimits,
    RithmicQuoteStatistics, RithmicSymbolMarginRate, RithmicTradeStatistics, RithmicVolumeAtPrice,
    SYMBOL_MARGIN_RATE_TYPE_NAME, TRADE_STATISTICS_TYPE_NAME, VOLUME_AT_PRICE_TYPE_NAME,
};
pub use live::RithmicLiveDataClient;
pub use volume_profile::RithmicMinuteVolumeProfileBar;
