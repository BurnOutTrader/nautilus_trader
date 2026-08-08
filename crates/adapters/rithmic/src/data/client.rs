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

//! Rithmic data client implementation.

use std::{fmt::Debug, sync::Arc};

use dashmap::DashMap;
use nautilus_model::data::{InstrumentStatus, OrderBookDepth10};
use rithmic_rs::rti::{request_tick_bar_update, request_time_bar_replay::BarType as TimeBarType};

use crate::{
    common::{
        enums::ConnectionState,
        types::{ExchangeId, RithmicSymbol, UnixNanos},
    },
    data::{
        END_OF_DAY_PRICES_TYPE_NAME, INDICATOR_PRICES_TYPE_NAME, OPEN_INTEREST_TYPE_NAME,
        ORDER_PRICE_LIMITS_TYPE_NAME, QUOTE_STATISTICS_TYPE_NAME, SYMBOL_MARGIN_RATE_TYPE_NAME,
        TRADE_STATISTICS_TYPE_NAME, custom::RithmicCustomData,
    },
    error::{Result, RithmicError},
    gateway::RithmicGateway,
};

/// Rithmic history-plant bar types supported by the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RithmicBarType {
    SecondBar,
    MinuteBar,
    DailyBar,
    WeeklyBar,
    TickBar,
}

impl RithmicBarType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SecondBar => "SecondBar",
            Self::MinuteBar => "MinuteBar",
            Self::DailyBar => "DailyBar",
            Self::WeeklyBar => "WeeklyBar",
            Self::TickBar => "TickBar",
        }
    }
}

impl From<TimeBarType> for RithmicBarType {
    fn from(value: TimeBarType) -> Self {
        match value {
            TimeBarType::SecondBar => Self::SecondBar,
            TimeBarType::MinuteBar => Self::MinuteBar,
            TimeBarType::DailyBar => Self::DailyBar,
            TimeBarType::WeeklyBar => Self::WeeklyBar,
        }
    }
}

/// Quote tick data.
#[derive(Debug, Clone)]
pub struct QuoteTick {
    /// Instrument symbol.
    pub symbol: RithmicSymbol,
    /// Exchange.
    pub exchange: ExchangeId,
    /// Best bid price.
    pub bid_price: f64,
    /// Best ask price.
    pub ask_price: f64,
    /// Bid size.
    pub bid_size: f64,
    /// Ask size.
    pub ask_size: f64,
    /// Price precision (number of decimal places).
    pub price_precision: u8,
    /// Size precision (number of decimal places).
    pub size_precision: u8,
    /// Timestamp in nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp.
    pub ts_init: UnixNanos,
}

/// Trade tick data.
#[derive(Debug, Clone)]
pub struct TradeTick {
    /// Instrument symbol.
    pub symbol: RithmicSymbol,
    /// Exchange.
    pub exchange: ExchangeId,
    /// Trade price.
    pub price: f64,
    /// Trade size.
    pub size: f64,
    /// Aggressor side ("BUY" or "SELL").
    pub aggressor_side: String,
    /// Trade ID.
    pub trade_id: String,
    /// Price precision (number of decimal places).
    pub price_precision: u8,
    /// Size precision (number of decimal places).
    pub size_precision: u8,
    /// Timestamp in nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp.
    pub ts_init: UnixNanos,
}

/// Live history-plant bar data.
#[derive(Debug, Clone)]
pub struct TimeBar {
    /// Instrument symbol.
    pub symbol: RithmicSymbol,
    /// Exchange.
    pub exchange: ExchangeId,
    /// Rithmic bar type.
    pub bar_type: RithmicBarType,
    /// The bar period/step (for example `1` for a 1-minute bar).
    pub bar_period: i32,
    /// Open price.
    pub open_price: f64,
    /// High price.
    pub high_price: f64,
    /// Low price.
    pub low_price: f64,
    /// Close price.
    pub close_price: f64,
    /// Trade volume.
    pub volume: f64,
    /// Price precision (number of decimal places).
    pub price_precision: u8,
    /// Size precision (number of decimal places).
    pub size_precision: u8,
    /// Rithmic bar marker, typically epoch seconds for the bar close.
    pub marker: Option<i64>,
    /// Event timestamp in nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp in nanoseconds.
    pub ts_init: UnixNanos,
}

/// A single order-book delta (depth-by-order update).
#[derive(Debug, Clone)]
pub struct BookDelta {
    /// Instrument symbol.
    pub symbol: RithmicSymbol,
    /// Exchange.
    pub exchange: ExchangeId,
    /// "ADD" or "REMOVE".
    pub action: String,
    /// "BUY" or "SELL".
    pub side: String,
    /// Price level.
    pub price: f64,
    /// Order size.
    pub size: f64,
    /// Order priority (used as order_id).
    pub order_id: u64,
    /// Venue sequence number.
    pub sequence: u64,
    /// Record flags (e.g. RecordFlag::F_LAST = 0x80 on the last delta in a batch).
    pub flags: u8,
    /// Price precision (number of decimal places).
    pub price_precision: u8,
    /// Size precision (number of decimal places).
    pub size_precision: u8,
    /// Event timestamp in nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp in nanoseconds.
    pub ts_init: UnixNanos,
}

/// Subscription tracking for order-book feeds on a single instrument.
#[derive(Debug, Clone, Default)]
struct BookSubscription {
    deltas: bool,
    depth10: bool,
}

/// Additional ticker-plant surfaces tracked per instrument.
#[derive(Debug, Clone, Default)]
struct ExtraMarketDataSubscription {
    instrument_status: bool,
    trade_statistics: bool,
    quote_statistics: bool,
    indicator_prices: bool,
    open_interest: bool,
    end_of_day_prices: bool,
    order_price_limits: bool,
    symbol_margin_rate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtraMarketDataKind {
    InstrumentStatus,
    TradeStatistics,
    QuoteStatistics,
    IndicatorPrices,
    OpenInterest,
    EndOfDayPrices,
    OrderPriceLimits,
    SymbolMarginRate,
}

impl ExtraMarketDataKind {
    pub(crate) fn from_custom_type(type_name: &str) -> Option<Self> {
        match type_name {
            TRADE_STATISTICS_TYPE_NAME => Some(Self::TradeStatistics),
            QUOTE_STATISTICS_TYPE_NAME => Some(Self::QuoteStatistics),
            INDICATOR_PRICES_TYPE_NAME => Some(Self::IndicatorPrices),
            OPEN_INTEREST_TYPE_NAME => Some(Self::OpenInterest),
            END_OF_DAY_PRICES_TYPE_NAME => Some(Self::EndOfDayPrices),
            ORDER_PRICE_LIMITS_TYPE_NAME => Some(Self::OrderPriceLimits),
            SYMBOL_MARGIN_RATE_TYPE_NAME => Some(Self::SymbolMarginRate),
            _ => None,
        }
    }
}

fn set_extra_market_data_flag(
    subscription: &mut ExtraMarketDataSubscription,
    kind: ExtraMarketDataKind,
    value: bool,
) {
    match kind {
        ExtraMarketDataKind::InstrumentStatus => subscription.instrument_status = value,
        ExtraMarketDataKind::TradeStatistics => subscription.trade_statistics = value,
        ExtraMarketDataKind::QuoteStatistics => subscription.quote_statistics = value,
        ExtraMarketDataKind::IndicatorPrices => subscription.indicator_prices = value,
        ExtraMarketDataKind::OpenInterest => subscription.open_interest = value,
        ExtraMarketDataKind::EndOfDayPrices => subscription.end_of_day_prices = value,
        ExtraMarketDataKind::OrderPriceLimits => subscription.order_price_limits = value,
        ExtraMarketDataKind::SymbolMarginRate => subscription.symbol_margin_rate = value,
    }
}

fn get_extra_market_data_flag(
    subscription: &ExtraMarketDataSubscription,
    kind: ExtraMarketDataKind,
) -> bool {
    match kind {
        ExtraMarketDataKind::InstrumentStatus => subscription.instrument_status,
        ExtraMarketDataKind::TradeStatistics => subscription.trade_statistics,
        ExtraMarketDataKind::QuoteStatistics => subscription.quote_statistics,
        ExtraMarketDataKind::IndicatorPrices => subscription.indicator_prices,
        ExtraMarketDataKind::OpenInterest => subscription.open_interest,
        ExtraMarketDataKind::EndOfDayPrices => subscription.end_of_day_prices,
        ExtraMarketDataKind::OrderPriceLimits => subscription.order_price_limits,
        ExtraMarketDataKind::SymbolMarginRate => subscription.symbol_margin_rate,
    }
}

fn extra_market_data_kinds(subscription: &ExtraMarketDataSubscription) -> Vec<ExtraMarketDataKind> {
    let mut kinds = Vec::new();

    if subscription.instrument_status {
        kinds.push(ExtraMarketDataKind::InstrumentStatus);
    }

    if subscription.trade_statistics {
        kinds.push(ExtraMarketDataKind::TradeStatistics);
    }

    if subscription.quote_statistics {
        kinds.push(ExtraMarketDataKind::QuoteStatistics);
    }

    if subscription.indicator_prices {
        kinds.push(ExtraMarketDataKind::IndicatorPrices);
    }

    if subscription.open_interest {
        kinds.push(ExtraMarketDataKind::OpenInterest);
    }

    if subscription.end_of_day_prices {
        kinds.push(ExtraMarketDataKind::EndOfDayPrices);
    }

    if subscription.order_price_limits {
        kinds.push(ExtraMarketDataKind::OrderPriceLimits);
    }

    if subscription.symbol_margin_rate {
        kinds.push(ExtraMarketDataKind::SymbolMarginRate);
    }

    kinds
}

/// Market data event emitted by the data client.
#[derive(Debug, Clone)]
pub enum MarketDataEvent {
    /// Quote tick (best bid/offer update).
    Quote(QuoteTick),
    /// Trade tick (last trade).
    Trade(TradeTick),
    /// Time bar update.
    Bar(TimeBar),
    /// Order book delta (depth-by-order update).
    BookDelta(BookDelta),
    /// Aggregated top-10 order book depth update.
    Depth10(Arc<OrderBookDepth10>),
    /// Instrument market status update.
    InstrumentStatus(InstrumentStatus),
    /// Rithmic-specific custom market data surface.
    Custom(RithmicCustomData),
    /// Connection state change.
    ConnectionState(ConnectionState),
    /// Successfully reconnected after disconnect.
    Reconnected,
    /// Successfully authenticated with venue.
    Authenticated,
    /// Error event.
    Error(String),
}

/// Subscription tracking for a single instrument.
#[derive(Debug, Clone, Default)]
struct InstrumentSubscription {
    quotes: bool,
    trades: bool,
}

/// Rithmic market data client.
///
/// Provides a high-level interface for subscribing to market data through
/// the `RithmicGateway`. The gateway handles the actual connection and message
/// processing; this client manages subscription state and provides a clean API.
///
/// # Example
///
/// ```rust,ignore
/// use rithmic_nt::{RithmicGateway, RithmicDataClient, GatewayConfig};
/// use std::sync::Arc;
///
/// let config = GatewayConfig::from_env()?;
/// let mut gateway = RithmicGateway::new(config);
///
/// // Take the receiver BEFORE wrapping in Arc (requires &mut self)
/// let mut rx = gateway.subscribe_market_data_events();
///
/// gateway.connect().await?;
///
/// let gateway = Arc::new(gateway);
/// let client = RithmicDataClient::new(Arc::clone(&gateway));
///
/// client.subscribe_quotes("ESZ4", "CME").await?;
///
/// while let Some(event) = rx.recv().await {
///     match event {
///         MarketDataEvent::Quote(q) => println!("Quote: {:?}", q),
///         MarketDataEvent::Trade(t) => println!("Trade: {:?}", t),
///         _ => {}
///     }
/// }
/// ```
pub struct RithmicDataClient {
    gateway: Arc<tokio::sync::RwLock<RithmicGateway>>,
    /// Tracks which instruments have active subscriptions.
    /// Key: "EXCHANGE:SYMBOL" (e.g., "CME:ESZ4")
    subscriptions: DashMap<String, InstrumentSubscription>,
    /// Tracks live bar subscriptions.
    /// Key: "EXCHANGE:SYMBOL:BarType:Period" (e.g., "CME:ESZ4:MinuteBar:1")
    bar_subscriptions: DashMap<String, ()>,
    /// Tracks live tick-bar subscriptions.
    /// Key: "EXCHANGE:SYMBOL:TickBar:Period" (e.g., "CME:ESZ4:TickBar:100")
    tick_bar_subscriptions: DashMap<String, ()>,
    /// Tracks order-book delta subscriptions.
    /// Key: "EXCHANGE:SYMBOL" (e.g., "CME:ESZ4")
    book_subscriptions: DashMap<String, BookSubscription>,
    /// Tracks additional ticker-plant update surfaces requested per instrument.
    /// Key: "EXCHANGE:SYMBOL" (e.g., "CME:ESZ4")
    extra_subscriptions: DashMap<String, ExtraMarketDataSubscription>,
}

impl RithmicDataClient {
    /// Creates a new data client backed by the given gateway.
    ///
    /// The gateway should already be connected before subscribing to data.
    pub fn new(gateway: Arc<tokio::sync::RwLock<RithmicGateway>>) -> Self {
        Self {
            gateway,
            subscriptions: DashMap::new(),
            bar_subscriptions: DashMap::new(),
            tick_bar_subscriptions: DashMap::new(),
            book_subscriptions: DashMap::new(),
            extra_subscriptions: DashMap::new(),
        }
    }

    /// Returns the current connection state from the gateway.
    pub fn connection_state(&self) -> ConnectionState {
        self.gateway
            .try_read()
            .map_or(ConnectionState::Disconnected, |gateway| {
                gateway.connection_state()
            })
    }

    /// Returns true if the gateway is connected.
    pub fn is_connected(&self) -> bool {
        self.gateway
            .try_read()
            .is_ok_and(|gateway| gateway.is_connected())
    }

    /// Returns a reference to the underlying gateway.
    pub fn gateway(&self) -> &Arc<tokio::sync::RwLock<RithmicGateway>> {
        &self.gateway
    }

    /// Subscribes to quotes (best bid/offer) for an instrument.
    ///
    /// After subscribing, `MarketDataEvent::Quote` events will be emitted
    /// on the gateway's market data receiver.
    ///
    /// # Note
    /// The current gateway market-data helper requests both `BBO` and `LAST_TRADE`.
    /// Calling `subscribe_quotes` will also enable receiving trades.
    pub async fn subscribe_quotes(&self, symbol: &str, exchange: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");

        // Check if we already have an active subscription for this instrument.
        // Returns true if we already have ANY subscription (quotes or trades),
        // meaning we don't need to send another subscribe request to Rithmic.
        let already_subscribed = {
            let entry = self.subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let sub = e.get_mut();

                    if sub.quotes {
                        return Ok(()); // Already subscribed to quotes
                    }
                    sub.quotes = true;
                    // If we already have trades subscription, we're already subscribed
                    // to this instrument at the Rithmic level
                    sub.trades
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(InstrumentSubscription {
                        quotes: true,
                        trades: false,
                    });
                    false // No existing subscription
                }
            }
        };

        // Only send subscription request if we don't already have one for this instrument

        if !already_subscribed {
            self.gateway
                .read()
                .await
                .subscribe_market_data(symbol, exchange)
                .await?;
        }

        Ok(())
    }

    /// Subscribes to trades (last trade) for an instrument.
    ///
    /// After subscribing, `MarketDataEvent::Trade` events will be emitted
    /// on the gateway's market data receiver.
    ///
    /// # Note
    /// The current gateway market-data helper requests both `BBO` and `LAST_TRADE`.
    /// Calling `subscribe_trades` will also enable receiving quotes.
    pub async fn subscribe_trades(&self, symbol: &str, exchange: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");

        // Check if we already have an active subscription for this instrument.
        // Returns true if we already have ANY subscription (quotes or trades).
        let already_subscribed = {
            let entry = self.subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let sub = e.get_mut();

                    if sub.trades {
                        return Ok(()); // Already subscribed to trades
                    }
                    sub.trades = true;
                    // If we already have quotes subscription, we're already subscribed
                    sub.quotes
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(InstrumentSubscription {
                        quotes: false,
                        trades: true,
                    });
                    false // No existing subscription
                }
            }
        };

        // Only send subscription request if we don't already have one for this instrument

        if !already_subscribed {
            self.gateway
                .read()
                .await
                .subscribe_market_data(symbol, exchange)
                .await?;
        }

        Ok(())
    }

    /// Subscribes to both quotes and trades for an instrument.
    ///
    /// This is equivalent to calling both `subscribe_quotes` and `subscribe_trades`.
    pub async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");

        // Check if we already have any subscription for this instrument.
        let already_subscribed = {
            let entry = self.subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let sub = e.get_mut();
                    let was_subscribed = sub.quotes || sub.trades;
                    sub.quotes = true;
                    sub.trades = true;
                    was_subscribed
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(InstrumentSubscription {
                        quotes: true,
                        trades: true,
                    });
                    false
                }
            }
        };

        if !already_subscribed {
            self.gateway
                .read()
                .await
                .subscribe_market_data(symbol, exchange)
                .await?;
        }

        Ok(())
    }

    /// Subscribes to live time bars for an instrument through the history plant.
    pub async fn subscribe_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
    ) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = bar_subscription_key(symbol, exchange, bar_type, bar_period);

        if self.bar_subscriptions.contains_key(&key) {
            return Ok(());
        }

        self.gateway
            .read()
            .await
            .subscribe_time_bars(symbol, exchange, bar_type, bar_period)
            .await?;
        self.bar_subscriptions.insert(key, ());

        Ok(())
    }

    /// Unsubscribes from quotes for an instrument (local tracking only).
    ///
    /// This method only updates local subscription tracking. It does **not**
    /// send an unsubscribe request to the venue. The venue will continue
    /// sending data until the connection is closed.
    ///
    /// Use [`unsubscribe_market_data_async`] if you need to stop data from
    /// the venue (e.g., to reduce bandwidth or message volume).
    ///
    /// [`unsubscribe_market_data_async`]: Self::unsubscribe_market_data_async
    pub fn unsubscribe_quotes(&self, symbol: &str, exchange: &str) {
        let key = format!("{exchange}:{symbol}");

        if let Some(mut sub) = self.subscriptions.get_mut(&key) {
            sub.quotes = false;

            if !sub.quotes && !sub.trades {
                drop(sub);
                self.subscriptions.remove(&key);
            }
        }
    }

    /// Unsubscribes from trades for an instrument (local tracking only).
    ///
    /// This method only updates local subscription tracking. It does **not**
    /// send an unsubscribe request to the venue. The venue will continue
    /// sending data until the connection is closed.
    ///
    /// Use [`unsubscribe_market_data_async`] if you need to stop data from
    /// the venue (e.g., to reduce bandwidth or message volume).
    ///
    /// [`unsubscribe_market_data_async`]: Self::unsubscribe_market_data_async
    pub fn unsubscribe_trades(&self, symbol: &str, exchange: &str) {
        let key = format!("{exchange}:{symbol}");

        if let Some(mut sub) = self.subscriptions.get_mut(&key) {
            sub.trades = false;

            if !sub.quotes && !sub.trades {
                drop(sub);
                self.subscriptions.remove(&key);
            }
        }
    }

    /// Unsubscribes from all market data (local tracking only).
    ///
    /// Clears local subscription tracking. Does **not** send unsubscribe
    /// requests to the venue.
    pub fn unsubscribe_all(&self) {
        self.subscriptions.clear();
        self.bar_subscriptions.clear();
        self.tick_bar_subscriptions.clear();
        self.book_subscriptions.clear();
        self.extra_subscriptions.clear();
    }

    /// Unsubscribes from all active subscriptions and notifies the venue.
    ///
    /// Iterates every active market-data and bar subscription, sends the
    /// corresponding unsubscribe request to the Rithmic ticker/history plant,
    /// then clears local tracking. Use this on disconnect to prevent the venue
    /// from continuing to push data on reconnect.
    pub async fn unsubscribe_all_async(&self) {
        // Market data (quotes + trades): key = "exchange:symbol"
        let market_keys: Vec<String> = self.subscriptions.iter().map(|r| r.key().clone()).collect();
        self.subscriptions.clear();

        for key in &market_keys {
            if let Some((exchange, symbol)) = key.split_once(':')
                && let Err(e) = self
                    .gateway
                    .read()
                    .await
                    .unsubscribe_market_data(symbol, exchange)
                    .await
            {
                log::warn!("Unsubscribe_all_async: market data {key}: {e}");
            }
        }

        // Bar subscriptions: key = "exchange:symbol:BarType:period"
        let bar_keys: Vec<String> = self
            .bar_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();
        self.bar_subscriptions.clear();

        for key in &bar_keys {
            let parts: Vec<&str> = key.splitn(4, ':').collect();

            if parts.len() == 4 {
                let (exchange, symbol, bar_type_str, period_str) =
                    (parts[0], parts[1], parts[2], parts[3]);
                let bar_type = match bar_type_str {
                    "SecondBar" => Some(TimeBarType::SecondBar),
                    "MinuteBar" => Some(TimeBarType::MinuteBar),
                    "DailyBar" => Some(TimeBarType::DailyBar),
                    "WeeklyBar" => Some(TimeBarType::WeeklyBar),
                    other => {
                        log::warn!(
                            "Unsubscribe_all_async: unknown bar type '{other}' in key '{key}'"
                        );
                        None
                    }
                };

                if let (Some(bar_type), Ok(period)) = (bar_type, period_str.parse::<i32>())
                    && let Err(e) = self
                        .gateway
                        .read()
                        .await
                        .unsubscribe_time_bars(symbol, exchange, bar_type, period)
                        .await
                {
                    log::warn!("Unsubscribe_all_async: bar {key}: {e}");
                }
            }
        }

        let tick_bar_keys: Vec<String> = self
            .tick_bar_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();
        self.tick_bar_subscriptions.clear();

        for key in &tick_bar_keys {
            let parts: Vec<&str> = key.splitn(4, ':').collect();

            if parts.len() != 4 {
                continue;
            }
            let (exchange, symbol, _, period_str) = (parts[0], parts[1], parts[2], parts[3]);
            let Ok(period) = period_str.parse::<u32>() else {
                continue;
            };

            let history_handle = {
                let gateway = self.gateway.read().await;
                gateway.history_handle().cloned()
            };
            let Some(handle) = history_handle else {
                log::warn!("Unsubscribe_all_async: history handle unavailable for {key}");
                continue;
            };

            if let Err(e) = handle
                .subscribe_tick_bar_updates(
                    symbol,
                    exchange,
                    request_tick_bar_update::BarType::TickBar,
                    request_tick_bar_update::BarSubType::Regular,
                    &period.to_string(),
                    request_tick_bar_update::Request::Unsubscribe,
                )
                .await
            {
                log::warn!("Unsubscribe_all_async: tick bar {key}: {e}");
            }
        }

        let book_keys: Vec<String> = self
            .book_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();
        self.book_subscriptions.clear();

        for key in &book_keys {
            if let Some((exchange, symbol)) = key.split_once(':')
                && let Err(e) = self
                    .gateway
                    .read()
                    .await
                    .unsubscribe_order_book(symbol, exchange)
                    .await
            {
                log::warn!("Unsubscribe_all_async: book {key}: {e}");
            }
        }

        let extra_entries: Vec<(String, Vec<ExtraMarketDataKind>)> = self
            .extra_subscriptions
            .iter()
            .map(|entry| (entry.key().clone(), extra_market_data_kinds(entry.value())))
            .collect();
        self.extra_subscriptions.clear();

        for (key, kinds) in &extra_entries {
            if kinds.is_empty() {
                continue;
            }

            if let Some((exchange, symbol)) = key.split_once(':') {
                for kind in kinds {
                    if let Err(e) = self
                        .apply_extra_market_data_subscription(symbol, exchange, *kind, false)
                        .await
                    {
                        log::warn!("Unsubscribe_all_async: extra market data {key}: {e}");
                    }
                }
            }
        }
    }

    /// Re-issues all active subscriptions to the venue.
    ///
    /// Called after a reconnect: local subscription maps still hold the active
    /// subscriptions but the venue's state is fresh. This bypasses the normal
    /// dedup logic and calls the gateway directly for every tracked instrument
    /// and bar subscription so data resumes without strategies needing to know
    /// a reconnect occurred.
    pub async fn resubscribe_all(&self) {
        // Market data: key = "exchange:symbol"
        let market_keys: Vec<String> = self.subscriptions.iter().map(|r| r.key().clone()).collect();

        for key in &market_keys {
            if let Some((exchange, symbol)) = key.split_once(':')
                && let Err(e) = self
                    .gateway
                    .read()
                    .await
                    .subscribe_market_data(symbol, exchange)
                    .await
            {
                log::warn!("Resubscribe_all: market data {key}: {e}");
            }
        }

        // Bar subscriptions: key = "exchange:symbol:BarType:period"
        let bar_keys: Vec<String> = self
            .bar_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();

        for key in &bar_keys {
            let parts: Vec<&str> = key.splitn(4, ':').collect();

            if parts.len() == 4 {
                let (exchange, symbol, bar_type_str, period_str) =
                    (parts[0], parts[1], parts[2], parts[3]);
                let bar_type = match bar_type_str {
                    "SecondBar" => Some(TimeBarType::SecondBar),
                    "MinuteBar" => Some(TimeBarType::MinuteBar),
                    "DailyBar" => Some(TimeBarType::DailyBar),
                    "WeeklyBar" => Some(TimeBarType::WeeklyBar),
                    other => {
                        log::warn!("Resubscribe_all: unknown bar type '{other}' in key '{key}'");
                        None
                    }
                };

                if let (Some(bar_type), Ok(period)) = (bar_type, period_str.parse::<i32>())
                    && let Err(e) = self
                        .gateway
                        .read()
                        .await
                        .subscribe_time_bars(symbol, exchange, bar_type, period)
                        .await
                {
                    log::warn!("Resubscribe_all: bar {key}: {e}");
                }
            }
        }

        let tick_bar_keys: Vec<String> = self
            .tick_bar_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();

        for key in &tick_bar_keys {
            let parts: Vec<&str> = key.splitn(4, ':').collect();

            if parts.len() != 4 {
                continue;
            }
            let (exchange, symbol, _, period_str) = (parts[0], parts[1], parts[2], parts[3]);
            let Ok(period) = period_str.parse::<u32>() else {
                continue;
            };

            let history_handle = {
                let gateway = self.gateway.read().await;
                gateway.history_handle().cloned()
            };
            let Some(handle) = history_handle else {
                log::warn!("Resubscribe_all: history handle unavailable for {key}");
                continue;
            };

            if let Err(e) = handle
                .subscribe_tick_bar_updates(
                    symbol,
                    exchange,
                    request_tick_bar_update::BarType::TickBar,
                    request_tick_bar_update::BarSubType::Regular,
                    &period.to_string(),
                    request_tick_bar_update::Request::Subscribe,
                )
                .await
            {
                log::warn!("Resubscribe_all: tick bar {key}: {e}");
            }
        }

        let book_keys: Vec<String> = self
            .book_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();

        for key in &book_keys {
            if let Some((exchange, symbol)) = key.split_once(':') {
                let gateway = self.gateway.read().await;

                if let Err(e) = gateway
                    .subscribe_order_book_bootstrapped(symbol, exchange)
                    .await
                {
                    log::warn!("Resubscribe_all: book {key}: {e}");
                }
            }
        }

        let extra_entries: Vec<(String, Vec<ExtraMarketDataKind>)> = self
            .extra_subscriptions
            .iter()
            .map(|entry| (entry.key().clone(), extra_market_data_kinds(entry.value())))
            .collect();

        for (key, kinds) in &extra_entries {
            if kinds.is_empty() {
                continue;
            }

            if let Some((exchange, symbol)) = key.split_once(':') {
                for kind in kinds {
                    if let Err(e) = self
                        .apply_extra_market_data_subscription(symbol, exchange, *kind, true)
                        .await
                    {
                        log::warn!("Resubscribe_all: extra market data {key}: {e}");
                    }
                }
            }
        }

        log::info!(
            "Resubscribe_all: re-issued {} market-data, {} time-bar, {} tick-bar, {} book, and {} extra market-data subscriptions",
            market_keys.len(),
            bar_keys.len(),
            tick_bar_keys.len(),
            book_keys.len(),
            extra_entries.len()
        );
    }

    /// Unsubscribes from market data and notifies the venue.
    ///
    /// Unlike the sync `unsubscribe_*` methods, this sends an actual
    /// unsubscribe request to the Rithmic ticker plant. Use this when
    /// you need to stop receiving data from the venue.
    pub async fn unsubscribe_market_data_async(&self, symbol: &str, exchange: &str) -> Result<()> {
        let key = format!("{exchange}:{symbol}");
        self.subscriptions.remove(&key);
        self.gateway
            .read()
            .await
            .unsubscribe_market_data(symbol, exchange)
            .await
    }

    /// Unsubscribes from live time bars and notifies the venue.
    pub async fn unsubscribe_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
    ) -> Result<()> {
        let key = bar_subscription_key(symbol, exchange, bar_type, bar_period);
        self.bar_subscriptions.remove(&key);
        self.gateway
            .read()
            .await
            .unsubscribe_time_bars(symbol, exchange, bar_type, bar_period)
            .await
    }

    /// Subscribes to live tick bars for an instrument through the history plant.
    pub async fn subscribe_tick_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_period: u32,
    ) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}:TickBar:{bar_period}");

        if self.tick_bar_subscriptions.contains_key(&key) {
            return Ok(());
        }

        let history_handle = {
            let gateway = self.gateway.read().await;
            gateway.history_handle().cloned()
        }
        .ok_or_else(|| RithmicError::Connection("History plant not connected".to_string()))?;

        history_handle
            .subscribe_tick_bar_updates(
                symbol,
                exchange,
                request_tick_bar_update::BarType::TickBar,
                request_tick_bar_update::BarSubType::Regular,
                &bar_period.to_string(),
                request_tick_bar_update::Request::Subscribe,
            )
            .await
            .map_err(|e| RithmicError::Api(format!("Tick bar subscribe failed: {e}")))?;

        self.tick_bar_subscriptions.insert(key, ());
        Ok(())
    }

    /// Unsubscribes from live tick bars and notifies the venue.
    pub async fn unsubscribe_tick_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_period: u32,
    ) -> Result<()> {
        let key = format!("{exchange}:{symbol}:TickBar:{bar_period}");
        self.tick_bar_subscriptions.remove(&key);

        let history_handle = {
            let gateway = self.gateway.read().await;
            gateway.history_handle().cloned()
        }
        .ok_or_else(|| RithmicError::Connection("History plant not connected".to_string()))?;

        history_handle
            .subscribe_tick_bar_updates(
                symbol,
                exchange,
                request_tick_bar_update::BarType::TickBar,
                request_tick_bar_update::BarSubType::Regular,
                &bar_period.to_string(),
                request_tick_bar_update::Request::Unsubscribe,
            )
            .await
            .map_err(|e| RithmicError::Api(format!("Tick bar unsubscribe failed: {e}")))?;

        Ok(())
    }

    /// Subscribes to order-book deltas for an instrument.
    pub async fn subscribe_book_deltas(&self, symbol: &str, exchange: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");
        let already_subscribed = {
            let entry = self.book_subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let sub = e.get_mut();

                    if sub.deltas {
                        return Ok(());
                    }
                    sub.deltas = true;
                    sub.depth10
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(BookSubscription {
                        deltas: true,
                        depth10: false,
                    });
                    false
                }
            }
        };

        if !already_subscribed {
            let gateway = self.gateway.read().await;
            gateway
                .subscribe_order_book_bootstrapped(symbol, exchange)
                .await?;
        }
        Ok(())
    }

    /// Subscribes to aggregated top-10 order book depth for an instrument.
    pub async fn subscribe_book_depth10(&self, symbol: &str, exchange: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");
        let already_subscribed = {
            let entry = self.book_subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let sub = e.get_mut();

                    if sub.depth10 {
                        return Ok(());
                    }
                    sub.depth10 = true;
                    sub.deltas
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(BookSubscription {
                        deltas: false,
                        depth10: true,
                    });
                    false
                }
            }
        };

        if !already_subscribed {
            let gateway = self.gateway.read().await;
            gateway
                .subscribe_order_book_bootstrapped(symbol, exchange)
                .await?;
        }
        Ok(())
    }

    /// Unsubscribes from order-book deltas and notifies the venue.
    pub async fn unsubscribe_book_deltas(&self, symbol: &str, exchange: &str) -> Result<()> {
        let key = format!("{exchange}:{symbol}");
        let should_unsubscribe = if let Some(mut sub) = self.book_subscriptions.get_mut(&key) {
            sub.deltas = false;
            let remaining = sub.deltas || sub.depth10;
            drop(sub);

            if !remaining {
                self.book_subscriptions.remove(&key);
            }
            !remaining
        } else {
            false
        };

        if should_unsubscribe {
            self.gateway
                .read()
                .await
                .unsubscribe_order_book(symbol, exchange)
                .await?;
        }
        Ok(())
    }

    /// Unsubscribes from top-10 order book depth and notifies the venue when unused.
    pub async fn unsubscribe_book_depth10(&self, symbol: &str, exchange: &str) -> Result<()> {
        let key = format!("{exchange}:{symbol}");
        let should_unsubscribe = if let Some(mut sub) = self.book_subscriptions.get_mut(&key) {
            sub.depth10 = false;
            let remaining = sub.deltas || sub.depth10;
            drop(sub);

            if !remaining {
                self.book_subscriptions.remove(&key);
            }
            !remaining
        } else {
            false
        };

        if should_unsubscribe {
            self.gateway
                .read()
                .await
                .unsubscribe_order_book(symbol, exchange)
                .await?;
        }
        Ok(())
    }

    async fn update_extra_market_data_subscription(
        &self,
        symbol: &str,
        exchange: &str,
        kind: ExtraMarketDataKind,
        enabled: bool,
    ) -> Result<()> {
        if !self.is_connected() {
            return Err(RithmicError::Connection("Not connected".to_string()));
        }

        let key = format!("{exchange}:{symbol}");
        {
            let entry = self.extra_subscriptions.entry(key.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(mut e) => {
                    let subscription = e.get_mut();
                    let was_enabled = get_extra_market_data_flag(subscription, kind);

                    if was_enabled == enabled {
                        return Ok(());
                    }

                    set_extra_market_data_flag(subscription, kind, enabled);

                    if extra_market_data_kinds(subscription).is_empty() {
                        drop(e);
                        self.extra_subscriptions.remove(&key);
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    if !enabled {
                        return Ok(());
                    }

                    let mut subscription = ExtraMarketDataSubscription::default();
                    set_extra_market_data_flag(&mut subscription, kind, true);
                    e.insert(subscription);
                }
            }
        }

        self.apply_extra_market_data_subscription(symbol, exchange, kind, enabled)
            .await
    }

    async fn apply_extra_market_data_subscription(
        &self,
        symbol: &str,
        exchange: &str,
        kind: ExtraMarketDataKind,
        enabled: bool,
    ) -> Result<()> {
        let gateway = self.gateway.read().await;

        match (kind, enabled) {
            (ExtraMarketDataKind::InstrumentStatus, true) => {
                gateway.subscribe_instrument_status(symbol, exchange).await
            }
            (ExtraMarketDataKind::InstrumentStatus, false) => {
                gateway
                    .unsubscribe_instrument_status(symbol, exchange)
                    .await
            }
            (ExtraMarketDataKind::TradeStatistics, true) => {
                gateway.subscribe_trade_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::TradeStatistics, false) => {
                gateway.unsubscribe_trade_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::QuoteStatistics, true) => {
                gateway.subscribe_quote_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::QuoteStatistics, false) => {
                gateway.unsubscribe_quote_statistics(symbol, exchange).await
            }
            (ExtraMarketDataKind::IndicatorPrices, true) => {
                gateway.subscribe_indicator_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::IndicatorPrices, false) => {
                gateway.unsubscribe_indicator_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::OpenInterest, true) => {
                gateway.subscribe_open_interest(symbol, exchange).await
            }
            (ExtraMarketDataKind::OpenInterest, false) => {
                gateway.unsubscribe_open_interest(symbol, exchange).await
            }
            (ExtraMarketDataKind::EndOfDayPrices, true) => {
                gateway.subscribe_end_of_day_prices(symbol, exchange).await
            }
            (ExtraMarketDataKind::EndOfDayPrices, false) => {
                gateway
                    .unsubscribe_end_of_day_prices(symbol, exchange)
                    .await
            }
            (ExtraMarketDataKind::OrderPriceLimits, true) => {
                gateway.subscribe_order_price_limits(symbol, exchange).await
            }
            (ExtraMarketDataKind::OrderPriceLimits, false) => {
                gateway
                    .unsubscribe_order_price_limits(symbol, exchange)
                    .await
            }
            (ExtraMarketDataKind::SymbolMarginRate, true) => {
                gateway.subscribe_symbol_margin_rate(symbol, exchange).await
            }
            (ExtraMarketDataKind::SymbolMarginRate, false) => {
                gateway
                    .unsubscribe_symbol_margin_rate(symbol, exchange)
                    .await
            }
        }
    }

    /// Subscribes to venue instrument-status updates (`MarketMode`) for an instrument.
    pub async fn subscribe_instrument_status(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.update_extra_market_data_subscription(
            symbol,
            exchange,
            ExtraMarketDataKind::InstrumentStatus,
            true,
        )
        .await
    }

    /// Unsubscribes from venue instrument-status updates for an instrument.
    pub async fn unsubscribe_instrument_status(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.update_extra_market_data_subscription(
            symbol,
            exchange,
            ExtraMarketDataKind::InstrumentStatus,
            false,
        )
        .await
    }

    /// Subscribes to a live custom market-data surface for an instrument.
    pub async fn subscribe_custom_data(
        &self,
        type_name: &str,
        symbol: &str,
        exchange: &str,
    ) -> Result<()> {
        let kind = ExtraMarketDataKind::from_custom_type(type_name).ok_or_else(|| {
            RithmicError::Api(format!(
                "Unsupported live custom market-data type: {type_name}"
            ))
        })?;

        self.update_extra_market_data_subscription(symbol, exchange, kind, true)
            .await
    }

    /// Unsubscribes from a live custom market-data surface for an instrument.
    pub async fn unsubscribe_custom_data(
        &self,
        type_name: &str,
        symbol: &str,
        exchange: &str,
    ) -> Result<()> {
        let kind = ExtraMarketDataKind::from_custom_type(type_name).ok_or_else(|| {
            RithmicError::Api(format!(
                "Unsupported live custom market-data type: {type_name}"
            ))
        })?;

        self.update_extra_market_data_subscription(symbol, exchange, kind, false)
            .await
    }

    /// Returns the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns all active subscription keys in "EXCHANGE:SYMBOL" format.
    pub fn subscriptions(&self) -> Vec<String> {
        self.subscriptions.iter().map(|r| r.key().clone()).collect()
    }

    /// Returns the number of active live bar subscriptions.
    pub fn bar_subscription_count(&self) -> usize {
        self.bar_subscriptions.len() + self.tick_bar_subscriptions.len()
    }

    /// Returns all active live bar subscription keys.
    pub fn bar_subscriptions(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .bar_subscriptions
            .iter()
            .map(|r| r.key().clone())
            .collect();
        keys.extend(self.tick_bar_subscriptions.iter().map(|r| r.key().clone()));
        keys
    }

    /// Returns the number of active order-book subscriptions.
    pub fn book_subscription_count(&self) -> usize {
        self.book_subscriptions.len()
    }

    /// Returns true if subscribed to order-book deltas for the given instrument.
    pub fn is_subscribed_book_deltas(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.book_subscriptions
            .get(&key)
            .is_some_and(|sub| sub.deltas)
    }

    /// Returns true if subscribed to order-book depth10 for the given instrument.
    pub fn is_subscribed_book_depth10(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.book_subscriptions
            .get(&key)
            .is_some_and(|sub| sub.depth10)
    }

    /// Returns true if subscribed to quotes for the given instrument.
    pub fn is_subscribed_quotes(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.subscriptions.get(&key).is_some_and(|s| s.quotes)
    }

    /// Returns true if subscribed to trades for the given instrument.
    pub fn is_subscribed_trades(&self, symbol: &str, exchange: &str) -> bool {
        let key = format!("{exchange}:{symbol}");
        self.subscriptions.get(&key).is_some_and(|s| s.trades)
    }

    /// Returns true if subscribed to live bars for the given symbol/bar shape.
    pub fn is_subscribed_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
    ) -> bool {
        let key = bar_subscription_key(symbol, exchange, bar_type, bar_period);
        self.bar_subscriptions.contains_key(&key)
            || self
                .tick_bar_subscriptions
                .contains_key(&format!("{exchange}:{symbol}:TickBar:{bar_period}"))
    }
}

impl Debug for RithmicDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicDataClient))
            .field("connection_state", &self.connection_state())
            .field("subscriptions", &self.subscription_count())
            .field("bar_subscriptions", &self.bar_subscription_count())
            .finish()
    }
}

fn bar_subscription_key(
    symbol: &str,
    exchange: &str,
    bar_type: TimeBarType,
    bar_period: i32,
) -> String {
    let bar_type = match bar_type {
        TimeBarType::SecondBar => "SecondBar",
        TimeBarType::MinuteBar => "MinuteBar",
        TimeBarType::DailyBar => "DailyBar",
        TimeBarType::WeeklyBar => "WeeklyBar",
    };

    format!("{exchange}:{symbol}:{bar_type}:{bar_period}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::RithmicEnv, gateway::GatewayConfig};

    fn create_test_gateway() -> Arc<tokio::sync::RwLock<RithmicGateway>> {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "fcm",
            "ib",
            "account",
        );
        Arc::new(tokio::sync::RwLock::new(RithmicGateway::new(config)))
    }

    #[rstest::rstest]
    fn test_data_client_creation() {
        let gateway = create_test_gateway();
        let client = RithmicDataClient::new(gateway);
        assert_eq!(client.connection_state(), ConnectionState::Disconnected);
        assert_eq!(client.subscription_count(), 0);
        assert_eq!(client.bar_subscription_count(), 0);
    }

    #[rstest::rstest]
    fn test_subscription_tracking() {
        let gateway = create_test_gateway();
        let client = RithmicDataClient::new(gateway);

        // Manually insert subscription for testing (without actually subscribing)
        client.subscriptions.insert(
            "CME:ESZ4".to_string(),
            InstrumentSubscription {
                quotes: true,
                trades: false,
            },
        );

        assert!(client.is_subscribed_quotes("ESZ4", "CME"));
        assert!(!client.is_subscribed_trades("ESZ4", "CME"));
        assert_eq!(client.subscription_count(), 1);

        // Update to include trades

        if let Some(mut sub) = client.subscriptions.get_mut("CME:ESZ4") {
            sub.trades = true;
        }
        assert!(client.is_subscribed_trades("ESZ4", "CME"));

        // Unsubscribe from quotes
        client.unsubscribe_quotes("ESZ4", "CME");
        assert!(!client.is_subscribed_quotes("ESZ4", "CME"));
        assert!(client.is_subscribed_trades("ESZ4", "CME"));
        assert_eq!(client.subscription_count(), 1);

        // Unsubscribe from trades - should remove entry
        client.unsubscribe_trades("ESZ4", "CME");
        assert_eq!(client.subscription_count(), 0);
    }

    #[rstest::rstest]
    fn test_unsubscribe_all() {
        let gateway = create_test_gateway();
        let client = RithmicDataClient::new(gateway);

        // Add some subscriptions
        client.subscriptions.insert(
            "CME:ESZ4".to_string(),
            InstrumentSubscription {
                quotes: true,
                trades: true,
            },
        );
        client.subscriptions.insert(
            "CME:NQZ4".to_string(),
            InstrumentSubscription {
                quotes: true,
                trades: false,
            },
        );
        client
            .bar_subscriptions
            .insert("CME:ESZ4:MinuteBar:1".to_string(), ());
        client.extra_subscriptions.insert(
            "CME:ESZ4".to_string(),
            ExtraMarketDataSubscription {
                instrument_status: true,
                open_interest: true,
                ..Default::default()
            },
        );

        assert_eq!(client.subscription_count(), 2);
        assert_eq!(client.bar_subscription_count(), 1);

        client.unsubscribe_all();
        assert_eq!(client.subscription_count(), 0);
        assert_eq!(client.bar_subscription_count(), 0);
        assert!(client.extra_subscriptions.is_empty());
    }

    #[rstest::rstest]
    fn test_subscriptions_list() {
        let gateway = create_test_gateway();
        let client = RithmicDataClient::new(gateway);

        client.subscriptions.insert(
            "CME:ESZ4".to_string(),
            InstrumentSubscription {
                quotes: true,
                trades: true,
            },
        );
        client.subscriptions.insert(
            "NYMEX:CLZ4".to_string(),
            InstrumentSubscription {
                quotes: true,
                trades: false,
            },
        );

        let subs = client.subscriptions();
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&"CME:ESZ4".to_string()));
        assert!(subs.contains(&"NYMEX:CLZ4".to_string()));
    }

    #[rstest::rstest]
    fn test_bar_subscriptions_list() {
        let gateway = create_test_gateway();
        let client = RithmicDataClient::new(gateway);

        client
            .bar_subscriptions
            .insert("CME:ESZ4:MinuteBar:1".to_string(), ());

        assert!(client.is_subscribed_bars("ESZ4", "CME", TimeBarType::MinuteBar, 1));
        assert_eq!(client.bar_subscription_count(), 1);
        assert_eq!(
            client.bar_subscriptions(),
            vec!["CME:ESZ4:MinuteBar:1".to_string()],
        );
    }

    #[rstest::rstest]
    fn test_extra_market_data_kinds_mapping() {
        let subscription = ExtraMarketDataSubscription {
            instrument_status: true,
            trade_statistics: true,
            quote_statistics: true,
            indicator_prices: true,
            open_interest: true,
            end_of_day_prices: true,
            order_price_limits: true,
            symbol_margin_rate: true,
        };

        let kinds = extra_market_data_kinds(&subscription);

        assert!(kinds.contains(&ExtraMarketDataKind::InstrumentStatus));
        assert!(kinds.contains(&ExtraMarketDataKind::TradeStatistics));
        assert!(kinds.contains(&ExtraMarketDataKind::QuoteStatistics));
        assert!(kinds.contains(&ExtraMarketDataKind::IndicatorPrices));
        assert!(kinds.contains(&ExtraMarketDataKind::OpenInterest));
        assert!(kinds.contains(&ExtraMarketDataKind::EndOfDayPrices));
        assert!(kinds.contains(&ExtraMarketDataKind::OrderPriceLimits));
        assert!(kinds.contains(&ExtraMarketDataKind::SymbolMarginRate));
    }
}
