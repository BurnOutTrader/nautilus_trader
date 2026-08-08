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

//! Live data client implementing the `DataClient` trait (v2 PyO3 path).
//!
//! This is the entry point for use with `LiveNode` and `DataClientFactory`.
//! It wraps the existing `RithmicDataClient` domain object, wires up the
//! event loop, and pushes `DataEvent`s into the NautilusTrader data engine
//! via `get_data_event_sender()`.

#![allow(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, BookResponse, CustomDataResponse, DataResponse, InstrumentResponse,
            InstrumentsResponse, RequestBars, RequestBookSnapshot, RequestCustomData,
            RequestInstrument, RequestInstruments, RequestTrades, SubscribeBars,
            SubscribeBookDeltas, SubscribeBookDepth10, SubscribeCustomData,
            SubscribeInstrumentStatus, SubscribeQuotes, SubscribeTrades, TradesResponse,
            UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10, UnsubscribeCustomData,
            UnsubscribeInstrumentStatus, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, CustomData, Data, DataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick, ensure_custom_data_json_registered,
    },
    enums::{AggressorSide, BarAggregation, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
    types::{Price, Quantity},
};
use parking_lot::RwLock as ParkingRwLock;
use rithmic_rs::{
    plants::ticker_plant::RithmicTickerPlantHandle,
    rti::{messages::RithmicMessage, request_time_bar_replay::BarType as TimeBarType},
};
use tokio::{task::JoinHandle, time::timeout};

use crate::{
    common::{
        consts::exchanges::KNOWN_EXCHANGES,
        parse::{rithmic_depth_order_id, tick_size_to_precision},
    },
    config::RithmicDataClientConfig,
    data::{
        END_OF_DAY_PRICES_TYPE_NAME, ExtraMarketDataKind, INDICATOR_PRICES_TYPE_NAME,
        MarketDataEvent, OPEN_INTEREST_TYPE_NAME, ORDER_PRICE_LIMITS_TYPE_NAME,
        QUOTE_STATISTICS_TYPE_NAME, RithmicCustomData, RithmicDataClient, RithmicEndOfDayPrices,
        RithmicIndicatorPrices, RithmicOpenInterest, RithmicOrderPriceLimits,
        RithmicQuoteStatistics, RithmicSymbolMarginRate, RithmicTradeStatistics,
        RithmicVolumeAtPrice, SYMBOL_MARGIN_RATE_TYPE_NAME, TRADE_STATISTICS_TYPE_NAME,
        VOLUME_AT_PRICE_TYPE_NAME,
        volume_profile::{RithmicMinuteVolumeProfileBar, VOLUME_PROFILE_TYPE_NAME},
    },
    gateway::{GatewayConfig, RithmicGateway},
    instruments::{
        discovery::enabled_exchange_names,
        front_month::load_supported_front_months_with_handle,
        parse::{
            apply_auxiliary_reference_data, candidate_exchanges_for_symbol, response_to_instrument,
            supported_product_for_symbol,
        },
    },
    shared_gateway::SharedGatewayLease,
};

const RITHMIC_VENUE: &str = "RITHMIC";
const REQUEST_INSTRUMENTS_EXCHANGE_CONCURRENCY: usize = 4;
const CONTRACT_EXCHANGE_RESOLUTION_TIMEOUT_SECS: u64 = 5;

fn rithmic_timestamp_to_unix_nanos(ssboe: Option<i32>, usecs: Option<i32>) -> UnixNanos {
    let secs = ssboe.unwrap_or_default().max(0) as u64;
    let micros = usecs.unwrap_or_default().max(0) as u64;
    UnixNanos::from(secs.saturating_mul(1_000_000_000) + micros.saturating_mul(1_000))
}

fn normalize_history_request_end(start_sec: i32, end_sec: i32, now_sec: i32) -> i32 {
    if start_sec > 0 && end_sec <= 0 {
        now_sec.max(start_sec)
    } else {
        end_sec
    }
}

fn live_trade_id(tick: &crate::data::TradeTick) -> TradeId {
    let trade_id = tick.trade_id.trim();
    if !trade_id.is_empty() {
        return TradeId::new(trade_id);
    }

    let symbol = tick.symbol.chars().take(8).collect::<String>();
    TradeId::from(format!("live:{}:{symbol}", tick.ts_event).as_str())
}

fn resolved_symbol_key(symbol: &str) -> String {
    symbol.to_ascii_uppercase()
}

fn cache_resolved_exchange(
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    symbol: &str,
    exchange: &str,
) {
    resolved_exchanges
        .write()
        .insert(resolved_symbol_key(symbol), exchange.to_string());
}

fn get_cached_exchange(
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    symbol: &str,
) -> Option<String> {
    resolved_exchanges
        .read()
        .get(&resolved_symbol_key(symbol))
        .cloned()
}

pub(crate) async fn resolve_contract_exchange(
    gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    instrument_id: &InstrumentId,
) -> anyhow::Result<(String, String)> {
    let (symbol, explicit_exchange) = parse_rithmic_instrument(instrument_id);

    if !explicit_exchange.is_empty() {
        cache_resolved_exchange(resolved_exchanges, &symbol, &explicit_exchange);
        return Ok((symbol, explicit_exchange));
    }

    if let Some(exchange) = get_cached_exchange(resolved_exchanges, &symbol) {
        return Ok((symbol, exchange));
    }

    let product = supported_product_for_symbol(&symbol)
        .ok_or_else(|| anyhow::anyhow!("Unsupported Rithmic contract symbol {instrument_id}"))?;

    if product.code.eq_ignore_ascii_case(&symbol) {
        anyhow::bail!(
            "Resolve the current front month before using root instrument {instrument_id}"
        );
    }

    let exchange_candidates = candidate_exchanges_for_symbol(&symbol, None);
    if exchange_candidates.len() == 1 {
        let exchange = exchange_candidates[0].to_string();
        cache_resolved_exchange(resolved_exchanges, &symbol, &exchange);
        return Ok((symbol, exchange));
    }

    let ticker = {
        let gateway = gateway.read().await;
        gateway.ticker_handle().cloned().ok_or_else(|| {
            anyhow::anyhow!("Ticker handle not available — is ticker plant connected?")
        })?
    };

    for candidate_exchange in exchange_candidates {
        let response = timeout(
            std::time::Duration::from_secs(CONTRACT_EXCHANGE_RESOLUTION_TIMEOUT_SECS),
            ticker.get_reference_data(&symbol, candidate_exchange),
        )
        .await;

        let Ok(Ok(response)) = response else {
            continue;
        };

        if response.error.is_some() {
            continue;
        }

        if matches!(response.message, RithmicMessage::ResponseReferenceData(_)) {
            let exchange = candidate_exchange.to_string();
            cache_resolved_exchange(resolved_exchanges, &symbol, &exchange);
            return Ok((symbol, exchange));
        }
    }

    anyhow::bail!("Unable to resolve exchange for Rithmic contract {instrument_id}")
}

fn request_instruments_tradeable_only(params: &Option<Params>) -> bool {
    params
        .as_ref()
        .and_then(|value| value.get_bool("tradeable_only"))
        .unwrap_or(false)
}

fn request_instruments_front_month_only(params: &Option<Params>) -> bool {
    params
        .as_ref()
        .and_then(|value| value.get_bool("front_month_only"))
        .unwrap_or(true)
}

fn request_instruments_param_exchanges(params: &Option<Params>) -> Option<Vec<String>> {
    let params = params.as_ref()?;

    if let Some(exchange) = params.get_str("exchange") {
        let normalized = exchange.trim();
        if !normalized.is_empty() {
            return Some(vec![normalized.to_string()]);
        }
    }

    let requested = params
        .get("exchanges")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .filter(|value| {
                    KNOWN_EXCHANGES
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(value))
                })
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    (!requested.is_empty()).then_some(requested)
}

async fn request_instruments_exchange_scope(
    ticker: &RithmicTickerPlantHandle,
    username: &str,
    params: &Option<Params>,
) -> Vec<String> {
    if let Some(requested) = request_instruments_param_exchanges(params) {
        return requested;
    }

    match ticker.list_exchanges(username).await {
        Ok(responses) => {
            let enabled = enabled_exchange_names(&responses);
            let supported = KNOWN_EXCHANGES
                .iter()
                .filter(|exchange| {
                    enabled
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(exchange))
                })
                .map(|exchange| exchange.to_string())
                .collect::<Vec<_>>();
            if supported.is_empty() {
                KNOWN_EXCHANGES
                    .iter()
                    .map(|exchange| exchange.to_string())
                    .collect()
            } else {
                supported
            }
        }
        Err(e) => {
            log::warn!(
                "Exchange permissions request failed during request_instruments, falling back to known exchanges: {e}"
            );
            KNOWN_EXCHANGES
                .iter()
                .map(|exchange| exchange.to_string())
                .collect()
        }
    }
}

fn time_bar_marker_to_unix_nanos(marker: Option<i32>) -> Option<UnixNanos> {
    marker.and_then(|value| {
        if value > 0 {
            Some(UnixNanos::from(value as u64 * 1_000_000_000))
        } else {
            None
        }
    })
}

fn historical_time_bar_to_bar(
    message: &RithmicMessage,
    bar_type: BarType,
    price_prec: u8,
    size_prec: u8,
    ts_init: UnixNanos,
) -> Option<Bar> {
    match message {
        RithmicMessage::ResponseTimeBarReplay(bar) => {
            let ts_event = time_bar_marker_to_unix_nanos(bar.marker)?;

            Some(Bar::new(
                bar_type,
                Price::new(bar.open_price.unwrap_or(0.0), price_prec),
                Price::new(bar.high_price.unwrap_or(0.0), price_prec),
                Price::new(bar.low_price.unwrap_or(0.0), price_prec),
                Price::new(bar.close_price.unwrap_or(0.0), price_prec),
                Quantity::new(bar.volume.unwrap_or(0) as f64, size_prec),
                ts_event,
                ts_init,
            ))
        }
        RithmicMessage::TimeBar(bar) => {
            let ts_event = time_bar_marker_to_unix_nanos(bar.marker)?;

            Some(Bar::new(
                bar_type,
                Price::new(bar.open_price.unwrap_or(0.0), price_prec),
                Price::new(bar.high_price.unwrap_or(0.0), price_prec),
                Price::new(bar.low_price.unwrap_or(0.0), price_prec),
                Price::new(bar.close_price.unwrap_or(0.0), price_prec),
                Quantity::new(bar.volume.unwrap_or(0) as f64, size_prec),
                ts_event,
                ts_init,
            ))
        }
        _ => None,
    }
}

fn normalize_time_history_bars(mut bars: Vec<Bar>) -> Vec<Bar> {
    bars.sort_by_key(|bar| bar.ts_event);
    bars.dedup_by_key(|bar| bar.ts_event);
    bars
}

fn historical_tick_bar_to_trade(
    message: &RithmicMessage,
    instrument_id: InstrumentId,
    price_prec: u8,
    size_prec: u8,
    sequence: usize,
) -> Option<TradeTick> {
    let RithmicMessage::ResponseTickBarReplay(bar) = message else {
        return None;
    };

    let ts_event = rithmic_timestamp_to_unix_nanos(
        bar.data_bar_ssboe.last().copied(),
        bar.data_bar_usecs.last().copied(),
    );
    if ts_event.as_u64() == 0 {
        return None;
    }
    let price = bar
        .close_price
        .or(bar.open_price)
        .or(bar.high_price)
        .or(bar.low_price)
        .unwrap_or(0.0);

    Some(TradeTick::new(
        instrument_id,
        Price::new(price, price_prec),
        Quantity::new(bar.volume.unwrap_or(0) as f64, size_prec),
        AggressorSide::NoAggressor,
        TradeId::from(format!("replay:{}:{sequence}", ts_event.as_u64()).as_str()),
        ts_event,
        ts_event,
    ))
}

/// Converts a Rithmic `InstrumentId` string back to Nautilus format.
///
/// `"ESH5"`, `"CME"` → `InstrumentId { symbol: "ESH5", venue: "RITHMIC" }`
fn make_instrument_id(symbol: &str, _exchange: &str) -> InstrumentId {
    crate::common::converters::rithmic_instrument_id(symbol)
}

/// Parses a Nautilus `InstrumentId` into `(symbol, exchange)` for Rithmic.
///
/// - `ESH5.RITHMIC`     → `("ESH5", "")`
/// - `ESH5.RITHMIC` → `("ESH5", "CME")`
pub(crate) fn parse_rithmic_instrument(instrument_id: &InstrumentId) -> (String, String) {
    let symbol_str = instrument_id.symbol.as_str();
    let mut parts = symbol_str.splitn(2, '.');
    let symbol = parts.next().unwrap_or(symbol_str).to_string();
    let exchange = parts.next().unwrap_or("").to_string();
    (symbol, exchange)
}

/// Maps a NautilusTrader `BarAggregation` to a Rithmic `TimeBarType`.
fn bar_aggregation_to_time_bar_type(aggregation: BarAggregation) -> anyhow::Result<TimeBarType> {
    match aggregation {
        BarAggregation::Second => Ok(TimeBarType::SecondBar),
        BarAggregation::Minute => Ok(TimeBarType::MinuteBar),
        BarAggregation::Day => Ok(TimeBarType::DailyBar),
        BarAggregation::Week => Ok(TimeBarType::WeeklyBar),
        other => Err(anyhow::anyhow!(
            "Unsupported bar aggregation for Rithmic: {other:?}"
        )),
    }
}

/// Constructs a bar subscription key matching `RithmicDataClient`'s internal format.
///
/// Format: `"EXCHANGE:SYMBOL:BarType:Period"` (e.g., `"CME:ESZ4:MinuteBar:1"`)
fn rithmic_bar_key(exchange: &str, symbol: &str, bar_type: TimeBarType, period: u32) -> String {
    let type_str = match bar_type {
        TimeBarType::SecondBar => "SecondBar",
        TimeBarType::MinuteBar => "MinuteBar",
        TimeBarType::DailyBar => "DailyBar",
        TimeBarType::WeeklyBar => "WeeklyBar",
    };
    format!("{exchange}:{symbol}:{type_str}:{period}")
}

/// Constructs a bar subscription key for tick bars.
///
/// Format: `"EXCHANGE:SYMBOL:TickBar:N"` (e.g., `"CME:ESZ4:TickBar:100"`)
fn rithmic_tick_bar_key(exchange: &str, symbol: &str, period: u32) -> String {
    format!("{exchange}:{symbol}:TickBar:{period}")
}

fn convert_trade_aggressor(value: &str) -> AggressorSide {
    match value {
        "BUY" => AggressorSide::Buyer,
        "SELL" => AggressorSide::Seller,
        _ => AggressorSide::NoAggressor,
    }
}

trait BookSubscriptionView {
    fn wants_book_deltas(&self, symbol: &str, exchange: &str) -> bool;
    fn wants_book_depth10(&self, symbol: &str, exchange: &str) -> bool;
}

impl BookSubscriptionView for RithmicDataClient {
    fn wants_book_deltas(&self, symbol: &str, exchange: &str) -> bool {
        self.is_subscribed_book_deltas(symbol, exchange)
    }

    fn wants_book_depth10(&self, symbol: &str, exchange: &str) -> bool {
        self.is_subscribed_book_depth10(symbol, exchange)
    }
}

impl BookSubscriptionView for Arc<RithmicDataClient> {
    fn wants_book_deltas(&self, symbol: &str, exchange: &str) -> bool {
        self.as_ref().wants_book_deltas(symbol, exchange)
    }

    fn wants_book_depth10(&self, symbol: &str, exchange: &str) -> bool {
        self.as_ref().wants_book_depth10(symbol, exchange)
    }
}

fn convert_book_delta_event(d: &crate::data::BookDelta) -> Option<OrderBookDelta> {
    let instrument_id = make_instrument_id(&d.symbol, &d.exchange);
    let action = match d.action.as_str() {
        "ADD" => BookAction::Add,
        "UPDATE" => BookAction::Update,
        "REMOVE" => BookAction::Delete,
        "CLEAR" => {
            return Some(OrderBookDelta::clear(
                instrument_id,
                d.sequence,
                UnixNanos::from(d.ts_event),
                UnixNanos::from(d.ts_init),
            ));
        }
        _ => return None,
    };
    let side = match d.side.as_str() {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return None,
    };
    let price_prec = if d.price_precision > 0 {
        d.price_precision
    } else {
        2
    };
    let size_prec = if d.size_precision > 0 {
        d.size_precision
    } else {
        0
    };
    let order = BookOrder::new(
        side,
        Price::new(d.price, price_prec),
        Quantity::new(d.size, size_prec),
        d.order_id,
    );
    OrderBookDelta::new_checked(
        instrument_id,
        action,
        order,
        d.flags,
        d.sequence,
        UnixNanos::from(d.ts_event),
        UnixNanos::from(d.ts_init),
    )
    .map_err(|e| {
        log::warn!(
            "Dropping invalid BookDelta for {}.{}.RITHMIC action={action:?}: {e}",
            d.symbol,
            d.exchange
        );
    })
    .ok()
}

pub(crate) fn depth10_from_order_book(
    book: &OrderBook,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderBookDepth10 {
    let mut bids = std::array::from_fn(|_| {
        BookOrder::new(OrderSide::Buy, Price::zero(0), Quantity::zero(0), 0)
    });
    let mut asks = std::array::from_fn(|_| {
        BookOrder::new(OrderSide::Sell, Price::zero(0), Quantity::zero(0), 0)
    });
    let mut bid_counts = [0_u32; 10];
    let mut ask_counts = [0_u32; 10];

    for (i, level) in book.bids(Some(10)).enumerate() {
        bids[i] = BookOrder::new(
            OrderSide::Buy,
            level.price.value,
            Quantity::new(level.size(), 0),
            0,
        );
        bid_counts[i] = level.len() as u32;
    }

    for (i, level) in book.asks(Some(10)).enumerate() {
        asks[i] = BookOrder::new(
            OrderSide::Sell,
            level.price.value,
            Quantity::new(level.size(), 0),
            0,
        );
        ask_counts[i] = level.len() as u32;
    }

    OrderBookDepth10::new(
        book.instrument_id,
        bids,
        asks,
        bid_counts,
        ask_counts,
        RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8,
        book.sequence,
        ts_event,
        ts_init,
    )
}

pub(crate) fn order_book_from_snapshot(
    instrument_id: InstrumentId,
    responses: &[rithmic_rs::api::RithmicResponse],
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> OrderBook {
    let snapshot_rows: Vec<&rithmic_rs::rti::ResponseDepthByOrderSnapshot> = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseDepthByOrderSnapshot(snapshot) => Some(snapshot),
            _ => None,
        })
        .collect();

    order_book_from_snapshot_rows(
        instrument_id,
        &snapshot_rows,
        price_precision,
        size_precision,
        ts_init,
    )
}

fn order_book_from_snapshot_rows(
    instrument_id: InstrumentId,
    snapshot_rows: &[&rithmic_rs::rti::ResponseDepthByOrderSnapshot],
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> OrderBook {
    use rithmic_rs::rti::response_depth_by_order_snapshot::TransactionType;

    let mut book = OrderBook::new(instrument_id, BookType::L3_MBO);
    let mut saw_snapshot = false;

    for snapshot in snapshot_rows {
        let sequence = snapshot.sequence_number.unwrap_or(0);
        let ts_event = ts_init;

        if !saw_snapshot {
            book.clear(sequence, ts_event);
            saw_snapshot = true;
        }

        let side = match snapshot
            .depth_side
            .and_then(|value| TransactionType::try_from(value).ok())
        {
            Some(TransactionType::Buy) => OrderSide::Buy,
            Some(TransactionType::Sell) => OrderSide::Sell,
            None => continue,
        };
        let price = snapshot.depth_price.unwrap_or_default();

        for (i, depth_order_priority) in snapshot.depth_order_priority.iter().enumerate() {
            let size = snapshot.depth_size.get(i).copied().unwrap_or_default();

            if size <= 0 {
                continue;
            }
            let order_id = rithmic_depth_order_id(
                snapshot.exchange_order_id.get(i).map(String::as_str),
                *depth_order_priority,
            );
            book.add(
                BookOrder::new(
                    side,
                    Price::new(price, price_precision),
                    Quantity::new(size as f64, size_precision),
                    order_id,
                ),
                RecordFlag::F_SNAPSHOT as u8,
                sequence,
                ts_event,
            );
        }
    }

    book
}

fn custom_data_to_nautilus(custom: RithmicCustomData) -> Data {
    match custom {
        RithmicCustomData::TradeStatistics(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(TRADE_STATISTICS_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::QuoteStatistics(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(QUOTE_STATISTICS_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::IndicatorPrices(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(INDICATOR_PRICES_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::OpenInterest(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(OPEN_INTEREST_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::EndOfDayPrices(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(END_OF_DAY_PRICES_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::OrderPriceLimits(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(ORDER_PRICE_LIMITS_TYPE_NAME, None, Some(identifier)),
            ))
        }
        RithmicCustomData::SymbolMarginRate(value) => {
            let identifier = value.instrument_id.to_string();
            Data::Custom(CustomData::new(
                Arc::new(value),
                DataType::new(SYMBOL_MARGIN_RATE_TYPE_NAME, None, Some(identifier)),
            ))
        }
    }
}

fn process_market_data_event(
    event: MarketDataEvent,
    bar_type_map: &ParkingRwLock<AHashMap<String, BarType>>,
    order_books: &mut HashMap<InstrumentId, OrderBook>,
    order_book_delta_batches: &mut HashMap<InstrumentId, Vec<OrderBookDelta>>,
    book_subscriptions: &impl BookSubscriptionView,
) -> Vec<DataEvent> {
    let now = get_atomic_clock_realtime().get_time_ns();

    match event {
        MarketDataEvent::Quote(q) => {
            let instrument_id = make_instrument_id(&q.symbol, &q.exchange);
            let price_prec = if q.price_precision > 0 {
                q.price_precision
            } else {
                2
            };
            let size_prec = if q.size_precision > 0 {
                q.size_precision
            } else {
                0
            };
            vec![DataEvent::Data(Data::Quote(QuoteTick {
                instrument_id,
                bid_price: Price::new(q.bid_price, price_prec),
                ask_price: Price::new(q.ask_price, price_prec),
                bid_size: Quantity::new(q.bid_size, size_prec),
                ask_size: Quantity::new(q.ask_size, size_prec),
                ts_event: q.ts_event.into(),
                ts_init: q.ts_init.into(),
            }))]
        }
        MarketDataEvent::Trade(t) => {
            let instrument_id = make_instrument_id(&t.symbol, &t.exchange);
            let price_prec = if t.price_precision > 0 {
                t.price_precision
            } else {
                2
            };
            let size_prec = if t.size_precision > 0 {
                t.size_precision
            } else {
                0
            };
            vec![DataEvent::Data(Data::Trade(TradeTick {
                instrument_id,
                price: Price::new(t.price, price_prec),
                size: Quantity::new(t.size, size_prec),
                aggressor_side: convert_trade_aggressor(&t.aggressor_side),
                trade_id: live_trade_id(&t),
                ts_event: t.ts_event.into(),
                ts_init: t.ts_init.into(),
            }))]
        }
        MarketDataEvent::Bar(b) => {
            let key = format!(
                "{}:{}:{}:{}",
                b.exchange,
                b.symbol,
                b.bar_type.as_str(),
                b.bar_period
            );
            let bar_type = match bar_type_map.read().get(&key).copied() {
                Some(bt) => bt,
                None => {
                    log::warn!("Received bar with no matching subscription: {key}");
                    return Vec::new();
                }
            };
            let price_prec = if b.price_precision > 0 {
                b.price_precision
            } else {
                2
            };
            let size_prec = if b.size_precision > 0 {
                b.size_precision
            } else {
                0
            };
            vec![DataEvent::Data(Data::Bar(Bar::new(
                bar_type,
                Price::new(b.open_price, price_prec),
                Price::new(b.high_price, price_prec),
                Price::new(b.low_price, price_prec),
                Price::new(b.close_price, price_prec),
                Quantity::new(b.volume, size_prec),
                b.ts_event.into(),
                now,
            )))]
        }
        MarketDataEvent::BookDelta(d) => {
            let mut output = Vec::new();
            let wants_deltas = book_subscriptions.wants_book_deltas(&d.symbol, &d.exchange);
            let wants_depth10 = book_subscriptions.wants_book_depth10(&d.symbol, &d.exchange);
            let Some(delta) = convert_book_delta_event(&d) else {
                return output;
            };
            let instrument_id = make_instrument_id(&d.symbol, &d.exchange);

            if wants_deltas {
                let batch = order_book_delta_batches.entry(instrument_id).or_default();
                batch.push(delta);

                if delta.flags & RecordFlag::F_LAST as u8 != 0 {
                    let deltas = OrderBookDeltas::new(instrument_id, std::mem::take(batch));
                    output.push(DataEvent::Data(Data::Deltas(Box::new(deltas))));
                    order_book_delta_batches.remove(&instrument_id);
                }
            }

            if wants_depth10 {
                let book = order_books
                    .entry(instrument_id)
                    .or_insert_with(|| OrderBook::new(instrument_id, BookType::L3_MBO));

                if let Err(e) = book.apply_delta(&delta) {
                    log::warn!(
                        "Failed applying Rithmic delta to local book for {instrument_id}: {e}"
                    );
                    return output;
                }

                if delta.flags & RecordFlag::F_LAST as u8 != 0 {
                    output.push(DataEvent::Data(Data::from(depth10_from_order_book(
                        book,
                        delta.ts_event,
                        delta.ts_init,
                    ))));
                }
            }

            output
        }
        MarketDataEvent::Depth10(depth) => vec![DataEvent::Data(Data::from(*depth))],
        MarketDataEvent::InstrumentStatus(status) => vec![DataEvent::InstrumentStatus(status)],
        MarketDataEvent::Custom(custom) => vec![DataEvent::Data(custom_data_to_nautilus(custom))],
        MarketDataEvent::ConnectionState(_)
        | MarketDataEvent::Reconnected
        | MarketDataEvent::Authenticated
        | MarketDataEvent::Error(_) => Vec::new(),
    }
}

/// Live data client for Rithmic implementing the NautilusTrader `DataClient` trait.
///
/// This is the v2 PyO3 entry point — used with `LiveNode` and `RithmicDataClientFactory`.
/// The client:
/// 1. Creates and connects a `RithmicGateway` on `connect()`
/// 2. Spawns an event loop that converts `MarketDataEvent` → `DataEvent`
/// 3. Pushes events into the engine via `get_data_event_sender()`
pub struct RithmicLiveDataClient {
    client_id: ClientId,
    config: RithmicDataClientConfig,
    /// Maps bar subscription key → original `BarType` for bar event conversion.
    bar_type_map: Arc<ParkingRwLock<AHashMap<String, BarType>>>,
    inner: Option<Arc<RithmicDataClient>>,
    gateway: Option<SharedGatewayLease>,
    data_sender: Option<tokio::sync::mpsc::UnboundedSender<DataEvent>>,
    event_task: Option<JoinHandle<()>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    is_connected: Arc<AtomicBool>,
    pending_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    resolved_exchanges: Arc<ParkingRwLock<AHashMap<String, String>>>,
}

impl Debug for RithmicLiveDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicLiveDataClient))
            .field("client_id", &self.client_id)
            .field("is_connected", &self.is_connected.load(Ordering::Relaxed))
            .finish()
    }
}

impl RithmicLiveDataClient {
    /// Creates a new [`RithmicLiveDataClient`].
    ///
    /// The global data event sender is resolved lazily on `connect()`, so this
    /// constructor succeeds even before the `LiveNode` runner has started (e.g. in tests).
    #[must_use]
    pub fn new(client_id: ClientId, config: RithmicDataClientConfig) -> Self {
        Self {
            client_id,
            config,
            bar_type_map: Arc::new(ParkingRwLock::new(AHashMap::new())),
            inner: None,
            gateway: None,
            data_sender: None,
            event_task: None,
            shutdown_tx: None,
            is_connected: Arc::new(AtomicBool::new(false)),
            pending_tasks: Arc::new(Mutex::new(Vec::new())),
            resolved_exchanges: Arc::new(ParkingRwLock::new(AHashMap::new())),
        }
    }

    fn require_inner(&self) -> anyhow::Result<Arc<RithmicDataClient>> {
        self.inner
            .clone()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveDataClient is not connected"))
    }

    fn require_gateway(&self) -> anyhow::Result<Arc<tokio::sync::RwLock<RithmicGateway>>> {
        self.gateway
            .as_ref()
            .map(SharedGatewayLease::gateway)
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveDataClient is not connected"))
    }

    /// Spawns an async task on the Rithmic runtime. Errors are logged; the
    /// response arrives via the market-data event channel.
    fn spawn_ws<F>(&self, fut: F, context: &'static str)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let handle = get_runtime().spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{context}: {e:?}");
            }
        });
        let mut tasks = self
            .pending_tasks
            .lock()
            .expect("pending_tasks mutex poisoned");
        tasks.retain(|h| !h.is_finished());
        tasks.push(handle);
    }

    /// Builds a `GatewayConfig` for the data client (ticker plant only).
    fn gateway_config(&self) -> GatewayConfig {
        let c = &self.config;
        let mut cfg = GatewayConfig::new(
            c.environment,
            c.username.as_str(),
            c.password.as_str(),
            c.system_name.as_str(),
            c.fcm_id.as_deref().unwrap_or(""),
            c.ib_id.as_deref().unwrap_or(""),
            "", // data client has no account_id
        );
        cfg.app_name = c.app_name.clone();
        cfg.app_version = c.app_version.clone();
        cfg.server = c.server.clone();
        cfg.alt_server = c.alt_server.clone();
        cfg.enable_ticker = true;
        cfg.enable_order = false;
        cfg.enable_pnl = false;
        cfg.enable_history = c.enable_history;
        cfg
    }
}

#[async_trait(?Send)]
impl DataClient for RithmicLiveDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(Venue::from(RITHMIC_VENUE))
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Started: client_id={}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping: client_id={}", self.client_id);

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(task) = self.event_task.take() {
            task.abort();
        }
        self.is_connected.store(false, Ordering::Relaxed);
        self.resolved_exchanges.write().clear();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting: client_id={}", self.client_id);
        self.inner = None;
        self.gateway = None;
        self.event_task = None;
        self.shutdown_tx = None;
        self.is_connected.store(false, Ordering::Relaxed);
        self.resolved_exchanges.write().clear();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing: client_id={}", self.client_id);
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        // Register volume-profile type for JSON round-trip (idempotent).
        ensure_custom_data_json_registered::<RithmicMinuteVolumeProfileBar>().ok();
        ensure_custom_data_json_registered::<RithmicTradeStatistics>().ok();
        ensure_custom_data_json_registered::<RithmicQuoteStatistics>().ok();
        ensure_custom_data_json_registered::<RithmicIndicatorPrices>().ok();
        ensure_custom_data_json_registered::<RithmicOpenInterest>().ok();
        ensure_custom_data_json_registered::<RithmicEndOfDayPrices>().ok();
        ensure_custom_data_json_registered::<RithmicOrderPriceLimits>().ok();
        ensure_custom_data_json_registered::<RithmicSymbolMarginRate>().ok();
        ensure_custom_data_json_registered::<RithmicVolumeAtPrice>().ok();

        let gateway_config = self.gateway_config();
        let gateway = SharedGatewayLease::acquire(gateway_config.clone());
        let shared_gateway = gateway.gateway();
        let rx = {
            let guard = shared_gateway.read().await;
            guard.subscribe_market_data_events()
        };
        gateway
            .connect(&gateway_config)
            .await
            .map_err(|e| anyhow::anyhow!("Gateway connect failed: {e}"))?;
        let inner = Arc::new(RithmicDataClient::new(Arc::clone(&shared_gateway)));

        // Resolve the data event sender (lazily, so new() works without a runner).
        let data_sender = get_data_event_sender();
        self.data_sender = Some(data_sender.clone());

        // Spawn the event loop: MarketDataEvent → DataEvent → data engine.
        // The maps are Send + Sync (Arc<Mutex<_>>), safe to move into the task.
        // Note: precision_map not needed here - gateway populates precision in MarketDataEvent.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let is_connected = Arc::clone(&self.is_connected);
        let bar_type_map = Arc::clone(&self.bar_type_map);
        let inner_for_reconnect = Arc::clone(&inner);
        let gateway_for_reconnect = Arc::clone(&shared_gateway);

        let task = get_runtime().spawn(async move {
            let mut rx = rx;
            let mut shutdown_rx = shutdown_rx;
            let mut order_books: HashMap<InstrumentId, OrderBook> = HashMap::new();
            let mut order_book_delta_batches: HashMap<InstrumentId, Vec<OrderBookDelta>> =
                HashMap::new();

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        log::debug!("Rithmic data event loop shutdown");
                        break;
                    }
                    event = rx.recv() => {
                        match event {
                            Ok(MarketDataEvent::ConnectionState(crate::common::enums::ConnectionState::Reconnecting)) => {
                                log::warn!("Rithmic data gateway is reconnecting");
                                is_connected.store(false, Ordering::Relaxed);
                                order_books.clear();
                                order_book_delta_batches.clear();
                                let reconnect_result = {
                                    let mut gateway = gateway_for_reconnect.write().await;
                                    gateway.reconnect_if_needed().await
                                };

                                if let Err(e) = reconnect_result {
                                    log::error!("Rithmic data reconnect failed: {e}");
                                    break;
                                }
                            }
                            Ok(MarketDataEvent::Reconnected) => {
                                log::info!("Rithmic reconnected — re-issuing all subscriptions");
                                is_connected.store(true, Ordering::Relaxed);
                                inner_for_reconnect.resubscribe_all().await;
                            }
                            Ok(evt) => {
                                for data_event in process_market_data_event(
                                    evt,
                                    &bar_type_map,
                                    &mut order_books,
                                    &mut order_book_delta_batches,
                                    &inner_for_reconnect,
                                ) {
                                    if data_sender.send(data_event).is_err() {
                                        log::warn!("Data engine receiver dropped, stopping event loop");
                                        return;
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                log::warn!("Rithmic market data subscriber lagged by {skipped} events");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::info!("Rithmic market data channel closed");
                                is_connected.store(false, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
            }
        });

        self.inner = Some(inner);
        self.gateway = Some(gateway);
        self.event_task = Some(task);
        self.shutdown_tx = Some(shutdown_tx);
        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        // Send venue-side unsubscribes before tearing down the connection so the
        // venue stops pushing data (prevents stale data on reconnect).

        if let Some(inner) = &self.inner {
            inner.unsubscribe_all_async().await;
        }
        self.inner = None;

        if let Some(mut gateway) = self.gateway.take() {
            gateway.release().await;
        }
        self.bar_type_map.write().clear();
        self.stop()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_quotes(&symbol, &exchange)
                    .await
                    .map_err(Into::into)
            },
            "subscribe_quotes",
        );
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let Some(inner) = self.inner.clone() else {
            return Ok(());
        };
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .unsubscribe_market_data_async(&symbol, &exchange)
                    .await
                    .map_err(Into::into)
            },
            "unsubscribe_quotes",
        );
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_trades(&symbol, &exchange)
                    .await
                    .map_err(Into::into)
            },
            "subscribe_trades",
        );
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let Some(inner) = self.inner.clone() else {
            return Ok(());
        };
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .unsubscribe_market_data_async(&symbol, &exchange)
                    .await
                    .map_err(Into::into)
            },
            "unsubscribe_trades",
        );
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_book_deltas(&symbol, &exchange)
                    .await
                    .map_err(|e| anyhow::anyhow!("subscribe_book_deltas failed: {e}"))
            },
            "subscribe_book_deltas",
        );
        Ok(())
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_book_depth10(&symbol, &exchange)
                    .await
                    .map_err(|e| anyhow::anyhow!("subscribe_book_depth10 failed: {e}"))
            },
            "subscribe_book_depth10",
        );
        Ok(())
    }

    fn subscribe_instrument_status(
        &mut self,
        cmd: SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_instrument_status(&symbol, &exchange)
                    .await
                    .map_err(|e| anyhow::anyhow!("subscribe_instrument_status failed: {e}"))
            },
            "subscribe_instrument_status",
        );
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;

        if let Some(inner) = self.inner.clone() {
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    inner
                        .unsubscribe_book_deltas(&symbol, &exchange)
                        .await
                        .map_err(|e| anyhow::anyhow!("unsubscribe_book_deltas failed: {e}"))
                },
                "unsubscribe_book_deltas",
            );
        }
        Ok(())
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;

        if let Some(inner) = self.inner.clone() {
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    inner
                        .unsubscribe_book_depth10(&symbol, &exchange)
                        .await
                        .map_err(|e| anyhow::anyhow!("unsubscribe_book_depth10 failed: {e}"))
                },
                "unsubscribe_book_depth10",
            );
        }
        Ok(())
    }

    fn unsubscribe_instrument_status(
        &mut self,
        cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let instrument_id = cmd.instrument_id;

        if let Some(inner) = self.inner.clone() {
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    inner
                        .unsubscribe_instrument_status(&symbol, &exchange)
                        .await
                        .map_err(|e| anyhow::anyhow!("unsubscribe_instrument_status failed: {e}"))
                },
                "unsubscribe_instrument_status",
            );
        }
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        let (instrument_id, spec) = match &cmd.bar_type {
            BarType::Standard {
                instrument_id,
                spec,
                ..
            } => (*instrument_id, *spec),
            _ => anyhow::bail!(
                "Composite BarType is not supported for Rithmic: {}",
                cmd.bar_type
            ),
        };

        let period = spec.step.get() as u32;
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let bar_type_map = Arc::clone(&self.bar_type_map);
        let bar_type = cmd.bar_type;

        if spec.aggregation == BarAggregation::Tick {
            let inner = self.require_inner()?;
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    let key = rithmic_tick_bar_key(&exchange, &symbol, period);
                    bar_type_map.write().insert(key, bar_type);
                    inner
                        .subscribe_tick_bars(&symbol, &exchange, period)
                        .await
                        .map_err(Into::into)
                },
                "subscribe_tick_bars",
            );
            return Ok(());
        }

        let time_bar_type = bar_aggregation_to_time_bar_type(spec.aggregation)?;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let key = rithmic_bar_key(&exchange, &symbol, time_bar_type, period);
                bar_type_map.write().insert(key, bar_type);
                inner
                    .subscribe_bars(&symbol, &exchange, time_bar_type, period as i32)
                    .await
                    .map_err(Into::into)
            },
            "subscribe_bars",
        );
        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let (instrument_id, spec) = match &cmd.bar_type {
            BarType::Standard {
                instrument_id,
                spec,
                ..
            } => (*instrument_id, *spec),
            _ => anyhow::bail!(
                "Composite BarType is not supported for Rithmic: {}",
                cmd.bar_type
            ),
        };

        let period = spec.step.get() as u32;
        let Some(inner) = self.inner.clone() else {
            return Ok(());
        };
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let bar_type_map = Arc::clone(&self.bar_type_map);

        if spec.aggregation == BarAggregation::Tick {
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    let key = rithmic_tick_bar_key(&exchange, &symbol, period);
                    bar_type_map.write().remove(&key);
                    inner
                        .unsubscribe_tick_bars(&symbol, &exchange, period)
                        .await
                        .map_err(Into::into)
                },
                "unsubscribe_tick_bars",
            );
            return Ok(());
        }

        let time_bar_type = bar_aggregation_to_time_bar_type(spec.aggregation)?;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let key = rithmic_bar_key(&exchange, &symbol, time_bar_type, period);
                bar_type_map.write().remove(&key);
                inner
                    .unsubscribe_bars(&symbol, &exchange, time_bar_type, period as i32)
                    .await
                    .map_err(Into::into)
            },
            "unsubscribe_bars",
        );
        Ok(())
    }

    fn request_trades(&self, cmd: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let start_sec = cmd.start.map_or(0, |dt| dt.as_second() as i32);
        let now_sec = ((get_atomic_clock_realtime().get_time_ns().as_u64()) / 1_000_000_000)
            .min(i32::MAX as u64) as i32;
        let end_sec = normalize_history_request_end(
            start_sec,
            cmd.end.map_or(0, |dt| dt.as_second() as i32),
            now_sec,
        );

        log::warn!(
            "Historical Rithmic trades are synthesized from 1-tick bar replay; \
            aggressor side will always be NO_AGGRESSOR. Instrument: {instrument_id}"
        );

        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let data_sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not initialized"))?;
        let client_id = cmd.client_id.unwrap_or(self.client_id);
        let correlation_id = cmd.request_id;
        let cmd_start = cmd.start.map(|dt| dt.into());
        let cmd_end = cmd.end.map(|dt| dt.into());
        let limit = cmd.limit;
        let params = cmd.params;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let history_handle = {
                    let gateway = gateway.read().await;
                    gateway.history_handle().cloned()
                };

                let mut trades: Vec<TradeTick> = match history_handle {
                    None => {
                        log::error!(
                            "request_trades: history plant not connected — \
                            set enable_history=true in config. \
                            Sending empty TradesResponse"
                        );
                        vec![]
                    }
                    Some(handle) => match handle
                        .load_ticks(symbol.clone(), exchange.clone(), start_sec, end_sec)
                        .await
                    {
                        Err(e) => {
                            log::error!(
                                "request_trades failed for {instrument_id}: {e} — \
                                sending empty TradesResponse"
                            );
                            vec![]
                        }
                        Ok(responses) => {
                            let key = format!("{exchange}:{symbol}");
                            let (price_prec, size_prec) = {
                                let gateway = gateway.read().await;
                                gateway
                                    .instruments()
                                    .try_read()
                                    .ok()
                                    .and_then(|m| m.get(&key).cloned())
                                    .map_or((2, 0), |info| {
                                        let price_prec =
                                            info.tick_size.map_or(2, tick_size_to_precision);
                                        (price_prec, 0)
                                    })
                            };

                            responses
                                .iter()
                                .enumerate()
                                .filter_map(|(sequence, resp)| {
                                    historical_tick_bar_to_trade(
                                        &resp.message,
                                        instrument_id,
                                        price_prec,
                                        size_prec,
                                        sequence,
                                    )
                                })
                                .collect()
                        }
                    },
                };

                if let Some(limit) = limit {
                    let keep = limit.get();
                    if trades.len() > keep {
                        trades = trades.split_off(trades.len() - keep);
                    }
                }

                let now = get_atomic_clock_realtime().get_time_ns();
                let response = TradesResponse::new(
                    correlation_id,
                    client_id,
                    instrument_id,
                    trades,
                    cmd_start,
                    cmd_end,
                    now,
                    params,
                );

                data_sender
                    .send(DataEvent::Response(DataResponse::Trades(response)))
                    .map_err(|e| anyhow::anyhow!("Failed to send TradesResponse: {e}"))
            },
            "request_trades",
        );

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not available"))?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let params = request.params;
        let clock = get_atomic_clock_realtime();

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let ticker = {
                    let gateway = gateway.read().await;
                    gateway.ticker_handle().cloned().ok_or_else(|| {
                        anyhow::anyhow!("Ticker handle not available — is ticker plant connected?")
                    })?
                };

                let response = ticker
                    .get_reference_data(&symbol, &exchange)
                    .await
                    .map_err(|e| anyhow::anyhow!("get_reference_data failed: {e}"))?;

                if let Some(e) = &response.error {
                    anyhow::bail!("Reference data error for {symbol}: {e}");
                }

                let RithmicMessage::ResponseReferenceData(ref_data) = &response.message else {
                    anyhow::bail!("Unexpected response type for reference data");
                };

                let ts_init = clock.get_time_ns();
                let mut instrument = response_to_instrument(ref_data, ts_init)
                    .map_err(|e| anyhow::anyhow!("Failed to parse instrument: {e}"))?;

                if let Ok(aux_resp) = ticker
                    .get_auxilliary_reference_data(&symbol, &exchange)
                    .await
                    && let RithmicMessage::ResponseAuxilliaryReferenceData(aux) = &aux_resp.message
                {
                    apply_auxiliary_reference_data(&mut instrument, aux);
                }

                let resolved_id = instrument.id;
                let instrument_any = InstrumentAny::FuturesContract(instrument);
                let data_response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                    request_id,
                    client_id,
                    resolved_id,
                    instrument_any,
                    None,
                    None,
                    clock.get_time_ns(),
                    params,
                )));

                sender
                    .send(DataEvent::Response(data_response))
                    .map_err(|e| anyhow::anyhow!("Failed to send instrument response: {e}"))
            },
            "request_instrument",
        );
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let gateway = self.require_gateway()?;
        let sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not available"))?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let venue = request.venue.unwrap_or_else(|| Venue::from(RITHMIC_VENUE));
        let request_id = request.request_id;
        let params = request.params;
        let clock = get_atomic_clock_realtime();

        self.spawn_ws(
            async move {
                let (ticker, username) = {
                    let gateway = gateway.read().await;
                    let ticker = gateway.ticker_handle().cloned().ok_or_else(|| {
                        anyhow::anyhow!("Ticker handle not available — is ticker plant connected?")
                    })?;
                    let username = gateway.config().username.clone();
                    (ticker, username)
                };

                let tradeable_only = request_instruments_tradeable_only(&params);
                if !request_instruments_front_month_only(&params) {
                    log::debug!(
                        "Rithmic request_instruments no longer uses the full-chain discovery path; using supported front-month roots instead"
                    );
                }

                let exchanges = request_instruments_exchange_scope(&ticker, &username, &params).await;
                let ts_init = clock.get_time_ns();

                let instrument_results = stream::iter(exchanges)
                    .map(|exchange| {
                        let ticker = ticker.clone();
                        async move {
                            let result = load_supported_front_months_with_handle(
                                &ticker,
                                &exchange,
                                ts_init,
                                tradeable_only,
                            )
                            .await;
                            (exchange, result)
                        }
                    })
                    .buffer_unordered(REQUEST_INSTRUMENTS_EXCHANGE_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await;

                let mut instruments_by_id: std::collections::BTreeMap<InstrumentId, InstrumentAny> =
                    std::collections::BTreeMap::new();
                for (exchange_scope, result) in instrument_results {
                    match result {
                        Ok(instruments) => {
                            for instrument in instruments {
                                instruments_by_id.insert(instrument.id(), instrument);
                            }
                        }
                        Err(e) => {
                            log::debug!(
                                "Supported front-month load failed for exchange scope {exchange_scope} during request_instruments: {e}"
                            );
                        }
                    }
                }

                let instruments = instruments_by_id.into_values().collect::<Vec<_>>();

                let data_response = DataResponse::Instruments(InstrumentsResponse::new(
                    request_id,
                    client_id,
                    venue,
                    instruments,
                    None,
                    None,
                    clock.get_time_ns(),
                    params,
                ));

                sender
                    .send(DataEvent::Response(data_response))
                    .map_err(|e| anyhow::anyhow!("Failed to send instruments response: {e}"))
            },
            "request_instruments",
        );
        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not available"))?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let now = get_atomic_clock_realtime().get_time_ns();
                let responses = gateway
                    .read()
                    .await
                    .request_order_book_snapshot(&symbol, &exchange)
                    .await
                    .map_err(|e| anyhow::anyhow!("Order book snapshot request failed: {e}"))?;

                let (price_precision, size_precision) = {
                    let key = format!("{exchange}:{symbol}");
                    let gateway = gateway.read().await;
                    gateway
                        .instruments()
                        .try_read()
                        .ok()
                        .and_then(|map| map.get(&key).cloned())
                        .map_or((2, 0), |info| {
                            (info.tick_size.map_or(2, tick_size_to_precision), 0)
                        })
                };

                let book = order_book_from_snapshot(
                    instrument_id,
                    &responses,
                    price_precision,
                    size_precision,
                    now,
                );

                sender
                    .send(DataEvent::Response(DataResponse::Book(BookResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        book,
                        None,
                        None,
                        now,
                        params,
                    ))))
                    .map_err(|e| anyhow::anyhow!("Failed to send book snapshot response: {e}"))
            },
            "request_book_snapshot",
        );

        Ok(())
    }

    fn subscribe(&mut self, cmd: SubscribeCustomData) -> anyhow::Result<()> {
        if cmd.data_type.type_name() == VOLUME_PROFILE_TYPE_NAME {
            log::warn!(
                "RithmicMinuteVolumeProfileBar is historical-only — use request_data instead of \
                subscribe. To fetch bars call request_data with DataType identifier set to the \
                instrument_id (e.g. 'ESM5.RITHMIC')"
            );
        } else if cmd.data_type.type_name() == VOLUME_AT_PRICE_TYPE_NAME {
            log::warn!(
                "RithmicVolumeAtPrice is request-only — use request_data with DataType identifier \
                set to the instrument_id (e.g. 'ESM5.RITHMIC')"
            );
        } else if ExtraMarketDataKind::from_custom_type(cmd.data_type.type_name()).is_some() {
            let identifier = cmd.data_type.identifier().ok_or_else(|| {
                anyhow::anyhow!(
                    "subscribe({}): DataType must have an identifier equal to the instrument_id \
                    (e.g. 'ESM5.RITHMIC')",
                    cmd.data_type.type_name()
                )
            })?;
            let instrument_id = InstrumentId::from(identifier.to_string().as_str());
            let gateway = self.require_gateway()?;
            let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
            let type_name = cmd.data_type.type_name().to_string();
            let inner = self.require_inner()?;
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    inner
                        .subscribe_custom_data(&type_name, &symbol, &exchange)
                        .await
                        .map_err(|e| anyhow::anyhow!("subscribe({type_name}) failed: {e}"))
                },
                "subscribe_custom_data",
            );
        } else {
            log::warn!(
                "subscribe: unsupported custom DataType '{}' — \
                only live '{}'/'{}' market-data surfaces plus request-only '{}' and '{}' \
                are supported by this adapter",
                cmd.data_type.type_name(),
                TRADE_STATISTICS_TYPE_NAME,
                QUOTE_STATISTICS_TYPE_NAME,
                VOLUME_PROFILE_TYPE_NAME,
                VOLUME_AT_PRICE_TYPE_NAME,
            );
        }
        Ok(())
    }

    fn unsubscribe(&mut self, cmd: &UnsubscribeCustomData) -> anyhow::Result<()> {
        if ExtraMarketDataKind::from_custom_type(cmd.data_type.type_name()).is_some() {
            let identifier = cmd.data_type.identifier().ok_or_else(|| {
                anyhow::anyhow!(
                    "unsubscribe({}): DataType must have an identifier equal to the instrument_id \
                    (e.g. 'ESM5.RITHMIC')",
                    cmd.data_type.type_name()
                )
            })?;
            let instrument_id = InstrumentId::from(identifier.to_string().as_str());
            let gateway = self.require_gateway()?;
            let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
            let type_name = cmd.data_type.type_name().to_string();

            if let Some(inner) = self.inner.clone() {
                self.spawn_ws(
                    async move {
                        let (symbol, exchange) = resolve_contract_exchange(
                            &gateway,
                            &resolved_exchanges,
                            &instrument_id,
                        )
                        .await?;
                        inner
                            .unsubscribe_custom_data(&type_name, &symbol, &exchange)
                            .await
                            .map_err(|e| anyhow::anyhow!("unsubscribe({type_name}) failed: {e}"))
                    },
                    "unsubscribe_custom_data",
                );
            }
        }

        Ok(())
    }

    fn request_data(&self, request: RequestCustomData) -> anyhow::Result<()> {
        if request.data_type.type_name() == VOLUME_AT_PRICE_TYPE_NAME {
            let identifier = request
                .data_type
                .identifier()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "request_data(VolumeAtPrice): DataType must have an identifier equal to the \
                        instrument_id (e.g. 'ESM5.RITHMIC')"
                    )
                })?
                .to_string();

            let instrument_id = InstrumentId::from(identifier.as_str());
            let gateway = self.require_gateway()?;
            let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
            let data_sender = self
                .data_sender
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Data sender not initialized"))?;
            let client_id = request.client_id;
            let correlation_id = request.request_id;
            let data_type = request.data_type.clone();
            let params = request.params;

            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    let now = get_atomic_clock_realtime().get_time_ns();
                    let responses = gateway
                        .read()
                        .await
                        .ticker_handle()
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("Ticker handle not available"))?
                        .get_volume_at_price(&symbol, &exchange)
                        .await
                        .map_err(|e| anyhow::anyhow!("request_data(VolumeAtPrice) failed: {e}"))?;

                    let mut trade_price = Vec::new();
                    let mut volume_at_price = Vec::new();
                    let mut ts_event = now;

                    for response in responses {
                        let RithmicMessage::ResponseGetVolumeAtPrice(vap) = response.message else {
                            continue;
                        };
                        trade_price.extend(vap.trade_price);
                        volume_at_price.extend(vap.volume_at_price);
                        ts_event = rithmic_timestamp_to_unix_nanos(vap.ssboe, vap.usecs);
                    }

                    let payload = vec![RithmicVolumeAtPrice {
                        instrument_id,
                        trade_price,
                        volume_at_price,
                        ts_event,
                        ts_init: now,
                    }];

                    data_sender
                        .send(DataEvent::Response(DataResponse::Data(
                            CustomDataResponse::new(
                                correlation_id,
                                client_id,
                                Some(Venue::from(RITHMIC_VENUE)),
                                data_type,
                                payload,
                                None,
                                None,
                                now,
                                params,
                            ),
                        )))
                        .map_err(|e| anyhow::anyhow!("Failed to send VolumeAtPrice response: {e}"))
                },
                "request_volume_at_price",
            );

            return Ok(());
        }

        if request.data_type.type_name() != VOLUME_PROFILE_TYPE_NAME {
            log::warn!(
                "request_data: unsupported custom DataType '{}' — \
                only '{}' and '{}' are supported by this adapter",
                request.data_type.type_name(),
                VOLUME_PROFILE_TYPE_NAME,
                VOLUME_AT_PRICE_TYPE_NAME,
            );
            return Ok(());
        }

        let identifier = request
            .data_type
            .identifier()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "request_data(VolumeProfile): DataType must have an identifier equal to the \
                    instrument_id (e.g. 'ESM5.RITHMIC')"
                )
            })?
            .to_string();

        let instrument_id = InstrumentId::from(identifier.as_str());

        // Optional period in minutes — default 1.
        let period: i32 = request
            .data_type
            .metadata()
            .and_then(|m| m.get("period"))
            .and_then(|v| v.as_i64())
            .map_or(1, |v| v as i32);

        let start_sec = request.start.map_or(0, |dt| dt.as_second() as i32);
        let end_sec = request.end.map_or(0, |dt| dt.as_second() as i32);

        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let data_sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not initialized"))?;
        let client_id = request.client_id;
        let correlation_id = request.request_id;
        let data_type = request.data_type.clone();
        let cmd_start = request.start.map(|dt| dt.into());
        let cmd_end = request.end.map(|dt| dt.into());
        let params = request.params;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id).await?;
                let now = get_atomic_clock_realtime().get_time_ns();

                let history = match gateway.read().await.history_handle().cloned() {
                    Some(h) => h,
                    None => {
                        log::error!(
                            "request_data(VolumeProfile): history plant not connected — \
                            set enable_history=true in config. Dropping request for {symbol}.{exchange}"
                        );
                        return Ok(());
                    }
                };

                let responses = match history
                    .load_volume_profile_minute_bars(
                        symbol.clone(),
                        exchange.clone(),
                        period,
                        start_sec,
                        end_sec,
                        None,
                        None,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!(
                            "request_data(VolumeProfile) failed for {symbol}.{exchange}: {e}"
                        );
                        return Ok(());
                    }
                };

                let bars: Vec<RithmicMinuteVolumeProfileBar> = responses
                    .iter()
                    .filter_map(|resp| {
                        let RithmicMessage::ResponseVolumeProfileMinuteBars(vp) = &resp.message
                        else {
                            return None;
                        };
                        let ts_event = vp
                            .marker
                            .map_or(now, |m| UnixNanos::from(m as u64 * 1_000_000_000));
                        let poc_price = RithmicMinuteVolumeProfileBar::compute_poc(
                            &vp.profile_price,
                            &vp.profile_bid_volume,
                            &vp.profile_ask_volume,
                        );
                        Some(RithmicMinuteVolumeProfileBar {
                            instrument_id,
                            open_price: vp.open_price.unwrap_or(0.0),
                            high_price: vp.high_price.unwrap_or(0.0),
                            low_price: vp.low_price.unwrap_or(0.0),
                            close_price: vp.close_price.unwrap_or(0.0),
                            volume: vp.volume.unwrap_or(0),
                            bid_volume: vp.bid_volume.unwrap_or(0),
                            ask_volume: vp.ask_volume.unwrap_or(0),
                            num_trades: vp.num_trades.unwrap_or(0),
                            poc_price,
                            profile_price: vp.profile_price.clone(),
                            profile_bid_volume: vp.profile_bid_volume.clone(),
                            profile_ask_volume: vp.profile_ask_volume.clone(),
                            ts_event,
                            ts_init: now,
                        })
                    })
                    .collect();

                log::info!(
                    "request_data(VolumeProfile): loaded {} bars for {symbol}.{exchange}",
                    bars.len()
                );

                let response = CustomDataResponse::new(
                    correlation_id,
                    client_id,
                    Some(Venue::from(RITHMIC_VENUE)),
                    data_type,
                    bars,
                    cmd_start,
                    cmd_end,
                    now,
                    params,
                );

                data_sender
                    .send(DataEvent::Response(DataResponse::Data(response)))
                    .map_err(|e| anyhow::anyhow!("Failed to send CustomDataResponse: {e}"))
            },
            "request_volume_profile_bars",
        );

        Ok(())
    }

    fn request_bars(&self, cmd: RequestBars) -> anyhow::Result<()> {
        let (instrument_id, spec) = match &cmd.bar_type {
            BarType::Standard {
                instrument_id,
                spec,
                ..
            } => (*instrument_id, *spec),
            _ => anyhow::bail!(
                "Composite BarType is not supported for Rithmic: {}",
                cmd.bar_type
            ),
        };

        if spec.aggregation == BarAggregation::Tick {
            let period = spec.step.get() as u32;
            let start_sec = cmd.start.map_or(0, |dt| dt.as_second() as i32);
            let now_sec = ((get_atomic_clock_realtime().get_time_ns().as_u64()) / 1_000_000_000)
                .min(i32::MAX as u64) as i32;
            let end_sec = normalize_history_request_end(
                start_sec,
                cmd.end.map_or(0, |dt| dt.as_second() as i32),
                now_sec,
            );

            let gateway = self.require_gateway()?;
            let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
            let data_sender = self
                .data_sender
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Data sender not initialized"))?;
            let client_id = cmd.client_id.unwrap_or(self.client_id);
            let correlation_id = cmd.request_id;
            let bar_type = cmd.bar_type;
            let cmd_start = cmd.start.map(|dt| dt.into());
            let cmd_end = cmd.end.map(|dt| dt.into());

            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    let now = get_atomic_clock_realtime().get_time_ns();
                    let history_handle = {
                        let gateway = gateway.read().await;
                        gateway.history_handle().cloned()
                    };

                    // Always emits a BarsResponse — empty on error so strategy is not hung.
                    let bars: Vec<Bar> = match history_handle {
                        None => {
                            log::error!(
                                "request_tick_bars: history plant not connected — \
                                set enable_history=true in config. \
                                Sending empty BarsResponse"
                            );
                            vec![]
                        }
                        Some(handle) => {
                            match handle
                                .load_tick_bars(
                                    symbol.clone(),
                                    exchange.clone(),
                                    period,
                                    start_sec,
                                    end_sec,
                                )
                                .await
                            {
                                Err(e) => {
                                    log::error!(
                                        "request_tick_bars failed for {bar_type}: {e} — \
                                        sending empty BarsResponse"
                                    );
                                    vec![]
                                }
                                Ok(responses) => {
                                    let key = format!("{exchange}:{symbol}");
                                    let (price_prec, size_prec) = {
                                        let gateway = gateway.read().await;
                                        gateway
                                            .instruments()
                                            .try_read()
                                            .ok()
                                            .and_then(|m| m.get(&key).cloned())
                                            .map_or((2, 0), |info| {
                                                let pp = info
                                                    .tick_size
                                                    .map_or(2, tick_size_to_precision);
                                                (pp, 0)
                                            })
                                    };

                                    responses
                                        .iter()
                                        .filter_map(|resp| {
                                            let RithmicMessage::ResponseTickBarReplay(tick) =
                                                &resp.message
                                            else {
                                                return None;
                                            };
                                            let tick_period = tick
                                                .type_specifier
                                                .as_deref()
                                                .and_then(|s| s.parse::<u32>().ok())
                                                .unwrap_or(0);

                                            if tick_period != period {
                                                return None;
                                            }
                                            let ts_event = tick
                                                .data_bar_ssboe
                                                .first()
                                                .copied()
                                                .map_or(now, |secs| {
                                                    let usecs = tick
                                                        .data_bar_usecs
                                                        .first()
                                                        .copied()
                                                        .unwrap_or(0)
                                                        as u64;
                                                    UnixNanos::from(
                                                        (secs as u64) * 1_000_000_000
                                                            + usecs * 1_000,
                                                    )
                                                });
                                            Some(Bar::new(
                                                bar_type,
                                                Price::new(
                                                    tick.open_price.unwrap_or(0.0),
                                                    price_prec,
                                                ),
                                                Price::new(
                                                    tick.high_price.unwrap_or(0.0),
                                                    price_prec,
                                                ),
                                                Price::new(
                                                    tick.low_price.unwrap_or(0.0),
                                                    price_prec,
                                                ),
                                                Price::new(
                                                    tick.close_price.unwrap_or(0.0),
                                                    price_prec,
                                                ),
                                                Quantity::new(
                                                    tick.volume.unwrap_or(0) as f64,
                                                    size_prec,
                                                ),
                                                ts_event,
                                                now,
                                            ))
                                        })
                                        .collect()
                                }
                            }
                        }
                    };

                    let response = BarsResponse::new(
                        correlation_id,
                        client_id,
                        bar_type,
                        bars,
                        cmd_start,
                        cmd_end,
                        now,
                        None,
                    );

                    data_sender
                        .send(DataEvent::Response(DataResponse::Bars(response)))
                        .map_err(|e| anyhow::anyhow!("Failed to send BarsResponse: {e}"))
                },
                "request_tick_bars",
            );
            return Ok(());
        }

        let time_bar_type = bar_aggregation_to_time_bar_type(spec.aggregation)?;
        let period = spec.step.get() as i32;

        let start_sec = cmd.start.map_or(0, |dt| dt.as_second() as i32);
        let now_sec = ((get_atomic_clock_realtime().get_time_ns().as_u64()) / 1_000_000_000)
            .min(i32::MAX as u64) as i32;
        let end_sec = normalize_history_request_end(
            start_sec,
            cmd.end.map_or(0, |dt| dt.as_second() as i32),
            now_sec,
        );
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let data_sender = self
            .data_sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Data sender not initialized"))?;
        let client_id = cmd.client_id.unwrap_or(self.client_id);
        let correlation_id = cmd.request_id;
        let bar_type = cmd.bar_type;
        let cmd_start = cmd.start.map(|dt| dt.into());
        let cmd_end = cmd.end.map(|dt| dt.into());

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let now = get_atomic_clock_realtime().get_time_ns();

                // Always emits a BarsResponse, even on error — strategy must not hang.
                let bars: Vec<Bar> = match gateway
                    .read()
                    .await
                    .request_bars(
                        &symbol,
                        &exchange,
                        time_bar_type,
                        period,
                        start_sec,
                        end_sec,
                    )
                    .await
                {
                    Err(e) => {
                        log::error!(
                            "request_bars failed for {bar_type}: {e} — \
                            sending empty BarsResponse so strategy is not hung"
                        );
                        vec![]
                    }
                    Ok(responses) => {
                        let key = format!("{exchange}:{symbol}");
                        let (price_prec, size_prec) = {
                            let gateway = gateway.read().await;
                            gateway
                                .instruments()
                                .try_read()
                                .ok()
                                .and_then(|m| m.get(&key).cloned())
                                .map_or((2, 0), |info| {
                                    let price_prec =
                                        info.tick_size.map_or(2, tick_size_to_precision);
                                    (price_prec, 0)
                                })
                        };

                        let bars = responses
                            .iter()
                            .filter_map(|resp| {
                                historical_time_bar_to_bar(
                                    &resp.message,
                                    bar_type,
                                    price_prec,
                                    size_prec,
                                    now,
                                )
                            })
                            .collect();

                        normalize_time_history_bars(bars)
                    }
                };

                let response = BarsResponse::new(
                    correlation_id,
                    client_id,
                    bar_type,
                    bars,
                    cmd_start,
                    cmd_end,
                    now,
                    None,
                );

                data_sender
                    .send(DataEvent::Response(DataResponse::Bars(response)))
                    .map_err(|e| anyhow::anyhow!("Failed to send BarsResponse: {e}"))?;

                Ok(())
            },
            "request_bars",
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use nautilus_model::enums::RecordFlag;
    use rithmic_rs::rti::{
        DepthByOrder, ResponseDepthByOrderSnapshot, ResponseTickBarReplay, ResponseTimeBarReplay,
        depth_by_order::{TransactionType, UpdateType},
    };
    use serde::{Deserialize, de::DeserializeOwned};
    use serde_json::json;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct DepthSnapshotFixture {
        snapshots: Vec<SnapshotRowFixture>,
        delta: DepthDeltaFixture,
    }

    #[derive(Debug, Deserialize)]
    struct SnapshotRowFixture {
        template_id: i32,
        user_msg: Vec<String>,
        rq_handler_rp_code: Vec<String>,
        rp_code: Vec<String>,
        exchange: String,
        symbol: String,
        sequence_number: u64,
        depth_side: i32,
        depth_price: f64,
        depth_size: Vec<i32>,
        depth_order_priority: Vec<u64>,
        exchange_order_id: Vec<String>,
    }

    impl SnapshotRowFixture {
        fn into_message(self) -> ResponseDepthByOrderSnapshot {
            ResponseDepthByOrderSnapshot {
                template_id: self.template_id,
                user_msg: self.user_msg,
                rq_handler_rp_code: self.rq_handler_rp_code,
                rp_code: self.rp_code,
                exchange: Some(self.exchange),
                symbol: Some(self.symbol),
                sequence_number: Some(self.sequence_number),
                depth_side: Some(self.depth_side),
                depth_price: Some(self.depth_price),
                depth_size: self.depth_size,
                depth_order_priority: self.depth_order_priority,
                exchange_order_id: self.exchange_order_id,
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct DepthDeltaFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        sequence_number: u64,
        update_type: Vec<i32>,
        transaction_type: Vec<i32>,
        depth_price: Vec<f64>,
        prev_depth_price: Vec<f64>,
        prev_depth_price_flag: Vec<bool>,
        depth_size: Vec<i32>,
        depth_order_priority: Vec<u64>,
        exchange_order_id: Vec<String>,
        ssboe: i32,
        usecs: i32,
        source_ssboe: Option<i32>,
        source_usecs: Option<i32>,
        source_nsecs: Option<i32>,
        jop_ssboe: Option<i32>,
        jop_nsecs: Option<i32>,
    }

    impl DepthDeltaFixture {
        fn into_message(self) -> DepthByOrder {
            DepthByOrder {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                sequence_number: Some(self.sequence_number),
                update_type: self.update_type,
                transaction_type: self.transaction_type,
                depth_price: self.depth_price,
                prev_depth_price: self.prev_depth_price,
                prev_depth_price_flag: self.prev_depth_price_flag,
                depth_size: self.depth_size,
                depth_order_priority: self.depth_order_priority,
                exchange_order_id: self.exchange_order_id,
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
                source_ssboe: self.source_ssboe,
                source_usecs: self.source_usecs,
                source_nsecs: self.source_nsecs,
                jop_ssboe: self.jop_ssboe,
                jop_nsecs: self.jop_nsecs,
            }
        }
    }

    fn load_fixture<T: DeserializeOwned>(name: &str) -> T {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join(name);
        let bytes =
            fs::read(&path).unwrap_or_else(|e| panic!("failed reading fixture {path:?}: {e}"));
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("failed parsing fixture {path:?}: {e}"))
    }

    fn depth_delta_from_fixture(delta: &DepthByOrder, index: usize) -> crate::data::BookDelta {
        let action = match delta
            .update_type
            .get(index)
            .copied()
            .and_then(|value| UpdateType::try_from(value).ok())
            .expect("fixture update_type should be valid")
        {
            UpdateType::New => "ADD",
            UpdateType::Change => "UPDATE",
            UpdateType::Delete => "REMOVE",
        };
        let side = match delta
            .transaction_type
            .get(index)
            .copied()
            .and_then(|value| TransactionType::try_from(value).ok())
            .expect("fixture transaction_type should be valid")
        {
            TransactionType::Buy => "BUY",
            TransactionType::Sell => "SELL",
        };

        crate::data::BookDelta {
            symbol: delta.symbol.clone().expect("fixture symbol should be set"),
            exchange: delta
                .exchange
                .clone()
                .expect("fixture exchange should be set"),
            action: action.to_string(),
            side: side.to_string(),
            price: delta.depth_price[index],
            size: f64::from(delta.depth_size[index]),
            order_id: rithmic_depth_order_id(
                delta.exchange_order_id.get(index).map(String::as_str),
                delta.depth_order_priority[index],
            ),
            sequence: delta.sequence_number.unwrap_or_default(),
            flags: if index + 1 == delta.update_type.len() {
                RecordFlag::F_LAST as u8
            } else {
                0
            },
            price_precision: 2,
            size_precision: 0,
            ts_event: rithmic_timestamp_to_unix_nanos(delta.ssboe, delta.usecs).as_u64(),
            ts_init: rithmic_timestamp_to_unix_nanos(delta.ssboe, delta.usecs).as_u64(),
        }
    }

    struct TestBookSubscriptions {
        wants_deltas: bool,
        wants_depth10: bool,
    }

    impl BookSubscriptionView for TestBookSubscriptions {
        fn wants_book_deltas(&self, _symbol: &str, _exchange: &str) -> bool {
            self.wants_deltas
        }

        fn wants_book_depth10(&self, _symbol: &str, _exchange: &str) -> bool {
            self.wants_depth10
        }
    }

    #[rstest::rstest]
    fn normalize_history_request_end_uses_now_for_start_only_requests() {
        assert_eq!(
            normalize_history_request_end(1_700_000_000, 0, 1_700_000_060),
            1_700_000_060
        );
        assert_eq!(
            normalize_history_request_end(1_700_000_000, 1_700_000_030, 1_700_000_060),
            1_700_000_030
        );
        assert_eq!(normalize_history_request_end(0, 0, 1_700_000_060), 0);
    }

    #[rstest::rstest]
    fn request_instruments_helpers_honor_expected_defaults() {
        let mut params = Params::new();
        params.insert("exchange".to_string(), json!("CME"));
        params.insert("tradeable_only".to_string(), json!(true));

        assert!(request_instruments_front_month_only(&None));
        assert!(!request_instruments_tradeable_only(&None));
        assert_eq!(
            request_instruments_param_exchanges(&Some(params)),
            Some(vec!["CME".to_string()])
        );
    }

    #[rstest::rstest]
    fn process_market_data_event_batches_full_book_delta_messages() {
        let fixture = load_fixture::<DepthSnapshotFixture>("depth_snapshot_delta.json");
        let delta_message = fixture.delta.into_message();
        let subscriptions = TestBookSubscriptions {
            wants_deltas: true,
            wants_depth10: false,
        };
        let bar_type_map = ParkingRwLock::new(AHashMap::new());
        let mut order_books = HashMap::new();
        let mut order_book_delta_batches = HashMap::new();

        let first_events = process_market_data_event(
            MarketDataEvent::BookDelta(depth_delta_from_fixture(&delta_message, 0)),
            &bar_type_map,
            &mut order_books,
            &mut order_book_delta_batches,
            &subscriptions,
        );
        assert!(first_events.is_empty());
        assert_eq!(order_book_delta_batches.len(), 1);

        let second_events = process_market_data_event(
            MarketDataEvent::BookDelta(depth_delta_from_fixture(&delta_message, 1)),
            &bar_type_map,
            &mut order_books,
            &mut order_book_delta_batches,
            &subscriptions,
        );

        assert!(order_book_delta_batches.is_empty());
        assert_eq!(second_events.len(), 1);

        let DataEvent::Data(Data::Deltas(deltas)) = &second_events[0] else {
            panic!("expected full order book delta batch");
        };

        assert_eq!(deltas.instrument_id, InstrumentId::from("ESM6.RITHMIC"));
        assert_eq!(deltas.deltas.len(), 2);
        assert_eq!(deltas.deltas[0].flags, 0);
        assert_eq!(deltas.deltas[1].flags, RecordFlag::F_LAST as u8);
        assert_eq!(deltas.deltas[0].order.price.as_f64(), 4500.25);
        assert_eq!(deltas.deltas[1].order.price.as_f64(), 4500.75);
    }

    #[rstest::rstest]
    fn request_instruments_helpers_filter_exchange_lists_to_supported_scope() {
        let mut params = Params::new();
        params.insert("exchanges".to_string(), json!(["CME", "NASDAQ", "NYMEX"]));

        assert_eq!(
            request_instruments_param_exchanges(&Some(params)),
            Some(vec!["CME".to_string(), "NYMEX".to_string()])
        );
    }

    #[rstest::rstest]
    fn historical_time_bar_to_bar_accepts_replay_messages() {
        let bar_type = BarType::from("ESM6.RITHMIC-1-MINUTE-LAST-EXTERNAL");
        let message = RithmicMessage::ResponseTimeBarReplay(ResponseTimeBarReplay {
            template_id: 203,
            request_key: Some("history-req".to_string()),
            user_msg: vec!["1".to_string()],
            rp_code: vec![],
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            period: Some("1".to_string()),
            marker: Some(1_700_000_000),
            volume: Some(80),
            open_price: Some(4500.25),
            close_price: Some(4500.75),
            high_price: Some(4501.00),
            low_price: Some(4499.75),
            ..Default::default()
        });

        let bar = historical_time_bar_to_bar(&message, bar_type, 2, 0, UnixNanos::from(5))
            .expect("replay response should convert to Bar");

        assert_eq!(bar.bar_type, bar_type);
        assert_eq!(bar.open.as_f64(), 4500.25);
        assert_eq!(bar.close.as_f64(), 4500.75);
        assert_eq!(bar.volume.as_f64(), 80.0);
        assert_eq!(
            bar.ts_event,
            UnixNanos::from(1_700_000_000_u64 * 1_000_000_000)
        );
        assert_eq!(bar.ts_init, UnixNanos::from(5));
    }

    #[rstest::rstest]
    fn historical_time_bar_to_bar_drops_markerless_messages() {
        let bar_type = BarType::from("ESM6.RITHMIC-1-MINUTE-LAST-EXTERNAL");

        let replay = RithmicMessage::ResponseTimeBarReplay(ResponseTimeBarReplay {
            template_id: 203,
            request_key: Some("history-req".to_string()),
            user_msg: vec!["1".to_string()],
            rp_code: vec![],
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            period: Some("1".to_string()),
            marker: None,
            volume: Some(80),
            open_price: Some(4500.25),
            close_price: Some(4500.75),
            high_price: Some(4501.00),
            low_price: Some(4499.75),
            ..Default::default()
        });
        assert!(historical_time_bar_to_bar(&replay, bar_type, 2, 0, UnixNanos::from(5)).is_none());

        let live = RithmicMessage::TimeBar(rithmic_rs::rti::TimeBar {
            template_id: 250,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            r#type: None,
            period: Some("1".to_string()),
            marker: None,
            volume: Some(110),
            open_price: Some(4500.75),
            close_price: Some(4501.25),
            high_price: Some(4501.50),
            low_price: Some(4500.50),
            ..Default::default()
        });
        assert!(historical_time_bar_to_bar(&live, bar_type, 2, 0, UnixNanos::from(5)).is_none());
    }

    #[rstest::rstest]
    fn historical_tick_bar_to_trade_sets_no_aggressor_for_replay_ticks() {
        let instrument_id = InstrumentId::from("ESM6.RITHMIC");
        let message = RithmicMessage::ResponseTickBarReplay(ResponseTickBarReplay {
            template_id: 204,
            request_key: Some("history-req".to_string()),
            user_msg: vec!["1".to_string()],
            rq_handler_rp_code: vec![],
            rp_code: vec![],
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            r#type: Some(1),
            sub_type: Some(1),
            type_specifier: Some("1".to_string()),
            num_trades: Some(1),
            volume: Some(7),
            bid_volume: Some(0),
            ask_volume: Some(0),
            open_price: Some(4500.25),
            close_price: Some(4500.25),
            high_price: Some(4500.25),
            low_price: Some(4500.25),
            custom_session_open_ssm: None,
            data_bar_ssboe: vec![1_700_000_123],
            data_bar_usecs: vec![456_789],
        });

        let trade = historical_tick_bar_to_trade(&message, instrument_id, 2, 0, 3)
            .expect("replay tick should convert to TradeTick");

        assert_eq!(trade.instrument_id, instrument_id);
        assert_eq!(trade.price.as_f64(), 4500.25);
        assert_eq!(trade.size.as_f64(), 7.0);
        assert_eq!(trade.aggressor_side, AggressorSide::NoAggressor);
        assert_eq!(trade.trade_id.to_string(), "replay:1700000123456789000:3");
        assert_eq!(
            trade.ts_event,
            UnixNanos::from(1_700_000_123_456_789_000_u64)
        );
        assert_eq!(
            trade.ts_init,
            UnixNanos::from(1_700_000_123_456_789_000_u64)
        );
    }

    #[rstest::rstest]
    fn historical_tick_bar_to_trade_drops_zero_timestamp_replay_rows() {
        let instrument_id = InstrumentId::from("ESM6.RITHMIC");
        let message = RithmicMessage::ResponseTickBarReplay(ResponseTickBarReplay {
            template_id: 204,
            request_key: Some("history-req".to_string()),
            user_msg: vec!["1".to_string()],
            rq_handler_rp_code: vec![],
            rp_code: vec![],
            symbol: Some(String::new()),
            exchange: Some(String::new()),
            r#type: Some(1),
            sub_type: Some(1),
            type_specifier: Some("1".to_string()),
            num_trades: Some(0),
            volume: Some(0),
            bid_volume: Some(0),
            ask_volume: Some(0),
            open_price: Some(0.0),
            close_price: Some(0.0),
            high_price: Some(0.0),
            low_price: Some(0.0),
            custom_session_open_ssm: None,
            data_bar_ssboe: vec![],
            data_bar_usecs: vec![],
        });

        assert!(historical_tick_bar_to_trade(&message, instrument_id, 2, 0, 0).is_none());
    }

    #[rstest::rstest]
    fn live_trade_id_falls_back_when_exchange_order_id_is_missing() {
        let tick = crate::data::TradeTick {
            symbol: "MNQM6".to_string(),
            exchange: "CME".to_string(),
            price: 25_572.25,
            size: 3.0,
            aggressor_side: "BUY".to_string(),
            trade_id: String::new(),
            price_precision: 2,
            size_precision: 0,
            ts_event: 1_700_000_123_456_789_000_u64,
            ts_init: 1_700_000_123_456_789_000_u64,
        };

        let trade_id = live_trade_id(&tick);

        assert_eq!(trade_id.to_string(), "live:1700000123456789000:MNQM6");
    }

    #[rstest::rstest]
    fn normalize_time_history_bars_keeps_current_open_bar() {
        let bar_type = BarType::from("ESM6.RITHMIC-1-MINUTE-LAST-EXTERNAL");
        let ts_init = UnixNanos::from(5_u64);
        let bars = vec![
            Bar::new(
                bar_type,
                Price::new(4500.0, 2),
                Price::new(4500.5, 2),
                Price::new(4499.5, 2),
                Price::new(4500.25, 2),
                Quantity::new(100.0, 0),
                UnixNanos::from(1_700_000_000_u64 * 1_000_000_000),
                ts_init,
            ),
            Bar::new(
                bar_type,
                Price::new(4500.25, 2),
                Price::new(4500.75, 2),
                Price::new(4500.0, 2),
                Price::new(4500.5, 2),
                Quantity::new(80.0, 0),
                UnixNanos::from(1_700_000_060_u64 * 1_000_000_000),
                ts_init,
            ),
        ];

        let filtered = normalize_time_history_bars(bars);

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered[0].ts_event,
            UnixNanos::from(1_700_000_000_u64 * 1_000_000_000)
        );
        assert_eq!(
            filtered[1].ts_event,
            UnixNanos::from(1_700_000_060_u64 * 1_000_000_000)
        );
    }

    #[rstest::rstest]
    fn order_book_snapshot_and_delta_fixtures_produce_expected_depth10() {
        let fixture = load_fixture::<DepthSnapshotFixture>("depth_snapshot_delta.json");
        let instrument_id = InstrumentId::from("ESM6.RITHMIC");
        let snapshot_rows: Vec<ResponseDepthByOrderSnapshot> = fixture
            .snapshots
            .into_iter()
            .map(SnapshotRowFixture::into_message)
            .collect();
        let snapshot_refs: Vec<&ResponseDepthByOrderSnapshot> = snapshot_rows.iter().collect();
        let ts_init = UnixNanos::from(1_700_000_100_500_000_000_u64);

        let mut book = order_book_from_snapshot_rows(instrument_id, &snapshot_refs, 2, 0, ts_init);
        let delta_message = fixture.delta.into_message();
        let initial_depth = depth10_from_order_book(&book, ts_init, ts_init);

        assert_eq!(initial_depth.bids[0].price.as_f64(), 4500.25);
        assert_eq!(initial_depth.bids[0].size.as_f64(), 12.0);
        assert_eq!(initial_depth.bid_counts[0], 2);
        assert_eq!(initial_depth.asks[0].price.as_f64(), 4500.50);
        assert_eq!(initial_depth.asks[0].size.as_f64(), 6.0);
        assert_eq!(initial_depth.ask_counts[0], 1);

        for index in 0..delta_message.update_type.len() {
            let delta = convert_book_delta_event(&depth_delta_from_fixture(&delta_message, index))
                .expect("fixture delta should convert into a Nautilus delta");
            book.apply_delta(&delta)
                .expect("fixture delta should apply to the local book");
        }

        let updated_depth = depth10_from_order_book(
            &book,
            UnixNanos::from(1_700_000_102_250_000_000_u64),
            ts_init,
        );

        assert_eq!(updated_depth.bids[0].price.as_f64(), 4500.25);
        assert_eq!(updated_depth.bids[0].size.as_f64(), 14.0);
        assert_eq!(updated_depth.bid_counts[0], 2);
        assert_eq!(updated_depth.asks[0].price.as_f64(), 4500.50);
        assert_eq!(updated_depth.asks[0].size.as_f64(), 6.0);
        assert_eq!(updated_depth.asks[1].price.as_f64(), 4500.75);
        assert_eq!(updated_depth.asks[1].size.as_f64(), 4.0);
        assert_eq!(updated_depth.ask_counts[1], 1);
        assert_eq!(
            updated_depth.flags,
            RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8
        );
    }

    #[rstest::rstest]
    fn order_book_price_move_uses_exchange_order_id_identity() {
        let instrument_id = InstrumentId::from("ESM6.RITHMIC");
        let snapshot = SnapshotRowFixture {
            template_id: 157,
            user_msg: vec![],
            rq_handler_rp_code: vec![],
            rp_code: vec![],
            exchange: "CME".to_string(),
            symbol: "ESM6".to_string(),
            sequence_number: 101,
            depth_side: TransactionType::Buy as i32,
            depth_price: 4500.25,
            depth_size: vec![7],
            depth_order_priority: vec![11],
            exchange_order_id: vec!["bid-11".to_string()],
        }
        .into_message();
        let snapshot_refs = vec![&snapshot];
        let ts_init = UnixNanos::from(1_700_000_100_500_000_000_u64);

        let mut book = order_book_from_snapshot_rows(instrument_id, &snapshot_refs, 2, 0, ts_init);

        let delta_message = DepthDeltaFixture {
            template_id: 156,
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 102,
            update_type: vec![UpdateType::Change as i32],
            transaction_type: vec![TransactionType::Buy as i32],
            depth_price: vec![4499.75],
            prev_depth_price: vec![4500.25],
            prev_depth_price_flag: vec![true],
            depth_size: vec![7],
            depth_order_priority: vec![1],
            exchange_order_id: vec!["bid-11".to_string()],
            ssboe: 1_700_000_102,
            usecs: 250_000,
            source_ssboe: None,
            source_usecs: None,
            source_nsecs: None,
            jop_ssboe: None,
            jop_nsecs: None,
        }
        .into_message();

        let delta = convert_book_delta_event(&depth_delta_from_fixture(&delta_message, 0))
            .expect("delta should convert when exchange_order_id is present");
        book.apply_delta(&delta)
            .expect("delta should move the existing order");

        let updated_depth = depth10_from_order_book(
            &book,
            UnixNanos::from(1_700_000_102_250_000_000_u64),
            ts_init,
        );

        assert_eq!(updated_depth.bids[0].price.as_f64(), 4499.75);
        assert_eq!(updated_depth.bids[0].size.as_f64(), 7.0);
        assert_eq!(updated_depth.bid_counts[0], 1);
        assert_eq!(updated_depth.bids[1].price.as_f64(), 0.0);
        assert_eq!(updated_depth.bids[1].size.as_f64(), 0.0);
    }
}
