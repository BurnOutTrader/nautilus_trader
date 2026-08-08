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
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender, task::TaskHandles},
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
    gateway::{GatewayConfig, InstrumentInfo, RithmicGateway},
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
const EVENT_TASK_SHUTDOWN_TIMEOUT_SECS: u64 = 5;

fn rithmic_timestamp_to_unix_nanos(ssboe: Option<i32>, usecs: Option<i32>) -> Option<UnixNanos> {
    let secs = u64::try_from(ssboe?).ok()?;
    let micros = u64::try_from(usecs.unwrap_or_default()).ok()?;
    if micros >= 1_000_000 {
        return None;
    }

    Some(UnixNanos::from(
        secs.checked_mul(1_000_000_000)?
            .checked_add(micros.checked_mul(1_000)?)?,
    ))
}

fn normalize_history_request_end(start_sec: i32, end_sec: i32, now_sec: i32) -> i32 {
    if start_sec > 0 && end_sec <= 0 {
        now_sec.max(start_sec)
    } else {
        end_sec
    }
}

fn checked_price(value: f64, precision: u8, context: &str) -> Option<Price> {
    Price::new_checked(value, precision)
        .map_err(|e| log::warn!("Dropping {context} with invalid price {value}: {e}"))
        .ok()
}

fn checked_quantity(value: f64, precision: u8, context: &str) -> Option<Quantity> {
    Quantity::new_checked(value, precision)
        .map_err(|e| log::warn!("Dropping {context} with invalid quantity {value}: {e}"))
        .ok()
}

fn checked_integer_quantity(value: u64, precision: u8, context: &str) -> Option<Quantity> {
    Quantity::from_mantissa_exponent_checked(value, 0, precision)
        .map_err(|e| log::warn!("Dropping {context} with invalid quantity {value}: {e}"))
        .ok()
}

fn checked_instrument_precisions(
    instrument: Option<&InstrumentInfo>,
    key: &str,
) -> anyhow::Result<(u8, u8)> {
    let tick_size = instrument
        .and_then(|info| info.tick_size)
        .ok_or_else(|| anyhow::anyhow!("Instrument precision is not available for {key}"))?;
    if !tick_size.is_finite() || tick_size <= 0.0 {
        anyhow::bail!("Instrument tick size is invalid for {key}: {tick_size}");
    }

    let price_precision = tick_size_to_precision(tick_size)
        .map_err(|e| anyhow::anyhow!("Instrument tick size is invalid for {key}: {e}"))?;
    Price::new_checked(tick_size, price_precision)
        .map_err(|e| anyhow::anyhow!("Instrument tick size is invalid for {key}: {e}"))?;
    // Rithmic market-data quantities are integer contract counts.
    Ok((price_precision, 0))
}

fn live_trade_id(tick: &crate::data::TradeTick) -> Option<TradeId> {
    let trade_id = tick.trade_id.trim();
    if !trade_id.is_empty() {
        return TradeId::new_checked(trade_id)
            .map_err(|e| log::warn!("Dropping trade with invalid venue trade ID: {e}"))
            .ok();
    }

    // Rithmic can omit both exchange order IDs. Hash every available immutable
    // trade field into a bounded deterministic fallback. Identical executions
    // with identical fields and timestamps remain indistinguishable.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tick.exchange.hash(&mut hasher);
    tick.symbol.hash(&mut hasher);
    tick.ts_event.hash(&mut hasher);
    tick.price.to_bits().hash(&mut hasher);
    tick.size.to_bits().hash(&mut hasher);
    let fallback = format!("R{:016x}{:016x}", tick.ts_event, hasher.finish());
    TradeId::new_checked(fallback)
        .map_err(|e| log::warn!("Dropping trade with invalid fallback trade ID: {e}"))
        .ok()
}

fn resolved_contract_key(symbol: &str, exchange: &str) -> String {
    format!(
        "{}:{}",
        exchange.trim().to_ascii_uppercase(),
        symbol.trim().to_ascii_uppercase()
    )
}

fn cache_resolved_exchange(
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    symbol: &str,
    exchange: &str,
) {
    resolved_exchanges.write().insert(
        resolved_contract_key(symbol, exchange),
        exchange.trim().to_ascii_uppercase(),
    );
}

fn get_cached_exchange(
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    symbol: &str,
) -> Option<String> {
    let symbol_suffix = format!(":{}", symbol.trim().to_ascii_uppercase());
    let resolved = resolved_exchanges.read();
    let mut matches = resolved
        .iter()
        .filter(|(key, _)| key.ends_with(&symbol_suffix))
        .map(|(_, exchange)| exchange.clone());
    let first = matches.next()?;
    matches.all(|exchange| exchange == first).then_some(first)
}

fn select_unique_resolved_exchange(
    instrument_id: &InstrumentId,
    matches: &[String],
) -> anyhow::Result<String> {
    match matches {
        [exchange] => Ok(exchange.clone()),
        [] => anyhow::bail!("Unable to resolve exchange for Rithmic contract {instrument_id}"),
        _ => anyhow::bail!(
            "Ambiguous exchange for Rithmic contract {instrument_id}; matched {}",
            matches.join(", ")
        ),
    }
}

pub(crate) async fn resolve_contract_exchange(
    gateway: &Arc<tokio::sync::RwLock<RithmicGateway>>,
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    instrument_id: &InstrumentId,
) -> anyhow::Result<(String, String)> {
    let (symbol, explicit_exchange) = parse_rithmic_instrument(instrument_id);

    if !explicit_exchange.is_empty() {
        crate::common::converters::rithmic_instrument_id(&symbol, &explicit_exchange)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
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

    let exchange_candidates = candidate_exchanges_for_symbol(&symbol, None)
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if exchange_candidates.len() == 1 {
        let exchange = exchange_candidates[0].clone();
        cache_resolved_exchange(resolved_exchanges, &symbol, &exchange);
        return Ok((symbol, exchange));
    }

    let ticker = {
        let gateway = gateway.read().await;
        gateway.ticker_handle().cloned().ok_or_else(|| {
            anyhow::anyhow!("Ticker handle not available — is ticker plant connected?")
        })?
    };

    let matches = stream::iter(exchange_candidates)
        .map(|candidate_exchange| {
            let ticker = ticker.clone();
            let symbol = symbol.clone();
            async move {
                let response = timeout(
                    std::time::Duration::from_secs(CONTRACT_EXCHANGE_RESOLUTION_TIMEOUT_SECS),
                    ticker.get_reference_data(&symbol, &candidate_exchange),
                )
                .await;
                let Ok(Ok(response)) = response else {
                    return None;
                };
                (response.error.is_none()
                    && matches!(response.message, RithmicMessage::ResponseReferenceData(_)))
                .then_some(candidate_exchange)
            }
        })
        .buffer_unordered(REQUEST_INSTRUMENTS_EXCHANGE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let exchange = select_unique_resolved_exchange(instrument_id, &matches)?;
    cache_resolved_exchange(resolved_exchanges, &symbol, &exchange);
    Ok((symbol, exchange))
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
    let seconds = u64::try_from(marker.filter(|value| *value > 0)?).ok()?;
    seconds.checked_mul(1_000_000_000).map(UnixNanos::from)
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

            Bar::new_checked(
                bar_type,
                checked_price(bar.open_price?, price_prec, "historical time bar")?,
                checked_price(bar.high_price?, price_prec, "historical time bar")?,
                checked_price(bar.low_price?, price_prec, "historical time bar")?,
                checked_price(bar.close_price?, price_prec, "historical time bar")?,
                checked_integer_quantity(bar.volume?, size_prec, "historical time bar")?,
                ts_event,
                ts_init,
            )
            .map_err(|e| log::warn!("Dropping invalid historical time bar: {e}"))
            .ok()
        }
        RithmicMessage::TimeBar(bar) => {
            let ts_event = time_bar_marker_to_unix_nanos(bar.marker)?;

            Bar::new_checked(
                bar_type,
                checked_price(bar.open_price?, price_prec, "live time bar")?,
                checked_price(bar.high_price?, price_prec, "live time bar")?,
                checked_price(bar.low_price?, price_prec, "live time bar")?,
                checked_price(bar.close_price?, price_prec, "live time bar")?,
                checked_integer_quantity(bar.volume?, size_prec, "live time bar")?,
                ts_event,
                ts_init,
            )
            .map_err(|e| log::warn!("Dropping invalid live time bar: {e}"))
            .ok()
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
    )?;
    let price = bar.close_price?;

    let sequence = u64::try_from(sequence).ok()?;
    let trade_id =
        TradeId::new_checked(format!("R{:016x}{:016x}", ts_event.as_u64(), sequence)).ok()?;
    TradeTick::new_checked(
        instrument_id,
        checked_price(price, price_prec, "historical tick bar")?,
        checked_integer_quantity(bar.volume?, size_prec, "historical tick bar")?,
        AggressorSide::NoAggressor,
        trade_id,
        ts_event,
        ts_event,
    )
    .map_err(|e| log::warn!("Dropping invalid historical tick bar trade: {e}"))
    .ok()
}

fn historical_tick_bar_to_bar(
    response: &rithmic_rs::rti::ResponseTickBarReplay,
    bar_type: BarType,
    expected_period: u32,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> Option<Bar> {
    let period = response
        .type_specifier
        .as_deref()
        .and_then(|value| value.parse::<u32>().ok())?;
    if period != expected_period {
        return None;
    }
    let ts_event = rithmic_timestamp_to_unix_nanos(
        response.data_bar_ssboe.first().copied(),
        response.data_bar_usecs.first().copied(),
    )?;

    Bar::new_checked(
        bar_type,
        checked_price(response.open_price?, price_precision, "historical tick bar")?,
        checked_price(response.high_price?, price_precision, "historical tick bar")?,
        checked_price(response.low_price?, price_precision, "historical tick bar")?,
        checked_price(
            response.close_price?,
            price_precision,
            "historical tick bar",
        )?,
        checked_integer_quantity(response.volume?, size_precision, "historical tick bar")?,
        ts_event,
        ts_init,
    )
    .map_err(|e| log::warn!("Dropping invalid historical tick bar: {e}"))
    .ok()
}

fn volume_profile_bar_from_response(
    response: &rithmic_rs::rti::ResponseVolumeProfileMinuteBars,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> Option<RithmicMinuteVolumeProfileBar> {
    let ts_event = time_bar_marker_to_unix_nanos(response.marker)?;
    let open_price = response.open_price.filter(|value| value.is_finite())?;
    let high_price = response.high_price.filter(|value| value.is_finite())?;
    let low_price = response.low_price.filter(|value| value.is_finite())?;
    let close_price = response.close_price.filter(|value| value.is_finite())?;
    let volume = response.volume?;
    let bid_volume = response.bid_volume?;
    let ask_volume = response.ask_volume?;
    let num_trades = response.num_trades?;
    if response.profile_price.len() != response.profile_bid_volume.len()
        || response.profile_price.len() != response.profile_ask_volume.len()
        || response
            .profile_price
            .iter()
            .any(|price| !price.is_finite())
        || response.profile_bid_volume.iter().any(|volume| *volume < 0)
        || response.profile_ask_volume.iter().any(|volume| *volume < 0)
    {
        return None;
    }

    let poc_price = response
        .profile_price
        .iter()
        .zip(
            response
                .profile_bid_volume
                .iter()
                .zip(response.profile_ask_volume.iter()),
        )
        .max_by_key(|(_, (bid, ask))| i64::from(**bid) + i64::from(**ask))
        .map(|(price, _)| *price);

    Some(RithmicMinuteVolumeProfileBar {
        instrument_id,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        bid_volume,
        ask_volume,
        num_trades,
        poc_price,
        profile_price: response.profile_price.clone(),
        profile_bid_volume: response.profile_bid_volume.clone(),
        profile_ask_volume: response.profile_ask_volume.clone(),
        ts_event,
        ts_init,
    })
}

fn append_volume_at_price_response(
    response: &rithmic_rs::rti::ResponseGetVolumeAtPrice,
    trade_prices: &mut Vec<f64>,
    volumes: &mut Vec<i32>,
) -> anyhow::Result<Option<UnixNanos>> {
    if response.trade_price.len() != response.volume_at_price.len()
        || response
            .trade_price
            .iter()
            .any(|price| !price.is_finite() || *price <= 0.0)
        || response.volume_at_price.iter().any(|volume| *volume < 0)
    {
        anyhow::bail!("Malformed VolumeAtPrice response");
    }
    if response.trade_price.is_empty() {
        return Ok(None);
    }
    let timestamp = rithmic_timestamp_to_unix_nanos(response.ssboe, response.usecs)
        .ok_or_else(|| anyhow::anyhow!("VolumeAtPrice response has no valid venue timestamp"))?;
    trade_prices.extend_from_slice(&response.trade_price);
    volumes.extend_from_slice(&response.volume_at_price);
    Ok(Some(timestamp))
}

/// Converts a Rithmic `InstrumentId` string back to Nautilus format.
///
/// `"ESH5"`, `"CME"` → `InstrumentId { symbol: "ESH5.CME", venue: "RITHMIC" }`
fn make_instrument_id(symbol: &str, exchange: &str) -> Option<InstrumentId> {
    match crate::common::converters::rithmic_instrument_id(symbol, exchange) {
        Ok(instrument_id) => Some(instrument_id),
        Err(e) => {
            log::warn!("Dropping invalid Rithmic instrument identity {symbol:?}/{exchange:?}: {e}");
            None
        }
    }
}

/// Parses a Nautilus `InstrumentId` into `(symbol, exchange)` for Rithmic.
///
/// - `ESH5.RITHMIC`     → `("ESH5", "")` (legacy input alias)
/// - `ESH5.CME.RITHMIC` → `("ESH5", "CME")`
pub(crate) fn parse_rithmic_instrument(instrument_id: &InstrumentId) -> (String, String) {
    let symbol_str = instrument_id.symbol.as_str();
    let mut parts = symbol_str.splitn(2, '.');
    let symbol = parts
        .next()
        .unwrap_or(symbol_str)
        .trim()
        .to_ascii_uppercase();
    let exchange = parts.next().unwrap_or("").trim().to_ascii_uppercase();
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
    let instrument_id = make_instrument_id(&d.symbol, &d.exchange)?;
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
    let order = BookOrder::new(
        side,
        checked_price(d.price, d.price_precision, "order-book delta")?,
        checked_quantity(d.size, d.size_precision, "order-book delta")?,
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
        let Some(size) = checked_quantity(level.size(), 0, "aggregated bid level") else {
            continue;
        };
        bids[i] = BookOrder::new(OrderSide::Buy, level.price.value, size, 0);
        bid_counts[i] = level.len() as u32;
    }

    for (i, level) in book.asks(Some(10)).enumerate() {
        let Some(size) = checked_quantity(level.size(), 0, "aggregated ask level") else {
            continue;
        };
        asks[i] = BookOrder::new(OrderSide::Sell, level.price.value, size, 0);
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
) -> anyhow::Result<OrderBook> {
    if let Some(e) = responses
        .iter()
        .find_map(|response| response.error.as_ref())
    {
        anyhow::bail!("Order book snapshot response error: {e}");
    }
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
) -> anyhow::Result<OrderBook> {
    use rithmic_rs::rti::response_depth_by_order_snapshot::TransactionType;

    let mut book = OrderBook::new(instrument_id, BookType::L3_MBO);
    let mut saw_snapshot = false;

    if snapshot_rows.is_empty() {
        anyhow::bail!("Order book snapshot contained no snapshot rows");
    }

    for snapshot in snapshot_rows {
        let symbol = snapshot
            .symbol
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Order book snapshot row is missing symbol"))?;
        let exchange = snapshot
            .exchange
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Order book snapshot row is missing exchange"))?;
        let row_instrument = crate::common::converters::rithmic_instrument_id(symbol, exchange)
            .map_err(|e| anyhow::anyhow!("Invalid snapshot instrument identity: {e}"))?;
        if row_instrument != instrument_id {
            anyhow::bail!(
                "Order book snapshot row identity {row_instrument} does not match {instrument_id}"
            );
        }
        let sequence = snapshot
            .sequence_number
            .ok_or_else(|| anyhow::anyhow!("Order book snapshot row is missing sequence"))?;
        let ts_event = ts_init;

        // Rithmic snapshot rows have no venue timestamp. Receipt/init time is
        // therefore the only protocol-available event-time anchor.
        if snapshot.depth_order_priority.is_empty()
            && snapshot.depth_size.is_empty()
            && snapshot.depth_side.is_none()
            && snapshot.depth_price.is_none()
        {
            if !saw_snapshot {
                book.clear(sequence, ts_event);
                saw_snapshot = true;
            }
            continue;
        }

        let side = match snapshot
            .depth_side
            .and_then(|value| TransactionType::try_from(value).ok())
        {
            Some(TransactionType::Buy) => OrderSide::Buy,
            Some(TransactionType::Sell) => OrderSide::Sell,
            None => anyhow::bail!("Order book snapshot row has invalid side"),
        };
        let Some(price) = snapshot
            .depth_price
            .and_then(|value| checked_price(value, price_precision, "order-book snapshot"))
        else {
            anyhow::bail!("Order book snapshot row has invalid price");
        };
        if snapshot.depth_order_priority.len() != snapshot.depth_size.len() {
            anyhow::bail!("Order book snapshot row has misaligned order vectors");
        }

        if !saw_snapshot {
            book.clear(sequence, ts_event);
            saw_snapshot = true;
        }

        for (i, depth_order_priority) in snapshot.depth_order_priority.iter().enumerate() {
            let size = snapshot.depth_size[i];

            if size < 0 {
                anyhow::bail!("Order book snapshot row has negative order size");
            }
            if size == 0 {
                continue;
            }
            let order_id = rithmic_depth_order_id(
                snapshot.exchange_order_id.get(i).map(String::as_str),
                *depth_order_priority,
            );
            let Some(quantity) = checked_integer_quantity(
                size.cast_unsigned().into(),
                size_precision,
                "order-book snapshot",
            ) else {
                anyhow::bail!("Order book snapshot row has invalid order size");
            };
            book.add(
                BookOrder::new(side, price, quantity, order_id),
                RecordFlag::F_SNAPSHOT as u8,
                sequence,
                ts_event,
            );
        }
    }

    if !saw_snapshot {
        anyhow::bail!("Order book snapshot contained no valid rows");
    }
    Ok(book)
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
            let Some(instrument_id) = make_instrument_id(&q.symbol, &q.exchange) else {
                return Vec::new();
            };
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
            let quote = QuoteTick::new_checked(
                instrument_id,
                match checked_price(q.bid_price, price_prec, "quote") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_price(q.ask_price, price_prec, "quote") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_quantity(q.bid_size, size_prec, "quote") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_quantity(q.ask_size, size_prec, "quote") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                q.ts_event.into(),
                q.ts_init.into(),
            )
            .map_err(|e| log::warn!("Dropping invalid Rithmic quote: {e}"));
            match quote {
                Ok(quote) => vec![DataEvent::Data(Data::Quote(quote))],
                Err(()) => Vec::new(),
            }
        }
        MarketDataEvent::Trade(t) => {
            let Some(instrument_id) = make_instrument_id(&t.symbol, &t.exchange) else {
                return Vec::new();
            };
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
            let Some(trade_id) = live_trade_id(&t) else {
                return Vec::new();
            };
            let trade = TradeTick::new_checked(
                instrument_id,
                match checked_price(t.price, price_prec, "trade") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_quantity(t.size, size_prec, "trade") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                convert_trade_aggressor(&t.aggressor_side),
                trade_id,
                t.ts_event.into(),
                t.ts_init.into(),
            )
            .map_err(|e| log::warn!("Dropping invalid Rithmic trade: {e}"));
            match trade {
                Ok(trade) => vec![DataEvent::Data(Data::Trade(trade))],
                Err(()) => Vec::new(),
            }
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
            let bar = Bar::new_checked(
                bar_type,
                match checked_price(b.open_price, price_prec, "live bar") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_price(b.high_price, price_prec, "live bar") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_price(b.low_price, price_prec, "live bar") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_price(b.close_price, price_prec, "live bar") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                match checked_quantity(b.volume, size_prec, "live bar") {
                    Some(value) => value,
                    None => return Vec::new(),
                },
                b.ts_event.into(),
                now,
            )
            .map_err(|e| log::warn!("Dropping invalid Rithmic bar: {e}"));
            match bar {
                Ok(bar) => vec![DataEvent::Data(Data::Bar(bar))],
                Err(()) => Vec::new(),
            }
        }
        MarketDataEvent::BookDelta(d) => {
            let mut output = Vec::new();
            let wants_deltas = book_subscriptions.wants_book_deltas(&d.symbol, &d.exchange);
            let wants_depth10 = book_subscriptions.wants_book_depth10(&d.symbol, &d.exchange);
            let Some(delta) = convert_book_delta_event(&d) else {
                return output;
            };
            let Some(instrument_id) = make_instrument_id(&d.symbol, &d.exchange) else {
                return output;
            };

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
    pending_tasks: TaskHandles,
    resolved_exchanges: Arc<ParkingRwLock<AHashMap<String, String>>>,
}

impl Debug for RithmicLiveDataClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicLiveDataClient))
            .field("client_id", &self.client_id)
            .field("is_connected", &self.is_connected.load(Ordering::Acquire))
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
            pending_tasks: TaskHandles::default(),
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
        self.pending_tasks.push(handle);
    }

    fn signal_event_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    fn abort_tasks(&mut self) {
        self.signal_event_shutdown();
        if let Some(task) = self.event_task.take() {
            task.abort();
        }
        self.pending_tasks.abort_all();
    }

    async fn shutdown_tasks(&mut self) {
        self.signal_event_shutdown();

        let pending = self.pending_tasks.take_all();
        for task in &pending {
            task.abort();
        }
        for task in pending {
            let _ = task.await;
        }

        let Some(mut event_task) = self.event_task.take() else {
            return;
        };
        if timeout(
            std::time::Duration::from_secs(EVENT_TASK_SHUTDOWN_TIMEOUT_SECS),
            &mut event_task,
        )
        .await
        .is_err()
        {
            log::warn!("Timed out waiting for Rithmic data event loop shutdown; aborting task");
            event_task.abort();
            let _ = event_task.await;
        }
    }

    fn clear_local_state(&mut self) {
        self.inner = None;
        self.data_sender = None;
        self.bar_type_map.write().clear();
        self.resolved_exchanges.write().clear();
        self.is_connected.store(false, Ordering::Release);
    }

    /// Builds a `GatewayConfig` for the data client (ticker plant only).
    fn gateway_config(&self) -> anyhow::Result<GatewayConfig> {
        let c = &self.config;
        let mut cfg = GatewayConfig::new(
            c.environment,
            c.username.as_str(),
            c.password.as_str(),
            c.system_name.as_str(),
            c.app_name.as_str(),
            c.fcm_id.as_deref().unwrap_or(""),
            c.ib_id.as_deref().unwrap_or(""),
            "", // data client has no account_id
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        cfg.app_version = c.app_version.clone();
        cfg.server = c.server.clone();
        cfg.alt_server = c.alt_server.clone();
        cfg.enable_ticker = true;
        cfg.enable_order = false;
        cfg.enable_pnl = false;
        cfg.enable_history = c.enable_history;
        Ok(cfg)
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
        self.abort_tasks();
        self.gateway = None;
        self.clear_local_state();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting: client_id={}", self.client_id);
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing: client_id={}", self.client_id);
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;
        if self.is_connected() {
            return Ok(());
        }

        // Register custom data types for JSON round-trip (idempotent).
        ensure_custom_data_json_registered::<RithmicMinuteVolumeProfileBar>()
            .map_err(|e| anyhow::anyhow!("Failed to register volume profile data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicTradeStatistics>()
            .map_err(|e| anyhow::anyhow!("Failed to register trade statistics data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicQuoteStatistics>()
            .map_err(|e| anyhow::anyhow!("Failed to register quote statistics data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicIndicatorPrices>()
            .map_err(|e| anyhow::anyhow!("Failed to register indicator prices data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicOpenInterest>()
            .map_err(|e| anyhow::anyhow!("Failed to register open interest data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicEndOfDayPrices>()
            .map_err(|e| anyhow::anyhow!("Failed to register end-of-day prices data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicOrderPriceLimits>()
            .map_err(|e| anyhow::anyhow!("Failed to register order price limits data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicSymbolMarginRate>()
            .map_err(|e| anyhow::anyhow!("Failed to register symbol margin rate data: {e}"))?;
        ensure_custom_data_json_registered::<RithmicVolumeAtPrice>()
            .map_err(|e| anyhow::anyhow!("Failed to register volume-at-price data: {e}"))?;

        let gateway_config = self.gateway_config()?;
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
                                is_connected.store(false, Ordering::Release);
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
                                match inner_for_reconnect.resubscribe_all().await {
                                    Ok(()) => is_connected.store(true, Ordering::Release),
                                    Err(e) => {
                                        is_connected.store(false, Ordering::Release);
                                        log::error!("Rithmic subscription recovery failed: {e}");
                                        break;
                                    }
                                }
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
                                log::warn!(
                                    "Rithmic market data subscriber lagged by {skipped} events; invalidating local books and recovering subscriptions"
                                );
                                is_connected.store(false, Ordering::Release);
                                order_books.clear();
                                order_book_delta_batches.clear();
                                // Re-issuing book subscriptions requests a fresh snapshot;
                                // other feeds are also refreshed so no stale venue intent remains.
                                match inner_for_reconnect.resubscribe_all().await {
                                    Ok(()) => is_connected.store(true, Ordering::Release),
                                    Err(e) => {
                                        log::error!("Rithmic lag recovery failed: {e}");
                                        is_connected.store(false, Ordering::Release);
                                        break;
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::info!("Rithmic market data channel closed");
                                is_connected.store(false, Ordering::Release);
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
        self.is_connected.store(true, Ordering::Release);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        // Send venue-side unsubscribes before tearing down the connection so the
        // venue stops pushing data (prevents stale data on reconnect).
        self.shutdown_tasks().await;

        if let Some(inner) = self.inner.take() {
            inner.unsubscribe_all_async().await;
        }

        if let Some(mut gateway) = self.gateway.take() {
            gateway.release().await;
        }
        self.clear_local_state();
        Ok(())
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
                    .unsubscribe_quotes_async(&symbol, &exchange)
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
                    .unsubscribe_trades_async(&symbol, &exchange)
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
        let (instrument_id, spec, aggregation_source) = match &cmd.bar_type {
            BarType::Standard {
                instrument_id,
                spec,
                aggregation_source,
            } => (*instrument_id, *spec, *aggregation_source),
            _ => anyhow::bail!(
                "Composite BarType is not supported for Rithmic: {}",
                cmd.bar_type
            ),
        };

        let period = u32::try_from(spec.step.get())
            .map_err(|_| anyhow::anyhow!("Rithmic bar step exceeds u32"))?;
        let gateway = self.require_gateway()?;
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let bar_type_map = Arc::clone(&self.bar_type_map);

        if spec.aggregation == BarAggregation::Tick {
            let inner = self.require_inner()?;
            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    inner
                        .subscribe_tick_bars(&symbol, &exchange, period)
                        .await
                        .map_err(anyhow::Error::from)?;
                    let key = rithmic_tick_bar_key(&exchange, &symbol, period);
                    let bar_type = BarType::new(
                        make_instrument_id(&symbol, &exchange).ok_or_else(|| {
                            anyhow::anyhow!("Invalid resolved Rithmic instrument identity")
                        })?,
                        spec,
                        aggregation_source,
                    );
                    bar_type_map.write().insert(key, bar_type);
                    Ok(())
                },
                "subscribe_tick_bars",
            );
            return Ok(());
        }

        let time_bar_type = bar_aggregation_to_time_bar_type(spec.aggregation)?;
        let time_period = i32::try_from(period)
            .map_err(|_| anyhow::anyhow!("Rithmic time-bar step exceeds i32"))?;
        let inner = self.require_inner()?;
        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .subscribe_bars(&symbol, &exchange, time_bar_type, time_period)
                    .await
                    .map_err(anyhow::Error::from)?;
                let key = rithmic_bar_key(&exchange, &symbol, time_bar_type, period);
                let bar_type = BarType::new(
                    make_instrument_id(&symbol, &exchange).ok_or_else(|| {
                        anyhow::anyhow!("Invalid resolved Rithmic instrument identity")
                    })?,
                    spec,
                    aggregation_source,
                );
                bar_type_map.write().insert(key, bar_type);
                Ok(())
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

        let period = u32::try_from(spec.step.get())
            .map_err(|_| anyhow::anyhow!("Rithmic bar step exceeds u32"))?;
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
                    inner
                        .unsubscribe_tick_bars(&symbol, &exchange, period)
                        .await
                        .map_err(anyhow::Error::from)?;
                    let key = rithmic_tick_bar_key(&exchange, &symbol, period);
                    bar_type_map.write().remove(&key);
                    Ok(())
                },
                "unsubscribe_tick_bars",
            );
            return Ok(());
        }

        let time_bar_type = bar_aggregation_to_time_bar_type(spec.aggregation)?;
        let time_period = i32::try_from(period)
            .map_err(|_| anyhow::anyhow!("Rithmic time-bar step exceeds i32"))?;

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                inner
                    .unsubscribe_bars(&symbol, &exchange, time_bar_type, time_period)
                    .await
                    .map_err(anyhow::Error::from)?;
                let key = rithmic_bar_key(&exchange, &symbol, time_bar_type, period);
                bar_type_map.write().remove(&key);
                Ok(())
            },
            "unsubscribe_bars",
        );
        Ok(())
    }

    fn request_trades(&self, cmd: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let start_sec = cmd
            .start
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Historical trade start exceeds Rithmic i32 range"))?
            .unwrap_or_default();
        let now_sec =
            i32::try_from(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000)
                .unwrap_or(i32::MAX);
        let requested_end = cmd
            .end
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Historical trade end exceeds Rithmic i32 range"))?
            .unwrap_or_default();
        let end_sec = normalize_history_request_end(start_sec, requested_end, now_sec);

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
                let instrument_id = make_instrument_id(&symbol, &exchange)
                    .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
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
                                let instruments = gateway
                                    .instruments()
                                    .try_read()
                                    .map_err(|e| anyhow::anyhow!("Instrument cache busy: {e}"))?;
                                checked_instrument_precisions(instruments.get(&key), &key)?
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
                    apply_auxiliary_reference_data(&mut instrument, aux).map_err(|e| {
                        anyhow::anyhow!("Failed to parse auxiliary reference data: {e}")
                    })?;
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
                let instrument_id = make_instrument_id(&symbol, &exchange)
                    .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
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
                    let instruments = gateway
                        .instruments()
                        .try_read()
                        .map_err(|e| anyhow::anyhow!("Instrument cache busy: {e}"))?;
                    checked_instrument_precisions(instruments.get(&key), &key)?
                };

                let book = order_book_from_snapshot(
                    instrument_id,
                    &responses,
                    price_precision,
                    size_precision,
                    now,
                )?;

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
                instrument_id (e.g. 'ESM5.CME.RITHMIC')"
            );
        } else if cmd.data_type.type_name() == VOLUME_AT_PRICE_TYPE_NAME {
            log::warn!(
                "RithmicVolumeAtPrice is request-only — use request_data with DataType identifier \
                set to the instrument_id (e.g. 'ESM5.CME.RITHMIC')"
            );
        } else if ExtraMarketDataKind::from_custom_type(cmd.data_type.type_name()).is_some() {
            let identifier = cmd.data_type.identifier().ok_or_else(|| {
                anyhow::anyhow!(
                    "subscribe({}): DataType must have an identifier equal to the instrument_id \
                    (e.g. 'ESM5.CME.RITHMIC')",
                    cmd.data_type.type_name()
                )
            })?;
            let instrument_id = InstrumentId::from_as_ref(identifier)
                .map_err(|e| anyhow::anyhow!("Invalid VolumeAtPrice instrument identifier: {e}"))?;
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
                    (e.g. 'ESM5.CME.RITHMIC')",
                    cmd.data_type.type_name()
                )
            })?;
            let instrument_id = InstrumentId::from_as_ref(identifier)
                .map_err(|e| anyhow::anyhow!("Invalid custom-data instrument identifier: {e}"))?;
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
                        instrument_id (e.g. 'ESM5.CME.RITHMIC')"
                    )
                })?
                .to_string();

            let instrument_id = InstrumentId::from_as_ref(&identifier)
                .map_err(|e| anyhow::anyhow!("Invalid VolumeAtPrice instrument identifier: {e}"))?;
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
                    let instrument_id = make_instrument_id(&symbol, &exchange)
                        .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
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
                    let mut ts_event = None;

                    for response in responses {
                        let RithmicMessage::ResponseGetVolumeAtPrice(vap) = response.message else {
                            continue;
                        };
                        if let Some(timestamp) = append_volume_at_price_response(
                            &vap,
                            &mut trade_price,
                            &mut volume_at_price,
                        )
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Malformed VolumeAtPrice response for {symbol}.{exchange}: {e}"
                            )
                        })? {
                            ts_event = Some(timestamp);
                        }
                    }
                    let ts_event = ts_event.ok_or_else(|| {
                        anyhow::anyhow!("VolumeAtPrice response contained no timestamped rows")
                    })?;

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
                    instrument_id (e.g. 'ESM5.CME.RITHMIC')"
                )
            })?
            .to_string();

        let instrument_id = InstrumentId::from_as_ref(&identifier)
            .map_err(|e| anyhow::anyhow!("Invalid volume-profile instrument identifier: {e}"))?;

        // Optional period in minutes — default 1.
        let period: i32 = request
            .data_type
            .metadata()
            .and_then(|m| m.get("period"))
            .and_then(|v| v.as_i64())
            .map(i32::try_from)
            .transpose()
            .map_err(|_| anyhow::anyhow!("Volume-profile period exceeds Rithmic i32 range"))?
            .unwrap_or(1);

        let start_sec = request
            .start
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Volume-profile start exceeds Rithmic i32 range"))?
            .unwrap_or_default();
        let end_sec = request
            .end
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Volume-profile end exceeds Rithmic i32 range"))?
            .unwrap_or_default();

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
                let instrument_id = make_instrument_id(&symbol, &exchange)
                    .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
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
                        volume_profile_bar_from_response(vp, instrument_id, now)
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
        let (instrument_id, spec, aggregation_source) = match &cmd.bar_type {
            BarType::Standard {
                instrument_id,
                spec,
                aggregation_source,
            } => (*instrument_id, *spec, *aggregation_source),
            _ => anyhow::bail!(
                "Composite BarType is not supported for Rithmic: {}",
                cmd.bar_type
            ),
        };

        if spec.aggregation == BarAggregation::Tick {
            let period = u32::try_from(spec.step.get())
                .map_err(|_| anyhow::anyhow!("Rithmic tick-bar step exceeds u32"))?;
            let start_sec = cmd
                .start
                .map(|dt| i32::try_from(dt.as_second()))
                .transpose()
                .map_err(|_| anyhow::anyhow!("Tick-bar history start exceeds i32 range"))?
                .unwrap_or_default();
            let now_sec =
                i32::try_from(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000)
                    .unwrap_or(i32::MAX);
            let requested_end = cmd
                .end
                .map(|dt| i32::try_from(dt.as_second()))
                .transpose()
                .map_err(|_| anyhow::anyhow!("Tick-bar history end exceeds i32 range"))?
                .unwrap_or_default();
            let end_sec = normalize_history_request_end(start_sec, requested_end, now_sec);

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

            self.spawn_ws(
                async move {
                    let (symbol, exchange) =
                        resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                            .await?;
                    let canonical_id = make_instrument_id(&symbol, &exchange)
                        .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
                    let bar_type = BarType::new(canonical_id, spec, aggregation_source);
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
                                        let instruments =
                                            gateway.instruments().try_read().map_err(|e| {
                                                anyhow::anyhow!("Instrument cache busy: {e}")
                                            })?;
                                        checked_instrument_precisions(instruments.get(&key), &key)?
                                    };

                                    responses
                                        .iter()
                                        .filter_map(|resp| {
                                            let RithmicMessage::ResponseTickBarReplay(tick) =
                                                &resp.message
                                            else {
                                                return None;
                                            };
                                            historical_tick_bar_to_bar(
                                                tick, bar_type, period, price_prec, size_prec, now,
                                            )
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
        let period = i32::try_from(spec.step.get())
            .map_err(|_| anyhow::anyhow!("Rithmic time-bar step exceeds i32"))?;

        let start_sec = cmd
            .start
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Time-bar history start exceeds i32 range"))?
            .unwrap_or_default();
        let now_sec =
            i32::try_from(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000)
                .unwrap_or(i32::MAX);
        let requested_end = cmd
            .end
            .map(|dt| i32::try_from(dt.as_second()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Time-bar history end exceeds i32 range"))?
            .unwrap_or_default();
        let end_sec = normalize_history_request_end(start_sec, requested_end, now_sec);
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

        self.spawn_ws(
            async move {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let canonical_id = make_instrument_id(&symbol, &exchange)
                    .ok_or_else(|| anyhow::anyhow!("Invalid resolved Rithmic instrument"))?;
                let bar_type = BarType::new(canonical_id, spec, aggregation_source);
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
                            let instruments = gateway
                                .instruments()
                                .try_read()
                                .map_err(|e| anyhow::anyhow!("Instrument cache busy: {e}"))?;
                            checked_instrument_precisions(instruments.get(&key), &key)?
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
        DepthByOrder, ResponseDepthByOrderSnapshot, ResponseGetVolumeAtPrice,
        ResponseTickBarReplay, ResponseTimeBarReplay, ResponseVolumeProfileMinuteBars,
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
            ts_event: rithmic_timestamp_to_unix_nanos(delta.ssboe, delta.usecs)
                .expect("fixture timestamp should be valid")
                .as_u64(),
            ts_init: rithmic_timestamp_to_unix_nanos(delta.ssboe, delta.usecs)
                .expect("fixture timestamp should be valid")
                .as_u64(),
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

    #[tokio::test]
    async fn connect_rejects_unvalidated_default_config_before_gateway_acquire() {
        let mut client = RithmicLiveDataClient::new(
            ClientId::new("RITHMIC-INVALID-CONFIG-TEST"),
            RithmicDataClientConfig::default(),
        );

        let e = client
            .connect()
            .await
            .expect_err("default config should fail validation");
        assert!(e.to_string().contains("username"));
        assert!(client.gateway.is_none());
        assert!(!client.is_connected());
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

        assert_eq!(deltas.instrument_id, InstrumentId::from("ESM6.CME.RITHMIC"));
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
        let bar_type = BarType::from("ESM6.CME.RITHMIC-1-MINUTE-LAST-EXTERNAL");
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
        let bar_type = BarType::from("ESM6.CME.RITHMIC-1-MINUTE-LAST-EXTERNAL");

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
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
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
        let trade_id = trade.trade_id.to_string();
        assert!(trade_id.starts_with('R'));
        assert_eq!(trade_id.len(), 33);
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
    fn historical_tick_bar_to_bar_requires_complete_ohlcv_and_timestamp() {
        let bar_type = BarType::from("ESM6.CME.RITHMIC-1-TICK-LAST-EXTERNAL");
        let complete = ResponseTickBarReplay {
            type_specifier: Some("1".to_string()),
            volume: Some(7),
            open_price: Some(4500.25),
            high_price: Some(4501.00),
            low_price: Some(4499.75),
            close_price: Some(4500.50),
            data_bar_ssboe: vec![1_700_000_123],
            data_bar_usecs: vec![456_789],
            ..Default::default()
        };
        assert!(
            historical_tick_bar_to_bar(&complete, bar_type, 1, 2, 0, UnixNanos::from(5),).is_some()
        );

        let mut incomplete = Vec::new();
        let mut value = complete.clone();
        value.open_price = None;
        incomplete.push(value);
        let mut value = complete.clone();
        value.high_price = None;
        incomplete.push(value);
        let mut value = complete.clone();
        value.low_price = None;
        incomplete.push(value);
        let mut value = complete.clone();
        value.close_price = None;
        incomplete.push(value);
        let mut value = complete.clone();
        value.volume = None;
        incomplete.push(value);
        let mut value = complete;
        value.data_bar_ssboe.clear();
        incomplete.push(value);

        for response in incomplete {
            assert!(
                historical_tick_bar_to_bar(&response, bar_type, 1, 2, 0, UnixNanos::from(5),)
                    .is_none()
            );
        }
    }

    #[rstest::rstest]
    fn volume_profile_bar_requires_complete_fields_and_valid_marker() {
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
        let complete = ResponseVolumeProfileMinuteBars {
            marker: Some(1_700_000_123),
            num_trades: Some(3),
            volume: Some(7),
            bid_volume: Some(4),
            ask_volume: Some(3),
            open_price: Some(4500.25),
            high_price: Some(4501.00),
            low_price: Some(4499.75),
            close_price: Some(4500.50),
            profile_price: vec![4500.25],
            profile_bid_volume: vec![4],
            profile_ask_volume: vec![3],
            ..Default::default()
        };
        assert!(
            volume_profile_bar_from_response(&complete, instrument_id, UnixNanos::from(5))
                .is_some()
        );

        let mut missing_marker = complete.clone();
        missing_marker.marker = None;
        assert!(
            volume_profile_bar_from_response(&missing_marker, instrument_id, UnixNanos::from(5),)
                .is_none()
        );

        let mut missing_volume = complete.clone();
        missing_volume.volume = None;
        assert!(
            volume_profile_bar_from_response(&missing_volume, instrument_id, UnixNanos::from(5),)
                .is_none()
        );

        let mut malformed_profile = complete;
        malformed_profile.profile_ask_volume.clear();
        assert!(
            volume_profile_bar_from_response(
                &malformed_profile,
                instrument_id,
                UnixNanos::from(5),
            )
            .is_none()
        );
    }

    #[rstest::rstest]
    fn volume_at_price_requires_aligned_valid_rows_and_venue_timestamp() {
        let complete = ResponseGetVolumeAtPrice {
            trade_price: vec![4500.25, 4500.50],
            volume_at_price: vec![4, 3],
            ssboe: Some(1_700_000_123),
            usecs: Some(456_789),
            ..Default::default()
        };
        let mut prices = Vec::new();
        let mut volumes = Vec::new();
        let timestamp = append_volume_at_price_response(&complete, &mut prices, &mut volumes)
            .expect("complete venue response should be accepted")
            .expect("nonempty response should carry its venue timestamp");
        assert_eq!(prices, complete.trade_price);
        assert_eq!(volumes, complete.volume_at_price);
        assert_eq!(timestamp, UnixNanos::from(1_700_000_123_456_789_000_u64));

        let mut malformed = complete.clone();
        malformed.volume_at_price.pop();
        assert!(
            append_volume_at_price_response(&malformed, &mut Vec::new(), &mut Vec::new()).is_err()
        );

        let mut malformed = complete.clone();
        malformed.trade_price[0] = f64::NAN;
        assert!(
            append_volume_at_price_response(&malformed, &mut Vec::new(), &mut Vec::new()).is_err()
        );

        let mut malformed = complete.clone();
        malformed.volume_at_price[0] = -1;
        assert!(
            append_volume_at_price_response(&malformed, &mut Vec::new(), &mut Vec::new()).is_err()
        );

        let mut missing_timestamp = complete;
        missing_timestamp.ssboe = None;
        assert!(
            append_volume_at_price_response(&missing_timestamp, &mut Vec::new(), &mut Vec::new(),)
                .is_err()
        );
    }

    #[rstest::rstest]
    fn historical_tick_bar_to_trade_drops_zero_timestamp_replay_rows() {
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
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

        let trade_id = live_trade_id(&tick).expect("fallback trade ID should be valid");
        assert_eq!(trade_id, live_trade_id(&tick).unwrap());
        assert!(trade_id.as_str().len() <= 36);

        let mut distinct = tick;
        distinct.exchange = "CBOT".to_string();
        assert_ne!(Some(trade_id), live_trade_id(&distinct));
    }

    #[rstest::rstest]
    fn live_trade_id_rejects_invalid_venue_id() {
        let tick = crate::data::TradeTick {
            symbol: "MNQM6".to_string(),
            exchange: "CME".to_string(),
            price: 25_572.25,
            size: 3.0,
            aggressor_side: "BUY".to_string(),
            trade_id: "invalid-💥-id".to_string(),
            price_precision: 2,
            size_precision: 0,
            ts_event: 1_700_000_123_456_789_000_u64,
            ts_init: 1_700_000_123_456_789_000_u64,
        };

        assert!(live_trade_id(&tick).is_none());
    }

    #[rstest::rstest]
    fn malformed_quote_numeric_payload_is_dropped() {
        let events = process_market_data_event(
            MarketDataEvent::Quote(crate::data::QuoteTick {
                symbol: "MNQM6".to_string(),
                exchange: "CME".to_string(),
                bid_price: f64::NAN,
                ask_price: 20_000.25,
                bid_size: 1.0,
                ask_size: f64::INFINITY,
                price_precision: 2,
                size_precision: 0,
                ts_event: 1,
                ts_init: 2,
            }),
            &ParkingRwLock::new(AHashMap::new()),
            &mut HashMap::new(),
            &mut HashMap::new(),
            &TestBookSubscriptions {
                wants_deltas: false,
                wants_depth10: false,
            },
        );

        assert!(events.is_empty());
    }

    #[rstest::rstest]
    fn legacy_exchange_less_input_resolves_to_canonical_event_identity() {
        let legacy_input = InstrumentId::from("MNQM6.RITHMIC");
        assert_eq!(
            parse_rithmic_instrument(&legacy_input),
            ("MNQM6".to_string(), String::new())
        );

        let canonical = make_instrument_id("MNQM6", "CME").unwrap();
        assert_eq!(canonical, InstrumentId::from("MNQM6.CME.RITHMIC"));
        assert_eq!(
            parse_rithmic_instrument(&canonical),
            ("MNQM6".to_string(), "CME".to_string())
        );

        let events = process_market_data_event(
            MarketDataEvent::Quote(crate::data::QuoteTick {
                symbol: "MNQM6".to_string(),
                exchange: "CME".to_string(),
                bid_price: 20_000.0,
                ask_price: 20_000.25,
                bid_size: 1.0,
                ask_size: 2.0,
                price_precision: 2,
                size_precision: 0,
                ts_event: 1,
                ts_init: 2,
            }),
            &ParkingRwLock::new(AHashMap::new()),
            &mut HashMap::new(),
            &mut HashMap::new(),
            &TestBookSubscriptions {
                wants_deltas: false,
                wants_depth10: false,
            },
        );
        let [DataEvent::Data(Data::Quote(quote))] = events.as_slice() else {
            panic!("expected one canonical quote event");
        };
        assert_eq!(quote.instrument_id, canonical);
    }

    #[rstest::rstest]
    fn resolved_exchange_cache_is_exchange_aware_and_rejects_ambiguous_aliases() {
        let cache = Arc::new(ParkingRwLock::new(AHashMap::new()));
        cache_resolved_exchange(&cache, "MYMZ6", "CBOT");
        assert_eq!(
            get_cached_exchange(&cache, "MYMZ6").as_deref(),
            Some("CBOT")
        );

        cache_resolved_exchange(&cache, "MYMZ6", "CME");
        assert_eq!(get_cached_exchange(&cache, "MYMZ6"), None);
        assert!(cache.read().contains_key("CBOT:MYMZ6"));
        assert!(cache.read().contains_key("CME:MYMZ6"));
    }

    #[rstest::rstest]
    fn exchange_resolution_requires_exactly_one_reference_match() {
        let instrument_id = InstrumentId::from("MYMZ6.RITHMIC");
        assert_eq!(
            select_unique_resolved_exchange(&instrument_id, &["CBOT".to_string()]).unwrap(),
            "CBOT"
        );
        assert!(select_unique_resolved_exchange(&instrument_id, &[]).is_err());

        let e = select_unique_resolved_exchange(
            &instrument_id,
            &["CBOT".to_string(), "CME".to_string()],
        )
        .unwrap_err();
        assert!(e.to_string().contains("Ambiguous exchange"));
        assert!(e.to_string().contains("CBOT, CME"));
    }

    #[rstest::rstest]
    fn normalize_time_history_bars_keeps_current_open_bar() {
        let bar_type = BarType::from("ESM6.CME.RITHMIC-1-MINUTE-LAST-EXTERNAL");
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
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
        let snapshot_rows: Vec<ResponseDepthByOrderSnapshot> = fixture
            .snapshots
            .into_iter()
            .map(SnapshotRowFixture::into_message)
            .collect();
        let snapshot_refs: Vec<&ResponseDepthByOrderSnapshot> = snapshot_rows.iter().collect();
        let ts_init = UnixNanos::from(1_700_000_100_500_000_000_u64);

        let mut book =
            order_book_from_snapshot_rows(instrument_id, &snapshot_refs, 2, 0, ts_init).unwrap();
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
    fn order_book_snapshot_distinguishes_valid_empty_from_missing_or_malformed() {
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
        let ts_init = UnixNanos::from(1_700_000_100_500_000_000_u64);
        assert!(order_book_from_snapshot_rows(instrument_id, &[], 2, 0, ts_init).is_err());

        let malformed = ResponseDepthByOrderSnapshot {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            sequence_number: None,
            ..Default::default()
        };
        assert!(
            order_book_from_snapshot_rows(instrument_id, &[&malformed], 2, 0, ts_init).is_err()
        );

        let valid_empty = ResponseDepthByOrderSnapshot {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            sequence_number: Some(101),
            ..Default::default()
        };
        let book =
            order_book_from_snapshot_rows(instrument_id, &[&valid_empty], 2, 0, ts_init).unwrap();
        let depth = depth10_from_order_book(&book, ts_init, ts_init);
        assert!(depth.bid_counts.iter().all(|count| *count == 0));
        assert!(depth.ask_counts.iter().all(|count| *count == 0));
        assert_eq!(book.sequence, 101);
    }

    #[rstest::rstest]
    fn order_book_price_move_uses_exchange_order_id_identity() {
        let instrument_id = InstrumentId::from("ESM6.CME.RITHMIC");
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

        let mut book =
            order_book_from_snapshot_rows(instrument_id, &snapshot_refs, 2, 0, ts_init).unwrap();

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
