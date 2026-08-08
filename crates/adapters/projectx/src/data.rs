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

//! Live market data client implementation for ProjectX.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use jiff::Timestamp;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, InstrumentResponse, RequestBars, RequestInstrument,
            SubscribeBookDeltas, SubscribeQuotes, SubscribeTrades, UnsubscribeBookDeltas,
            UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    Params, UnixNanos,
    correctness::{CorrectnessResultExt, FAILED},
    datetime::datetime_to_unix_nanos,
    time::get_atomic_clock_realtime,
};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, Data, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
        bar::get_bar_interval_ns,
    },
    enums::{AggressorSide, BarAggregation, BookAction, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use parking_lot::RwLock as ParkingRwLock;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::{Value, json};

use crate::{
    common::{
        consts::PROJECTX_VENUE,
        enums::ProjectXHub,
        symbols::{
            databento_to_projectx_symbol, projectx_contract_root, projectx_to_databento_symbol,
        },
    },
    config::ProjectXDataClientConfig,
    factories::{
        canonical_projectx_public_symbol, normalize_projectx_symbol_key,
        projectx_contract_to_instrument,
    },
    http::{client::ProjectXHttpClient, error::ProjectXHttpError},
    websocket::client::{ProjectXWsClient, ProjectXWsEvent},
};
use projectx_client::{
    Bar as PxApiBar, BarUnit, Contract, ContractId, DepthType, HistoryRequest, MarketDepth,
    MarketTrade, TradeLogType,
};

struct BarRequestSpec {
    contract_symbol: String,
    live: bool,
    unit: BarUnit,
    unit_number: i32,
    limit: i32,
    start: Timestamp,
    end: Timestamp,
}

type MappedBarRequest = (
    BarRequestSpec,
    bool,
    bool,
    ClientId,
    Option<UnixNanos>,
    Option<UnixNanos>,
);

fn provider_error_code(error: &ProjectXHttpError) -> Option<i32> {
    match error {
        ProjectXHttpError::Client(projectx_client::Error::Provider(provider_error)) => {
            Some(provider_error.code)
        }
        _ => None,
    }
}

fn build_history_request(
    request: &HistoryRequest,
    live: bool,
    start: Option<Timestamp>,
) -> anyhow::Result<HistoryRequest> {
    let start = start.unwrap_or_else(|| request.start_time().as_jiff().to_owned());
    HistoryRequest::builder(
        request.contract_id().clone(),
        live,
        start.into(),
        request.end_time(),
        request.unit(),
    )
    .unit_number(request.unit_number())
    .limit(request.limit())
    .include_partial_bar(true)
    .build()
    .map_err(anyhow::Error::from)
}

type ContractCache = Arc<ParkingRwLock<AHashMap<String, Contract>>>;

const MAX_HISTORICAL_BAR_PAGES: usize = 256;
const ALLOW_LIVE_HISTORY_FALLBACK_PARAM: &str = "allow_live_history_fallback";
const LIVE_HISTORY_FALLBACK_USED_PARAM: &str = "live_history_fallback_used";
const REQUESTED_LIVE_PARAM: &str = "requested_live";
const HISTORY_SOURCE_PARAM: &str = "history_source";
const HISTORY_SOURCE_LIVE_PARAM: &str = "history_source_live";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct DepthSlotKey {
    instrument_id: InstrumentId,
    side: u8,
    index: i32,
}

#[derive(Clone, Copy, Debug)]
struct DepthDeltaContext {
    seq: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct HistoricalBarsMetadata {
    live_history_fallback_used: bool,
    requested_live: bool,
    history_source_live: bool,
}

#[derive(Debug)]
pub struct ProjectXDataClient {
    client_id: ClientId,
    http_client: ProjectXHttpClient,
    market_data_live: bool,
    ws_market: Option<ProjectXWsClient>,
    ws_event_task: Option<tokio::task::JoinHandle<()>>,
    is_connected: AtomicBool,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    quote_subs: Arc<DashMap<String, InstrumentId>>,
    trade_subs: Arc<DashMap<String, InstrumentId>>,
    depth_subs: Arc<DashMap<String, InstrumentId>>,
    depth_sequence: Arc<DashMap<InstrumentId, u64>>,
    depth_ts_last: Arc<DashMap<InstrumentId, UnixNanos>>,
    depth_slots: Arc<DashMap<DepthSlotKey, Decimal>>,
    contracts_by_symbol_live: ContractCache,
    contracts_by_symbol_sim: ContractCache,
}

impl ProjectXDataClient {
    /// Creates a new [`ProjectXDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport client cannot be created.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(client_id: ClientId, config: ProjectXDataClientConfig) -> anyhow::Result<Self> {
        let http_client = ProjectXHttpClient::from_config(config.transport.clone())?;
        Ok(Self {
            client_id,
            http_client,
            market_data_live: config.market_data_live,
            ws_market: None,
            ws_event_task: None,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
            quote_subs: Arc::new(DashMap::new()),
            trade_subs: Arc::new(DashMap::new()),
            depth_subs: Arc::new(DashMap::new()),
            depth_sequence: Arc::new(DashMap::new()),
            depth_ts_last: Arc::new(DashMap::new()),
            depth_slots: Arc::new(DashMap::new()),
            contracts_by_symbol_live: Arc::new(ParkingRwLock::new(AHashMap::new())),
            contracts_by_symbol_sim: Arc::new(ParkingRwLock::new(AHashMap::new())),
        })
    }

    fn contract_from_symbol(symbol: &str) -> String {
        databento_to_projectx_symbol(symbol).unwrap_or_else(|_| symbol.to_string())
    }

    fn normalize_symbol_key(value: &str) -> String {
        normalize_projectx_symbol_key(value)
    }

    fn instrument_symbol_from_contract_id(contract_id: &str) -> String {
        projectx_to_databento_symbol(contract_id).unwrap_or_else(|_| contract_id.to_string())
    }

    fn canonical_public_symbol(contract: &Contract) -> String {
        canonical_projectx_public_symbol(contract)
    }

    fn market_trade_id(trade: &MarketTrade, ts_event: UnixNanos) -> TradeId {
        let mut hasher = DefaultHasher::new();
        trade.symbol_id.to_string().hash(&mut hasher);
        ts_event.as_u64().hash(&mut hasher);
        trade
            .price
            .to_f64()
            .unwrap_or_default()
            .to_bits()
            .hash(&mut hasher);
        trade.volume.hash(&mut hasher);
        trade.trade_type.code().hash(&mut hasher);
        TradeId::new(format!("PXM-{:016X}", hasher.finish()))
    }

    fn select_contract_cache<'a>(
        live: bool,
        contracts_by_symbol_live: &'a ContractCache,
        contracts_by_symbol_sim: &'a ContractCache,
    ) -> &'a ContractCache {
        if live {
            contracts_by_symbol_live
        } else {
            contracts_by_symbol_sim
        }
    }

    fn contract_cache(&self, live: bool) -> &ContractCache {
        Self::select_contract_cache(
            live,
            &self.contracts_by_symbol_live,
            &self.contracts_by_symbol_sim,
        )
    }

    fn clone_contract_cache(&self, live: bool) -> ContractCache {
        Arc::clone(self.contract_cache(live))
    }

    fn register_contract_mapping(
        map: &DashMap<String, InstrumentId>,
        contract_id: &str,
        instrument_id: InstrumentId,
    ) {
        map.insert(contract_id.to_string(), instrument_id);
        map.insert(projectx_contract_root(contract_id), instrument_id);
    }

    fn register_contract_aliases(
        map: &DashMap<String, InstrumentId>,
        contract: Option<&Contract>,
        instrument_id: InstrumentId,
    ) {
        map.insert(instrument_id.symbol.inner().to_string(), instrument_id);
        map.insert(
            projectx_contract_root(instrument_id.symbol.inner().as_str()),
            instrument_id,
        );

        if let Some(contract) = contract {
            Self::register_contract_mapping(map, contract.id.as_ref(), instrument_id);
            map.insert(Self::normalize_symbol_key(&contract.name), instrument_id);
            map.insert(
                Self::normalize_symbol_key(contract.symbol_id.as_ref()),
                instrument_id,
            );
        } else {
            let contract_id = Self::contract_from_symbol(instrument_id.symbol.inner().as_str());
            Self::register_contract_mapping(map, &contract_id, instrument_id);
        }
    }

    fn cache_contract_alias(
        contracts_by_symbol: &ContractCache,
        symbol: &str,
        contract: &Contract,
    ) {
        let key = Self::normalize_symbol_key(symbol);
        contracts_by_symbol.write().insert(key, contract.clone());
    }

    fn cache_contract_aliases(contracts_by_symbol: &ContractCache, contract: &Contract) {
        Self::cache_contract_alias(
            contracts_by_symbol,
            &Self::canonical_public_symbol(contract),
            contract,
        );
        Self::cache_contract_alias(contracts_by_symbol, &contract.name, contract);
        Self::cache_contract_alias(contracts_by_symbol, contract.symbol_id.as_ref(), contract);
        Self::cache_contract_alias(
            contracts_by_symbol,
            &Self::instrument_symbol_from_contract_id(contract.id.as_ref()),
            contract,
        );
    }

    fn contract_matches_symbol(contract: &Contract, symbol: &str) -> bool {
        let key = Self::normalize_symbol_key(symbol);
        Self::normalize_symbol_key(&contract.name) == key
            || Self::normalize_symbol_key(contract.symbol_id.as_ref()) == key
            || Self::normalize_symbol_key(&Self::instrument_symbol_from_contract_id(
                contract.id.as_ref(),
            )) == key
    }

    async fn resolve_contract_id_for_symbol(
        http: &ProjectXHttpClient,
        contracts_by_symbol: &ContractCache,
        symbol: &str,
        live: bool,
    ) -> String {
        if let Some(contract) = contracts_by_symbol
            .read()
            .get(&Self::normalize_symbol_key(symbol))
            .cloned()
        {
            return contract.id.to_string();
        }

        let fallback_contract_id = Self::contract_from_symbol(symbol);
        let search_text = Self::normalize_symbol_key(symbol);

        match http.search_contracts(live, &search_text).await {
            Ok(contracts) => {
                for contract in contracts {
                    if Self::contract_matches_symbol(&contract, symbol) {
                        let contract_id = contract.id.to_string();
                        Self::cache_contract_aliases(contracts_by_symbol, &contract);
                        return contract_id;
                    }
                }
            }
            Err(e) => {
                log::warn!("ProjectX contract search failed for symbol {symbol}: {e}");
            }
        }

        fallback_contract_id
    }

    async fn resolve_contract_for_symbol(
        http: &ProjectXHttpClient,
        contracts_by_symbol: &ContractCache,
        symbol: &str,
        live: bool,
    ) -> anyhow::Result<Contract> {
        if let Some(contract) = contracts_by_symbol
            .read()
            .get(&Self::normalize_symbol_key(symbol))
            .cloned()
        {
            return Ok(contract);
        }

        let contract_id =
            Self::resolve_contract_id_for_symbol(http, contracts_by_symbol, symbol, live).await;
        let contract = http.contract_by_id(&ContractId::new(&contract_id)?).await?;
        Self::cache_contract_aliases(contracts_by_symbol, &contract);
        Ok(contract)
    }

    fn remove_contract_mapping(map: &DashMap<String, InstrumentId>, contract_id: &str) {
        map.remove(contract_id);
        map.remove(&projectx_contract_root(contract_id));
    }

    fn resolve_instrument_id(
        map: &DashMap<String, InstrumentId>,
        primary: Option<&str>,
        fallback: Option<&str>,
    ) -> Option<InstrumentId> {
        if let Some(primary) = primary {
            if let Some(instrument_id) = map.get(primary).map(|v| *v) {
                return Some(instrument_id);
            }
            let root = projectx_contract_root(primary);

            if let Some(instrument_id) = map.get(&root).map(|v| *v) {
                return Some(instrument_id);
            }
        }

        if let Some(fallback) = fallback {
            if let Some(instrument_id) = map.get(fallback).map(|v| *v) {
                return Some(instrument_id);
            }
            let root = projectx_contract_root(fallback);

            if let Some(instrument_id) = map.get(&root).map(|v| *v) {
                return Some(instrument_id);
            }
        }

        if map.len() == 1 {
            return map.iter().next().map(|entry| *entry.value());
        }

        None
    }

    fn contract_to_instrument(contract: &Contract) -> anyhow::Result<InstrumentAny> {
        projectx_contract_to_instrument(contract)
    }

    async fn load_instruments(&self) -> anyhow::Result<()> {
        let contracts = self
            .http_client
            .available_contracts(self.market_data_live)
            .await?;

        let contracts_by_symbol = self.contract_cache(self.market_data_live);
        contracts_by_symbol.write().clear();
        let mut loaded = 0usize;

        for contract in contracts {
            Self::cache_contract_aliases(contracts_by_symbol, &contract);

            match Self::contract_to_instrument(&contract) {
                Ok(instrument) => {
                    if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                        log::warn!("ProjectX instrument send failed: {e}");
                    } else {
                        loaded += 1;
                    }
                }
                Err(e) => {
                    log::warn!("ProjectX instrument parse failed for {}: {e}", contract.id);
                }
            }
        }

        log::info!("ProjectX loaded {loaded} instrument(s) from contract catalog");
        Ok(())
    }

    fn parse_depth_side(depth_type: DepthType) -> Option<OrderSide> {
        match depth_type {
            DepthType::Ask | DepthType::BestAsk | DepthType::NewBestAsk => Some(OrderSide::Sell),
            DepthType::Bid | DepthType::BestBid | DepthType::NewBestBid => Some(OrderSide::Buy),
            _ => None,
        }
    }

    fn depth_slot_side_key(side: OrderSide) -> u8 {
        match side {
            OrderSide::Buy => 0,
            OrderSide::Sell => 1,
            _ => unreachable!("depth slot side must be buy or sell"),
        }
    }

    fn depth_slot_key(instrument_id: InstrumentId, side: OrderSide, index: i32) -> DepthSlotKey {
        DepthSlotKey {
            instrument_id,
            side: Self::depth_slot_side_key(side),
            index,
        }
    }

    fn depth_slot_index(depth: &MarketDepth) -> Option<i32> {
        depth.index.or(match depth.depth_type {
            // ProjectX can deliver top-of-book-only feeds without a depth index.
            // Track best-side updates against a synthetic top slot so the prior
            // best level is removed when the new best price arrives.
            DepthType::BestAsk
            | DepthType::BestBid
            | DepthType::NewBestBid
            | DepthType::NewBestAsk => Some(0),
            _ => None,
        })
    }

    fn clear_depth_slots_for_instrument(
        depth_slots: &DashMap<DepthSlotKey, Decimal>,
        instrument_id: InstrumentId,
    ) {
        depth_slots.retain(|key, _| key.instrument_id != instrument_id);
    }

    fn map_trade_aggressor(trade_type: TradeLogType) -> AggressorSide {
        match trade_type {
            TradeLogType::Buy => AggressorSide::Buyer,
            TradeLogType::Sell => AggressorSide::Seller,
            _ => AggressorSide::NoAggressor,
        }
    }

    fn depth_level_size(depth: &MarketDepth) -> f64 {
        // ProjectX emits both `volume` and `currentVolume`; for MBP state we prioritize the
        // current level quantity and only fall back to `volume` when current level is absent.

        if depth.current_volume > 0 {
            return depth.current_volume as f64;
        }

        if depth.current_volume == 0 && depth.volume > 0 {
            return depth.volume as f64;
        }

        0.0
    }

    fn next_depth_sequence(
        depth_sequence: &DashMap<InstrumentId, u64>,
        instrument_id: InstrumentId,
    ) -> u64 {
        let mut entry = depth_sequence.entry(instrument_id).or_insert(0);
        *entry += 1;
        *entry
    }

    fn clamp_depth_ts_event(
        depth_ts_last: &DashMap<InstrumentId, UnixNanos>,
        instrument_id: InstrumentId,
        ts_event: UnixNanos,
    ) -> UnixNanos {
        let mut entry = depth_ts_last.entry(instrument_id).or_insert(ts_event);
        let clamped = ts_event.max(*entry);
        *entry = clamped;
        clamped
    }

    fn build_depth_delta(
        instrument_id: InstrumentId,
        side: OrderSide,
        price: Decimal,
        size: f64,
        action: BookAction,
        is_last: bool,
        context: DepthDeltaContext,
    ) -> OrderBookDelta {
        let mut flags = RecordFlag::F_MBP as u8;
        if is_last {
            flags |= RecordFlag::F_LAST as u8;
        }

        let price_bits = price.to_f64().unwrap_or_default().to_bits();
        OrderBookDelta::new(
            instrument_id,
            action,
            BookOrder::new(
                side,
                Price::from_decimal(price).expect_display(FAILED),
                Quantity::new(size.max(0.0), 0),
                price_bits,
            ),
            flags,
            context.seq,
            context.ts_event,
            context.ts_init,
        )
    }

    fn map_depth_event(
        depth_slots: &DashMap<DepthSlotKey, Decimal>,
        instrument_id: InstrumentId,
        depth: &MarketDepth,
        seq: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Vec<OrderBookDelta> {
        if depth.depth_type == DepthType::Reset {
            Self::clear_depth_slots_for_instrument(depth_slots, instrument_id);
            return vec![OrderBookDelta::clear(instrument_id, seq, ts_event, ts_init)];
        }

        let Some(side) = Self::parse_depth_side(depth.depth_type) else {
            return Vec::new();
        };

        let level_size = Self::depth_level_size(depth);
        let depth_price = depth.price;
        let slot_key = Self::depth_slot_index(depth)
            .map(|index| Self::depth_slot_key(instrument_id, side, index));
        let context = DepthDeltaContext {
            seq,
            ts_event,
            ts_init,
        };
        let mut deltas = Vec::new();

        if level_size > 0.0 {
            if let Some(slot_key) = slot_key {
                if let Some(mut entry) = depth_slots.get_mut(&slot_key) {
                    let previous_price = *entry;
                    if previous_price != depth_price {
                        deltas.push(Self::build_depth_delta(
                            instrument_id,
                            side,
                            previous_price,
                            0.0,
                            BookAction::Delete,
                            false,
                            context,
                        ));
                    }
                    *entry = depth_price;
                } else {
                    depth_slots.insert(slot_key, depth_price);
                }
            }

            deltas.push(Self::build_depth_delta(
                instrument_id,
                side,
                depth_price,
                level_size,
                BookAction::Update,
                true,
                context,
            ));

            return deltas;
        }

        let delete_price = if let Some(slot_key) = slot_key {
            depth_slots
                .remove(&slot_key)
                .map_or(depth_price, |(_, price)| price)
        } else {
            depth_price
        };

        vec![Self::build_depth_delta(
            instrument_id,
            side,
            delete_price,
            0.0,
            BookAction::Delete,
            true,
            context,
        )]
    }

    fn map_bar_request(
        request: &RequestBars,
        client_id: ClientId,
        default_live: bool,
    ) -> anyhow::Result<MappedBarRequest> {
        let bar_type = request.bar_type;
        let spec = bar_type.spec();
        let instrument_id = bar_type.instrument_id();
        let params = request.params.as_ref();
        let step =
            i32::try_from(spec.step.get()).map_err(|_| anyhow::anyhow!("Invalid bar step"))?;

        let default_unit = match spec.aggregation {
            // ProjectX API enum: 1=Second, 2=Minute, 3=Hour, 4=Day, 5=Week, 6=Month.
            BarAggregation::Second => 1,
            BarAggregation::Minute => 2,
            BarAggregation::Hour => 3,
            BarAggregation::Day => 4,
            BarAggregation::Week => 5,
            BarAggregation::Month => 6,
            _ => anyhow::bail!(
                "Unsupported ProjectX bar aggregation: {:?}",
                spec.aggregation
            ),
        };

        let unit = params
            .and_then(|p| p.get_i64("unit"))
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(default_unit);
        let bar_unit = match unit {
            1 => BarUnit::Second,
            2 => BarUnit::Minute,
            3 => BarUnit::Hour,
            4 => BarUnit::Day,
            5 => BarUnit::Week,
            6 => BarUnit::Month,
            _ => anyhow::bail!("Unsupported ProjectX bar unit code: {unit}"),
        };
        let unit_number = params
            .and_then(|p| p.get_i64("unit_number"))
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(step);
        let limit = request
            .limit
            .map(|n| n.get() as i32)
            .or_else(|| {
                params
                    .and_then(|p| p.get_i64("limit"))
                    .and_then(|v| i32::try_from(v).ok())
            })
            .unwrap_or(500);
        let request_live = params
            .and_then(|p| p.get_bool("live"))
            .unwrap_or(default_live);
        let contract_live = params
            .and_then(|p| p.get_bool("contract_live"))
            .unwrap_or(default_live);
        let allow_live_history_fallback = params
            .and_then(|p| p.get_bool(ALLOW_LIVE_HISTORY_FALLBACK_PARAM))
            .unwrap_or(false);

        let start = request.start.unwrap_or_else(|| {
            Timestamp::now()
                .checked_sub(jiff::Span::new().days(1))
                .unwrap_or_else(|_| Timestamp::now())
        });
        let end = request.end.unwrap_or_else(Timestamp::now);
        let spec = BarRequestSpec {
            contract_symbol: Self::contract_from_symbol(instrument_id.symbol.inner().as_str()),
            live: request_live,
            unit: bar_unit,
            unit_number,
            limit,
            start,
            end,
        };

        Ok((
            spec,
            contract_live,
            allow_live_history_fallback,
            request.client_id.unwrap_or(client_id),
            datetime_to_unix_nanos(Some(start)),
            datetime_to_unix_nanos(Some(end)),
        ))
    }

    fn map_historical_bars(bar_type: BarType, bars: Vec<PxApiBar>) -> Vec<Bar> {
        let mut mapped = bars
            .into_iter()
            .filter_map(|bar| {
                let ts_event = u64::try_from(bar.t.as_jiff().as_nanosecond())
                    .ok()
                    .map(UnixNanos::from)?;
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
            })
            .collect::<Vec<_>>();
        mapped.sort_by_key(|bar| bar.ts_event);
        mapped.dedup_by_key(|bar| bar.ts_event);
        mapped
    }

    fn normalize_historical_bars(mut bars: Vec<Bar>) -> Vec<Bar> {
        bars.sort_by_key(|bar| bar.ts_event);
        bars.dedup_by_key(|bar| bar.ts_event);
        bars
    }

    fn next_historical_page_start(
        bar_type: BarType,
        bars: &[Bar],
        current_start_nanos: Option<UnixNanos>,
        end_nanos: Option<UnixNanos>,
    ) -> Option<UnixNanos> {
        let last_bar = bars.last()?;
        let next_start = last_bar
            .ts_event
            .checked_add(get_bar_interval_ns(&bar_type).as_u64())?;

        if let Some(current_start_nanos) = current_start_nanos
            && next_start <= current_start_nanos
        {
            return None;
        }

        if let Some(end_nanos) = end_nanos
            && next_start >= end_nanos
        {
            return None;
        }

        Some(next_start)
    }

    fn build_bars_response_params(
        params: Option<Params>,
        data_count: usize,
        error_code: Option<i32>,
        error_message: Option<&str>,
        metadata: HistoricalBarsMetadata,
    ) -> Params {
        let mut response_params = params.unwrap_or_default();
        response_params.insert("data_count".to_string(), json!(data_count));
        response_params.insert(
            LIVE_HISTORY_FALLBACK_USED_PARAM.to_string(),
            json!(metadata.live_history_fallback_used),
        );
        response_params.insert(
            REQUESTED_LIVE_PARAM.to_string(),
            json!(metadata.requested_live),
        );
        response_params.insert(
            HISTORY_SOURCE_PARAM.to_string(),
            json!(if metadata.history_source_live {
                "live"
            } else {
                "sim"
            }),
        );
        response_params.insert(
            HISTORY_SOURCE_LIVE_PARAM.to_string(),
            json!(metadata.history_source_live),
        );

        if let Some(code) = error_code {
            response_params.insert("error_code".to_string(), json!(code));
        }

        if let Some(message) = error_message {
            response_params.insert("error_message".to_string(), json!(message));
        }
        response_params
    }

    fn should_retry_live_history_with_sim(
        request: &HistoryRequest,
        error_code: Option<i32>,
        allow_live_history_fallback: bool,
    ) -> bool {
        allow_live_history_fallback && request.is_live() && error_code == Some(1)
    }

    async fn retrieve_bars_page(
        http: &ProjectXHttpClient,
        request: &HistoryRequest,
        instrument_symbol: &str,
        allow_live_history_fallback: bool,
    ) -> Result<(Vec<PxApiBar>, HistoricalBarsMetadata), ProjectXHttpError> {
        match http.retrieve_bars(request).await {
            Err(e)
                if Self::should_retry_live_history_with_sim(
                    request,
                    provider_error_code(&e),
                    allow_live_history_fallback,
                ) =>
            {
                log::info!(
                    "ProjectX live historical bars request rejected for {instrument_symbol}: {e}; retrying with live=false because {ALLOW_LIVE_HISTORY_FALLBACK_PARAM}=true",
                );
                let fallback_request = HistoryRequest::builder(
                    request.contract_id().clone(),
                    false,
                    request.start_time(),
                    request.end_time(),
                    request.unit(),
                )
                .unit_number(request.unit_number())
                .limit(request.limit())
                .include_partial_bar(true)
                .build()
                .map_err(anyhow::Error::from)?;
                http.retrieve_bars(&fallback_request).await.map(|response| {
                    (
                        response,
                        HistoricalBarsMetadata {
                            live_history_fallback_used: true,
                            requested_live: true,
                            history_source_live: false,
                        },
                    )
                })
            }
            Ok(response) => Ok((
                response,
                HistoricalBarsMetadata {
                    live_history_fallback_used: false,
                    requested_live: request.is_live(),
                    history_source_live: request.is_live(),
                },
            )),
            Err(e) => Err(e),
        }
    }

    fn send_bars_response(
        sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
        context: &BarsResponseContext,
        bars: Vec<Bar>,
        params: Params,
    ) {
        let response = DataResponse::Bars(BarsResponse::new(
            context.request_id,
            context.client_id,
            context.bar_type,
            bars,
            context.start_nanos,
            context.end_nanos,
            get_atomic_clock_realtime().get_time_ns(),
            Some(params),
        ));

        if let Err(e) = sender.send(DataEvent::Response(response)) {
            log::warn!("ProjectX bars response send failed: {e}");
        }
    }

    fn spawn_ws_event_task(&mut self, ws_market: &ProjectXWsClient) {
        let ws_market = ws_market.clone();
        let sender = self.data_sender.clone();
        let quote_subs = Arc::clone(&self.quote_subs);
        let trade_subs = Arc::clone(&self.trade_subs);
        let depth_subs = Arc::clone(&self.depth_subs);
        let depth_sequence = Arc::clone(&self.depth_sequence);
        let depth_ts_last = Arc::clone(&self.depth_ts_last);
        let depth_slots = Arc::clone(&self.depth_slots);

        let handle = get_runtime().spawn(async move {
            let Some(mut rx) = ws_market.take_event_receiver().await else {
                return;
            };

            while let Some(event) = rx.recv().await {
                match event {
                    ProjectXWsEvent::MarketQuote(quote) => {
                        let instrument_id = Self::resolve_instrument_id(
                            &quote_subs,
                            Some(quote.raw_symbol.as_ref()),
                            quote.symbol_name.as_deref(),
                        );
                        let Some(instrument_id) = instrument_id else {
                            continue;
                        };

                        let ts_event = quote
                            .timestamp
                            .and_then(|ts| {
                                u64::try_from(ts.as_jiff().as_nanosecond())
                                    .ok()
                                    .map(UnixNanos::from)
                            })
                            .or_else(|| {
                                u64::try_from(quote.last_updated.as_jiff().as_nanosecond())
                                    .ok()
                                    .map(UnixNanos::from)
                            })
                            .unwrap_or_else(|| get_atomic_clock_realtime().get_time_ns());
                        let best_bid = quote.best_bid.unwrap_or_default();
                        let best_ask = quote.best_ask.unwrap_or_default();
                        let px_precision = best_bid.scale().max(best_ask.scale()) as u8;
                        let quote_tick = QuoteTick::new(
                            instrument_id,
                            Price::from_decimal_dp(best_bid, px_precision).expect_display(FAILED),
                            Price::from_decimal_dp(best_ask, px_precision).expect_display(FAILED),
                            Quantity::new(0.0, 0),
                            Quantity::new(0.0, 0),
                            ts_event,
                            get_atomic_clock_realtime().get_time_ns(),
                        );
                        let _ = sender.send(DataEvent::Data(Data::Quote(quote_tick)));
                    }
                    ProjectXWsEvent::MarketTrade(trade) => {
                        let instrument_id = Self::resolve_instrument_id(
                            &trade_subs,
                            Some(trade.symbol_id.as_ref()),
                            None,
                        );
                        let Some(instrument_id) = instrument_id else {
                            continue;
                        };

                        if trade.volume <= 0 {
                            continue;
                        }

                        let ts_event = u64::try_from(trade.timestamp.as_jiff().as_nanosecond())
                            .ok()
                            .map_or_else(
                                || get_atomic_clock_realtime().get_time_ns(),
                                UnixNanos::from,
                            );
                        let price = trade.price;
                        let tick = TradeTick::new(
                            instrument_id,
                            Price::from_decimal(price).expect_display(FAILED),
                            Quantity::new(trade.volume as f64, 0),
                            Self::map_trade_aggressor(trade.trade_type),
                            Self::market_trade_id(&trade, ts_event),
                            ts_event,
                            get_atomic_clock_realtime().get_time_ns(),
                        );
                        let _ = sender.send(DataEvent::Data(Data::Trade(tick)));
                    }
                    ProjectXWsEvent::MarketDepth(depth) => {
                        let instrument_id = Self::resolve_instrument_id(
                            &depth_subs,
                            depth.symbol_id.as_ref().map(ToString::to_string).as_deref(),
                            None,
                        );
                        let Some(instrument_id) = instrument_id else {
                            continue;
                        };

                        let raw_ts_event = u64::try_from(depth.timestamp.as_jiff().as_nanosecond())
                            .ok()
                            .map_or_else(
                                || get_atomic_clock_realtime().get_time_ns(),
                                UnixNanos::from,
                            );
                        let ts_event =
                            Self::clamp_depth_ts_event(&depth_ts_last, instrument_id, raw_ts_event);
                        let seq = Self::next_depth_sequence(&depth_sequence, instrument_id);
                        let ts_init = get_atomic_clock_realtime().get_time_ns();
                        let deltas = Self::map_depth_event(
                            &depth_slots,
                            instrument_id,
                            &depth,
                            seq,
                            ts_event,
                            ts_init,
                        );

                        if !deltas.is_empty() {
                            let grouped = OrderBookDeltas::new(instrument_id, deltas);
                            let _ = sender.send(DataEvent::Data(Data::Deltas(Box::new(grouped))));
                        }
                    }
                    _ => {}
                }
            }
        });
        self.ws_event_task = Some(handle);
    }

    fn find_contract(&self, symbol: &str) -> Option<Contract> {
        let key = Self::normalize_symbol_key(symbol);
        self.contract_cache(self.market_data_live)
            .read()
            .get(&key)
            .cloned()
    }

    fn resolve_unsubscribe_contract_id_from_cache(
        contracts_by_symbol: &ContractCache,
        public_symbol: &str,
    ) -> String {
        let key = Self::normalize_symbol_key(public_symbol);
        contracts_by_symbol.read().get(&key).cloned().map_or_else(
            || Self::contract_from_symbol(public_symbol),
            |entry| entry.id.to_string(),
        )
    }

    fn resolve_unsubscribe_contract_id(&self, public_symbol: &str) -> String {
        Self::resolve_unsubscribe_contract_id_from_cache(
            self.contract_cache(self.market_data_live),
            public_symbol,
        )
    }
}

struct BarsResponseContext {
    request_id: nautilus_core::UUID4,
    client_id: ClientId,
    bar_type: BarType,
    start_nanos: Option<UnixNanos>,
    end_nanos: Option<UnixNanos>,
}

#[async_trait(?Send)]
impl DataClient for ProjectXDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*PROJECTX_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.ws_event_task.take() {
            task.abort();
        }

        if let Some(ws_market) = self.ws_market.take() {
            get_runtime().spawn(async move {
                let _ = ws_market.disconnect().await;
            });
        }
        self.http_client.stop();
        self.is_connected.store(false, Ordering::Release);
        self.depth_slots.clear();
        self.depth_sequence.clear();
        self.depth_ts_last.clear();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        self.http_client.start().await?;
        self.load_instruments().await?;
        let ws_market = self.http_client.create_ws_client(ProjectXHub::Market);
        ws_market.connect().await?;
        self.spawn_ws_event_task(&ws_market);
        self.ws_market = Some(ws_market);
        self.is_connected.store(true, Ordering::Release);
        log::info!("ProjectX data client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.ws_event_task.take() {
            task.abort();
        }

        if let Some(ws_market) = &self.ws_market {
            ws_market.disconnect().await?;
        }
        self.ws_market = None;
        self.http_client.stop();
        self.is_connected.store(false, Ordering::Release);
        self.depth_slots.clear();
        self.depth_sequence.clear();
        self.depth_ts_last.clear();
        log::info!("ProjectX data client disconnected");
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            anyhow::bail!("ProjectX market websocket not connected");
        };

        let ws_market = ws_market.clone();
        let http = self.http_client.clone();
        let contracts_by_symbol = Arc::clone(self.contract_cache(self.market_data_live));
        let market_data_live = self.market_data_live;
        let public_symbol = cmd.instrument_id.symbol.inner().to_string();
        let contract = self.find_contract(&public_symbol);
        Self::register_contract_aliases(&self.quote_subs, contract.as_ref(), cmd.instrument_id);

        get_runtime().spawn(async move {
            let contract_id = if let Some(contract) = contract {
                contract.id.to_string()
            } else {
                Self::resolve_contract_id_for_symbol(
                    &http,
                    &contracts_by_symbol,
                    &public_symbol,
                    market_data_live,
                )
                .await
            };

            if let Err(e) = ws_market
                .invoke(
                    "SubscribeContractQuotes",
                    vec![Value::String(contract_id)],
                    true,
                )
                .await
            {
                log::warn!("ProjectX subscribe quotes failed: {e}");
            }
        });
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            anyhow::bail!("ProjectX market websocket not connected");
        };

        let ws_market = ws_market.clone();
        let http = self.http_client.clone();
        let contracts_by_symbol = Arc::clone(self.contract_cache(self.market_data_live));
        let market_data_live = self.market_data_live;
        let public_symbol = cmd.instrument_id.symbol.inner().to_string();
        let contract = self.find_contract(&public_symbol);
        Self::register_contract_aliases(&self.trade_subs, contract.as_ref(), cmd.instrument_id);

        get_runtime().spawn(async move {
            let contract_id = if let Some(contract) = contract {
                contract.id.to_string()
            } else {
                Self::resolve_contract_id_for_symbol(
                    &http,
                    &contracts_by_symbol,
                    &public_symbol,
                    market_data_live,
                )
                .await
            };

            if let Err(e) = ws_market
                .invoke(
                    "SubscribeContractTrades",
                    vec![Value::String(contract_id)],
                    true,
                )
                .await
            {
                log::warn!("ProjectX subscribe trades failed: {e}");
            }
        });
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            anyhow::bail!("ProjectX market websocket not connected");
        };

        let ws_market = ws_market.clone();
        let http = self.http_client.clone();
        let contracts_by_symbol = Arc::clone(self.contract_cache(self.market_data_live));
        let market_data_live = self.market_data_live;
        let public_symbol = cmd.instrument_id.symbol.inner().to_string();
        let contract = self.find_contract(&public_symbol);
        Self::register_contract_aliases(&self.depth_subs, contract.as_ref(), cmd.instrument_id);

        get_runtime().spawn(async move {
            let contract_id = if let Some(contract) = contract {
                contract.id.to_string()
            } else {
                Self::resolve_contract_id_for_symbol(
                    &http,
                    &contracts_by_symbol,
                    &public_symbol,
                    market_data_live,
                )
                .await
            };

            if let Err(e) = ws_market
                .invoke(
                    "SubscribeContractMarketDepth",
                    vec![Value::String(contract_id)],
                    true,
                )
                .await
            {
                log::warn!("ProjectX subscribe depth failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            return Ok(());
        };

        let ws_market = ws_market.clone();
        let contract_id =
            self.resolve_unsubscribe_contract_id(cmd.instrument_id.symbol.inner().as_str());
        Self::remove_contract_mapping(&self.quote_subs, &contract_id);

        get_runtime().spawn(async move {
            let tracked_args = vec![Value::String(contract_id.clone())];

            if let Err(e) = ws_market
                .unsubscribe(
                    "UnsubscribeContractQuotes",
                    "SubscribeContractQuotes",
                    &tracked_args,
                    vec![Value::String(contract_id)],
                )
                .await
            {
                log::warn!("ProjectX unsubscribe quotes failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            return Ok(());
        };

        let ws_market = ws_market.clone();
        let contract_id =
            self.resolve_unsubscribe_contract_id(cmd.instrument_id.symbol.inner().as_str());
        Self::remove_contract_mapping(&self.trade_subs, &contract_id);

        get_runtime().spawn(async move {
            let tracked_args = vec![Value::String(contract_id.clone())];

            if let Err(e) = ws_market
                .unsubscribe(
                    "UnsubscribeContractTrades",
                    "SubscribeContractTrades",
                    &tracked_args,
                    vec![Value::String(contract_id)],
                )
                .await
            {
                log::warn!("ProjectX unsubscribe trades failed: {e}");
            }
        });
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let Some(ws_market) = &self.ws_market else {
            return Ok(());
        };

        let ws_market = ws_market.clone();
        let contract_id =
            self.resolve_unsubscribe_contract_id(cmd.instrument_id.symbol.inner().as_str());
        Self::remove_contract_mapping(&self.depth_subs, &contract_id);
        self.depth_sequence.remove(&cmd.instrument_id);
        self.depth_ts_last.remove(&cmd.instrument_id);
        Self::clear_depth_slots_for_instrument(&self.depth_slots, cmd.instrument_id);

        get_runtime().spawn(async move {
            let tracked_args = vec![Value::String(contract_id.clone())];

            if let Err(e) = ws_market
                .unsubscribe(
                    "UnsubscribeContractMarketDepth",
                    "SubscribeContractMarketDepth",
                    &tracked_args,
                    vec![Value::String(contract_id)],
                )
                .await
            {
                log::warn!("ProjectX unsubscribe depth failed: {e}");
            }
        });
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let bar_type = request.bar_type;
        let instrument_symbol = bar_type.instrument_id().symbol.inner().to_string();
        let request_id = request.request_id;
        let params = request.params.clone();
        let (
            api_request,
            contract_live,
            allow_live_history_fallback,
            client_id,
            start_nanos,
            end_nanos,
        ) = Self::map_bar_request(&request, self.client_id, self.market_data_live)?;
        let contracts_by_symbol = self.clone_contract_cache(contract_live);

        get_runtime().spawn(async move {
            let contract_id = Self::resolve_contract_id_for_symbol(
                &http,
                &contracts_by_symbol,
                &api_request.contract_symbol,
                contract_live,
            )
            .await;
            let Ok(contract_id) = ContractId::new(contract_id) else {
                log::warn!("ProjectX invalid contract id for {instrument_symbol}");
                return;
            };
            let mut page_request = match HistoryRequest::builder(
                contract_id,
                api_request.live,
                api_request.start.into(),
                api_request.end.into(),
                api_request.unit,
            )
            .unit_number(api_request.unit_number)
            .limit(api_request.limit)
            .include_partial_bar(true)
            .build()
            {
                Ok(request) => request,
                Err(e) => {
                    log::warn!("ProjectX invalid history request: {e}");
                    return;
                }
            };
            let response_context = BarsResponseContext {
                request_id,
                client_id,
                bar_type,
                start_nanos,
                end_nanos,
            };

            let mut current_start_nanos = start_nanos;
            let mut bars = Vec::new();
            let mut page_count = 0usize;
            let error_code = None;
            let mut error_message: Option<String> = None;
            let mut response_metadata = HistoricalBarsMetadata {
                requested_live: page_request.is_live(),
                history_source_live: page_request.is_live(),
                ..HistoricalBarsMetadata::default()
            };

            loop {
                if page_count >= MAX_HISTORICAL_BAR_PAGES {
                    error_message = Some(format!(
                        "ProjectX historical bars request exceeded {MAX_HISTORICAL_BAR_PAGES} pages"
                    ));
                    log::warn!("{}", error_message.as_deref().unwrap_or_default());
                    break;
                }
                page_count += 1;

                match Self::retrieve_bars_page(
                    &http,
                    &page_request,
                    &instrument_symbol,
                    allow_live_history_fallback,
                )
                .await
                {
                    Ok((page_bars, metadata)) => {
                        if metadata.live_history_fallback_used {
                            response_metadata.live_history_fallback_used = true;
                            response_metadata.history_source_live = false;
                            match build_history_request(&page_request, false, None) {
                                Ok(request) => page_request = request,
                                Err(e) => {
                                    log::warn!("ProjectX history request rebuild failed: {e}");
                                    break;
                                }
                            }
                        }
                        let page_bars = Self::normalize_historical_bars(
                            Self::map_historical_bars(bar_type, page_bars),
                        );

                        if page_bars.is_empty() {
                            break;
                        }

                        let next_start = Self::next_historical_page_start(
                            bar_type,
                            &page_bars,
                            current_start_nanos,
                            end_nanos,
                        );
                        bars.extend(page_bars);
                        bars.sort_by_key(|bar| bar.ts_event);
                        bars.dedup_by_key(|bar| bar.ts_event);

                        let Some(next_start) = next_start else {
                            if let (Some(last_bar), Some(end_nanos)) = (bars.last(), end_nanos)
                                && last_bar
                                    .ts_event
                                    .checked_add(get_bar_interval_ns(&bar_type).as_u64())
                                    .is_some_and(|bar_close| bar_close < end_nanos)
                            {
                                log::warn!(
                                    "ProjectX historical bars request stalled before requested end for {instrument_symbol}"
                                );
                            }
                            break;
                        };

                        current_start_nanos = Some(next_start);
                        let next_start_ts = Timestamp::from_nanosecond(i128::from(
                            next_start.as_u64(),
                        ))
                        .unwrap_or(api_request.end);
                        match build_history_request(
                            &page_request,
                            page_request.is_live(),
                            Some(next_start_ts),
                        ) {
                            Ok(request) => page_request = request,
                            Err(e) => {
                                log::warn!("ProjectX history request rebuild failed: {e}");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("ProjectX bars request failed: {e}");
                        error_message = Some(e.to_string());
                        break;
                    }
                }
            }

            let response_params = Self::build_bars_response_params(
                params.clone(),
                bars.len(),
                error_code,
                error_message.as_deref(),
                response_metadata,
            );
            Self::send_bars_response(&sender, &response_context, bars, response_params);
        });

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let contracts_by_symbol = Arc::clone(self.contract_cache(self.market_data_live));
        let sender = self.data_sender.clone();
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let params = request.params;
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let market_data_live = self.market_data_live;

        get_runtime().spawn(async move {
            match Self::resolve_contract_for_symbol(
                &http,
                &contracts_by_symbol,
                instrument_id.symbol.inner().as_str(),
                market_data_live,
            )
            .await
            .and_then(|contract| Self::contract_to_instrument(&contract))
            {
                Ok(instrument) => {
                    let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                        request_id,
                        client_id,
                        instrument.id(),
                        instrument,
                        start_nanos,
                        end_nanos,
                        get_atomic_clock_realtime().get_time_ns(),
                        params,
                    )));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::warn!("ProjectX instrument response send failed: {e}");
                    }
                }
                Err(e) => {
                    log::warn!("ProjectX instrument request failed for {instrument_id}: {e}");
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroUsize, sync::Arc};

    use ahash::AHashMap;
    use dashmap::DashMap;
    use jiff::civil::date;
    use nautilus_common::messages::data::RequestBars;
    use nautilus_core::{Params, UUID4, UnixNanos};
    use nautilus_model::{
        data::BarType,
        enums::{BookAction, OrderSide, RecordFlag},
        identifiers::{ClientId, InstrumentId},
    };
    use parking_lot::RwLock as ParkingRwLock;
    use serde_json::{Value, json};

    use super::{
        ALLOW_LIVE_HISTORY_FALLBACK_PARAM, HISTORY_SOURCE_LIVE_PARAM, HISTORY_SOURCE_PARAM,
        HistoricalBarsMetadata, LIVE_HISTORY_FALLBACK_USED_PARAM, ProjectXDataClient,
        REQUESTED_LIVE_PARAM,
    };
    use projectx_client::{
        Bar, Contract, ContractId, DepthType, HistoryRequest, MarketDepth, MarketQuote,
        MarketTrade, SymbolId, Timestamp,
    };
    use rust_decimal::Decimal;

    fn ts(value: &str) -> Timestamp {
        Timestamp::new(value).expect("valid timestamp")
    }

    fn px_bar(t: &str, o: f64, h: f64, l: f64, c: f64, v: i64) -> Bar {
        serde_json::from_value(json!({
            "t": t,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
            "v": v,
        }))
        .expect("bar")
    }

    fn contract(id: &str, name: &str, active: bool) -> Contract {
        serde_json::from_value(json!({
            "id": id,
            "name": name,
            "description": "Micro E-mini S&P 500",
            "tickSize": 0.25,
            "tickValue": 1.25,
            "activeContract": active,
            "symbolId": "MES",
        }))
        .expect("contract")
    }

    fn load_fixture(name: &str) -> Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join(name);
        let bytes =
            fs::read(&path).unwrap_or_else(|e| panic!("failed reading fixture {path:?}: {e}"));
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("failed parsing fixture {path:?}: {e}"))
    }

    #[rstest::rstest]
    fn signalr_invocation_decodes_market_trade_batch() {
        let value = json!({
            "type": 1,
            "target": "GatewayTrade",
            "arguments": ["MESM6", [
                {
                    "symbolId": "MESM6",
                    "price": 5200.25,
                    "timestamp": "2026-04-02T00:00:00Z",
                    "type": 0,
                    "volume": 1
                }
            ]]
        });
        let invocation = projectx_client::SignalRInvocation::from_value(value)
            .expect("invocation")
            .expect("type-1 frame");
        let decoded = invocation.decode_batch::<MarketTrade>();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].as_ref().unwrap().symbol_id.to_string(), "MESM6");
        assert_eq!(decoded[0].as_ref().unwrap().volume, 1);
    }

    #[rstest::rstest]
    fn signalr_invocation_decodes_market_quote_symbol_alias() {
        let value = json!({
            "type": 1,
            "target": "GatewayQuote",
            "arguments": [{
                "symbol": "MESM6",
                "bestBid": 5200.0,
                "bestAsk": 5200.25,
                "lastPrice": 5200.0,
                "change": 0.0,
                "changePercent": 0.0,
                "volume": 1,
                "lastUpdated": "2026-04-02T00:00:00Z",
                "timestamp": "2026-04-02T00:00:00Z"
            }]
        });
        let invocation = projectx_client::SignalRInvocation::from_value(value)
            .expect("invocation")
            .expect("type-1 frame");
        let decoded = invocation.decode_batch::<MarketQuote>();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].as_ref().unwrap().raw_symbol.to_string(), "MESM6");
    }

    #[rstest::rstest]
    fn signalr_invocation_decodes_market_depth_symbol_id_fallback() {
        let value = json!({
            "type": 1,
            "target": "GatewayDepth",
            "arguments": [{
                "symbolId": "CON.F.US.MES.M26",
                "timestamp": "2026-04-02T00:00:00Z",
                "type": 2,
                "price": 5200.0,
                "volume": 1,
                "currentVolume": 3
            }]
        });
        let invocation = projectx_client::SignalRInvocation::from_value(value)
            .expect("invocation")
            .expect("type-1 frame");
        let decoded = invocation.decode_batch::<MarketDepth>();
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0]
                .as_ref()
                .unwrap()
                .symbol_id
                .as_ref()
                .unwrap()
                .to_string(),
            "CON.F.US.MES.M26",
        );
    }

    #[rstest::rstest]
    fn projectx_market_data_fixtures_decode_into_supported_payload_types() {
        let fixture = load_fixture("market_data_events.json");

        let quote: MarketQuote = serde_json::from_value(fixture["quote"].clone()).expect("quote");
        let trade: MarketTrade = serde_json::from_value(fixture["trade"].clone()).expect("trade");
        let depth: MarketDepth = serde_json::from_value(fixture["depth"].clone()).expect("depth");

        assert_eq!(quote.raw_symbol.to_string(), "MNQM6");
        assert_eq!(trade.symbol_id.to_string(), "CON.F.US.MNQ.M26");
        assert_eq!(depth.current_volume, 7);
    }

    #[rstest::rstest]
    fn projectx_historical_bars_fixture_maps_into_sorted_bars() {
        let fixture = load_fixture("historical_bars_response.json");
        let bars: Vec<Bar> = serde_json::from_value(fixture["bars"].clone())
            .expect("historical bars fixture should deserialize");
        let bar_type = BarType::from("MNQM26.PROJECTX-1-MINUTE-LAST-EXTERNAL");

        let bars = ProjectXDataClient::map_historical_bars(bar_type, bars);

        assert_eq!(bars.len(), 2);
        assert!(bars[0].ts_event < bars[1].ts_event);
        assert_eq!(bars[0].close.as_f64(), 21214.0);
        assert_eq!(bars[1].close.as_f64(), 21217.5);
    }

    #[rstest::rstest]
    fn signalr_invocation_from_value_handles_invocation_frames() {
        let value = json!({
            "type": 1,
            "target": "GatewayQuote",
            "arguments": [{"rawSymbol":"MESM6"}]
        });
        let invocation = projectx_client::SignalRInvocation::from_value(value)
            .expect("invocation frame should parse")
            .expect("type-1 frame");
        assert_eq!(invocation.target(), "GatewayQuote");
    }

    #[rstest::rstest]
    fn parse_depth_side_maps_known_codes() {
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::Ask),
            Some(OrderSide::Sell)
        );
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::Bid),
            Some(OrderSide::Buy)
        );
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::BestAsk),
            Some(OrderSide::Sell)
        );
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::BestBid),
            Some(OrderSide::Buy)
        );
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::NewBestBid),
            Some(OrderSide::Buy)
        );
        assert_eq!(
            ProjectXDataClient::parse_depth_side(DepthType::NewBestAsk),
            Some(OrderSide::Sell)
        );
        assert_eq!(ProjectXDataClient::parse_depth_side(DepthType::Trade), None);
    }

    #[rstest::rstest]
    fn depth_level_size_prefers_current_volume_with_safe_fallback() {
        let with_current: MarketDepth = serde_json::from_value(json!({
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:00Z",
            "type": 2,
            "price": 5200.0,
            "volume": 1,
            "currentVolume": 5,
            "index": 1
        }))
        .expect("depth");
        assert_eq!(ProjectXDataClient::depth_level_size(&with_current), 5.0);

        let fallback_volume: MarketDepth = serde_json::from_value(json!({
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:00Z",
            "type": 2,
            "price": 5200.0,
            "volume": 3,
            "currentVolume": 0,
            "index": 1
        }))
        .expect("depth");
        assert_eq!(ProjectXDataClient::depth_level_size(&fallback_volume), 3.0);

        let delete_level: MarketDepth = serde_json::from_value(json!({
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:00Z",
            "type": 2,
            "price": 5200.0,
            "volume": 0,
            "currentVolume": 0,
            "index": 1
        }))
        .expect("depth");
        assert_eq!(ProjectXDataClient::depth_level_size(&delete_level), 0.0);
    }

    #[rstest::rstest]
    fn map_depth_event_emits_clear_delta_for_reset_type() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        depth_slots.insert(
            ProjectXDataClient::depth_slot_key(instrument_id, OrderSide::Buy, 1),
            Decimal::from(5200),
        );
        let depth: MarketDepth = serde_json::from_value(json!({"type": 6,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:00Z",
            "price": 5200.0,
            "volume": 0,
            "currentVolume": 0,
            "index": null
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            7,
            UnixNanos::from(11_u64),
            UnixNanos::from(12_u64),
        );

        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].action, BookAction::Clear);
        assert_eq!(deltas[0].flags, RecordFlag::F_SNAPSHOT as u8);
        assert_eq!(deltas[0].sequence, 7);
        assert!(depth_slots.is_empty());
    }

    #[rstest::rstest]
    fn map_depth_event_zero_size_becomes_delete() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        let depth: MarketDepth = serde_json::from_value(json!({"type": 2,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:00Z",
            "price": 5200.0,
            "volume": 0,
            "currentVolume": 0,
            "index": 3
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            8,
            UnixNanos::from(21_u64),
            UnixNanos::from(22_u64),
        );

        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].action, BookAction::Delete);
        assert_eq!(deltas[0].order.side, OrderSide::Buy);
        assert_eq!(deltas[0].order.size.as_f64(), 0.0);
        assert_eq!(
            deltas[0].flags,
            RecordFlag::F_LAST as u8 | RecordFlag::F_MBP as u8,
        );
    }

    #[rstest::rstest]
    fn map_depth_event_index_price_move_emits_delete_then_update() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        depth_slots.insert(
            ProjectXDataClient::depth_slot_key(instrument_id, OrderSide::Buy, 1),
            Decimal::from(5200),
        );
        let depth: MarketDepth = serde_json::from_value(json!({"type": 2,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:01Z",
            "price": 5200.25,
            "volume": 1,
            "currentVolume": 4,
            "index": 1
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            9,
            UnixNanos::from(31_u64),
            UnixNanos::from(32_u64),
        );

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].action, BookAction::Delete);
        assert_eq!(deltas[0].order.side, OrderSide::Buy);
        assert_eq!(deltas[0].order.price.as_f64(), 5200.0);
        assert_eq!(deltas[0].order.size.as_f64(), 0.0);
        assert_eq!(deltas[0].flags, RecordFlag::F_MBP as u8);
        assert_eq!(deltas[1].action, BookAction::Update);
        assert_eq!(deltas[1].order.side, OrderSide::Buy);
        assert_eq!(deltas[1].order.price.as_f64(), 5200.25);
        assert_eq!(deltas[1].order.size.as_f64(), 4.0);
        assert_eq!(
            deltas[1].flags,
            RecordFlag::F_LAST as u8 | RecordFlag::F_MBP as u8,
        );
    }

    #[rstest::rstest]
    fn map_depth_event_best_without_index_replaces_prior_top_slot() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        depth_slots.insert(
            ProjectXDataClient::depth_slot_key(instrument_id, OrderSide::Sell, 0),
            Decimal::from(5201),
        );
        let depth: MarketDepth = serde_json::from_value(json!({"type": 10,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:01Z",
            "price": 5201.25,
            "volume": 1,
            "currentVolume": 3,
            "index": null
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            10,
            UnixNanos::from(41_u64),
            UnixNanos::from(42_u64),
        );

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].action, BookAction::Delete);
        assert_eq!(deltas[0].order.side, OrderSide::Sell);
        assert_eq!(deltas[0].order.price.as_f64(), 5201.0);
        assert_eq!(deltas[0].order.size.as_f64(), 0.0);
        assert_eq!(deltas[1].action, BookAction::Update);
        assert_eq!(deltas[1].order.side, OrderSide::Sell);
        assert_eq!(deltas[1].order.price.as_f64(), 5201.25);
        assert_eq!(deltas[1].order.size.as_f64(), 3.0);
        assert_eq!(
            deltas[1].flags,
            RecordFlag::F_LAST as u8 | RecordFlag::F_MBP as u8,
        );
    }

    #[rstest::rstest]
    fn map_depth_event_zero_size_delete_uses_tracked_slot_price() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        depth_slots.insert(
            ProjectXDataClient::depth_slot_key(instrument_id, OrderSide::Sell, 2),
            Decimal::from(5201),
        );
        let depth: MarketDepth = serde_json::from_value(json!({"type": 1,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:01Z",
            "price": 5200.75,
            "volume": 0,
            "currentVolume": 0,
            "index": 2
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            10,
            UnixNanos::from(41_u64),
            UnixNanos::from(42_u64),
        );

        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].action, BookAction::Delete);
        assert_eq!(deltas[0].order.side, OrderSide::Sell);
        assert_eq!(deltas[0].order.price.as_f64(), 5201.0);
        assert!(depth_slots.is_empty());
    }

    #[rstest::rstest]
    fn map_depth_event_best_delete_without_index_uses_tracked_top_slot_price() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_slots = DashMap::new();
        depth_slots.insert(
            ProjectXDataClient::depth_slot_key(instrument_id, OrderSide::Buy, 0),
            "5200.25".parse::<Decimal>().expect("decimal"),
        );
        let depth: MarketDepth = serde_json::from_value(json!({"type": 9,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:01Z",
            "price": 5200.0,
            "volume": 0,
            "currentVolume": 0,
            "index": null
        }))
        .expect("depth");

        let deltas = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &depth,
            11,
            UnixNanos::from(51_u64),
            UnixNanos::from(52_u64),
        );

        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].action, BookAction::Delete);
        assert_eq!(deltas[0].order.side, OrderSide::Buy);
        assert_eq!(deltas[0].order.price.as_f64(), 5200.25);
        assert_eq!(deltas[0].order.size.as_f64(), 0.0);
        assert!(depth_slots.is_empty());
    }

    #[rstest::rstest]
    fn depth_sequence_and_ts_event_remain_monotonic_when_event_timestamps_regress() {
        let instrument_id = InstrumentId::from("MESM26.PROJECTX");
        let depth_sequence = DashMap::new();
        let depth_ts_last = DashMap::new();
        let depth_slots = DashMap::new();
        let newer_depth: MarketDepth = serde_json::from_value(json!({"type": 2,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:02Z",
            "price": 5200.25,
            "volume": 1,
            "currentVolume": 4,
            "index": 1
        }))
        .expect("depth");
        let older_depth: MarketDepth = serde_json::from_value(json!({
            "type": 2,
            "symbolId": "CON.F.US.MES.M26",
            "timestamp": "2026-04-02T00:00:01Z",
            "price": 5200.0,
            "volume": 1,
            "currentVolume": 2
        }))
        .expect("depth");

        let newer_seq = ProjectXDataClient::next_depth_sequence(&depth_sequence, instrument_id);
        let older_seq = ProjectXDataClient::next_depth_sequence(&depth_sequence, instrument_id);
        let newer_ts = ProjectXDataClient::clamp_depth_ts_event(
            &depth_ts_last,
            instrument_id,
            UnixNanos::from(200_u64),
        );
        let older_ts = ProjectXDataClient::clamp_depth_ts_event(
            &depth_ts_last,
            instrument_id,
            UnixNanos::from(100_u64),
        );
        let newer = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &newer_depth,
            newer_seq,
            newer_ts,
            UnixNanos::from(210_u64),
        );
        let older = ProjectXDataClient::map_depth_event(
            &depth_slots,
            instrument_id,
            &older_depth,
            older_seq,
            older_ts,
            UnixNanos::from(220_u64),
        );

        assert_eq!(newer_seq, 1);
        assert_eq!(older_seq, 2);
        assert_eq!(newer[0].sequence, 1);
        assert_eq!(older[0].sequence, 2);
        assert_eq!(newer[0].ts_event, UnixNanos::from(200_u64));
        assert_eq!(older[0].ts_event, UnixNanos::from(200_u64));
    }

    #[rstest::rstest]
    fn map_bar_request_maps_defaults_and_overrides() {
        let bar_type = BarType::from("MESM26.PROJECTX-5-MINUTE-LAST-EXTERNAL");
        let start = date(2026, 4, 1)
            .at(0, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .expect("valid timestamp")
            .timestamp();
        let end = date(2026, 4, 1)
            .at(1, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .expect("valid timestamp")
            .timestamp();

        let mut params = Params::new();
        params.insert("unit".to_string(), json!(2));
        params.insert("unit_number".to_string(), json!(3));
        params.insert("limit".to_string(), json!(120));
        let request = RequestBars::new(
            bar_type,
            Some(start),
            Some(end),
            Some(NonZeroUsize::new(200).expect("non-zero")),
            Some(ClientId::from("PROJECTX")),
            UUID4::new(),
            UnixNanos::default(),
            Some(params),
        );

        let (
            mapped,
            _contract_live,
            allow_live_history_fallback,
            client_id,
            start_nanos,
            end_nanos,
        ) = ProjectXDataClient::map_bar_request(&request, ClientId::from("PX-DEFAULT"), false)
            .expect("bar request should map");

        assert_eq!(mapped.contract_symbol, "CON.F.US.MES.M26");
        assert_eq!(mapped.unit, projectx_client::BarUnit::Minute);
        assert_eq!(mapped.unit_number, 3);
        assert_eq!(mapped.limit, 200); // explicit request limit takes precedence
        assert!(!mapped.live);
        assert!(!allow_live_history_fallback);
        assert_eq!(client_id, ClientId::from("PROJECTX"));
        assert!(start_nanos.is_some());
        assert!(end_nanos.is_some());
    }

    #[rstest::rstest]
    fn map_bar_request_allows_distinct_contract_live_override() {
        let bar_type = BarType::from("MESM26.PROJECTX-15-SECOND-LAST-EXTERNAL");
        let mut params = Params::new();
        params.insert("live".to_string(), json!(true));
        params.insert("contract_live".to_string(), json!(false));
        params.insert(ALLOW_LIVE_HISTORY_FALLBACK_PARAM.to_string(), json!(true));
        let request = RequestBars::new(
            bar_type,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            Some(params),
        );

        let (mapped, contract_live, allow_live_history_fallback, ..) =
            ProjectXDataClient::map_bar_request(&request, ClientId::from("PX-DEFAULT"), true)
                .expect("bar request should map");

        assert!(mapped.live);
        assert!(!contract_live);
        assert!(allow_live_history_fallback);
    }

    #[rstest::rstest]
    fn map_bar_request_uses_documented_default_units() {
        let second = RequestBars::new(
            BarType::from("MESM26.PROJECTX-15-SECOND-LAST-INTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let minute = RequestBars::new(
            BarType::from("MESM26.PROJECTX-1-MINUTE-LAST-EXTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let hour = RequestBars::new(
            BarType::from("MESM26.PROJECTX-1-HOUR-LAST-EXTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let day = RequestBars::new(
            BarType::from("MESM26.PROJECTX-1-DAY-LAST-EXTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );

        let (second_mapped, ..) =
            ProjectXDataClient::map_bar_request(&second, ClientId::from("PX-DEFAULT"), false)
                .expect("second request should map");
        let (minute_mapped, ..) =
            ProjectXDataClient::map_bar_request(&minute, ClientId::from("PX-DEFAULT"), false)
                .expect("minute request should map");
        let (hour_mapped, ..) =
            ProjectXDataClient::map_bar_request(&hour, ClientId::from("PX-DEFAULT"), false)
                .expect("hour request should map");
        let (day_mapped, ..) =
            ProjectXDataClient::map_bar_request(&day, ClientId::from("PX-DEFAULT"), false)
                .expect("day request should map");

        assert_eq!(second_mapped.unit, projectx_client::BarUnit::Second);
        assert_eq!(second_mapped.unit_number, 15);
        assert_eq!(minute_mapped.unit, projectx_client::BarUnit::Minute);
        assert_eq!(minute_mapped.unit_number, 1);
        assert_eq!(hour_mapped.unit, projectx_client::BarUnit::Hour);
        assert_eq!(hour_mapped.unit_number, 1);
        assert_eq!(day_mapped.unit, projectx_client::BarUnit::Day);
        assert_eq!(day_mapped.unit_number, 1);
    }

    #[rstest::rstest]
    fn map_historical_bars_orders_oldest_first_and_sets_ts_init_from_event() {
        let bar_type = BarType::from("MNQM26.PROJECTX-15-SECOND-LAST-EXTERNAL");
        let bars = vec![
            px_bar(
                "2026-04-08T11:22:15Z",
                25201.0,
                25202.0,
                25200.5,
                25201.5,
                10,
            ),
            px_bar(
                "2026-04-08T11:22:00Z",
                25200.0,
                25201.0,
                25199.5,
                25200.5,
                8,
            ),
        ];

        let mapped = ProjectXDataClient::map_historical_bars(bar_type, bars);

        assert_eq!(mapped.len(), 2);
        assert!(mapped[0].ts_event < mapped[1].ts_event);
        assert_eq!(mapped[0].ts_init, mapped[0].ts_event);
        assert_eq!(mapped[1].ts_init, mapped[1].ts_event);
    }

    #[rstest::rstest]
    fn normalize_historical_bars_keeps_open_bar_at_request_cutoff() {
        let bar_type = BarType::from("MNQM26.PROJECTX-15-SECOND-LAST-EXTERNAL");
        let bars = ProjectXDataClient::map_historical_bars(
            bar_type,
            vec![
                px_bar(
                    "2026-04-08T11:22:00Z",
                    25200.0,
                    25201.0,
                    25199.5,
                    25200.5,
                    8,
                ),
                px_bar(
                    "2026-04-08T11:22:15Z",
                    25201.0,
                    25202.0,
                    25200.5,
                    25201.5,
                    10,
                ),
            ],
        );

        let filtered = ProjectXDataClient::normalize_historical_bars(bars);

        assert_eq!(filtered.len(), 2);
        assert_eq!(
            filtered[0].ts_event,
            UnixNanos::from(
                u64::try_from(ts("2026-04-08T11:22:00Z").as_jiff().as_nanosecond()).unwrap(),
            ),
        );
        assert_eq!(
            filtered[1].ts_event,
            UnixNanos::from(
                u64::try_from(ts("2026-04-08T11:22:15Z").as_jiff().as_nanosecond()).unwrap(),
            ),
        );
    }

    #[rstest::rstest]
    fn next_historical_page_start_advances_when_history_ends_before_requested_end() {
        let bar_type = BarType::from("MNQM26.PROJECTX-15-SECOND-LAST-EXTERNAL");
        let start = UnixNanos::from(
            u64::try_from(ts("2026-04-08T11:22:00Z").as_jiff().as_nanosecond()).unwrap(),
        );
        let end = UnixNanos::from(
            u64::try_from(ts("2026-04-08T11:23:00Z").as_jiff().as_nanosecond()).unwrap(),
        );
        let bars = ProjectXDataClient::map_historical_bars(
            bar_type,
            vec![
                px_bar(
                    "2026-04-08T11:22:00Z",
                    25200.0,
                    25201.0,
                    25199.5,
                    25200.5,
                    8,
                ),
                px_bar(
                    "2026-04-08T11:22:30Z",
                    25203.0,
                    25204.0,
                    25202.5,
                    25203.5,
                    12,
                ),
            ],
        );

        let next_start =
            ProjectXDataClient::next_historical_page_start(bar_type, &bars, Some(start), Some(end))
                .expect("should advance");

        assert_eq!(
            next_start,
            UnixNanos::from(
                u64::try_from(ts("2026-04-08T11:22:45Z").as_jiff().as_nanosecond()).unwrap(),
            ),
        );
    }

    #[rstest::rstest]
    fn next_historical_page_start_stops_once_requested_end_is_covered() {
        let bar_type = BarType::from("MNQM26.PROJECTX-15-SECOND-LAST-EXTERNAL");
        let start = UnixNanos::from(
            u64::try_from(ts("2026-04-08T11:22:00Z").as_jiff().as_nanosecond()).unwrap(),
        );
        let end = UnixNanos::from(
            u64::try_from(ts("2026-04-08T11:23:00Z").as_jiff().as_nanosecond()).unwrap(),
        );
        let bars = ProjectXDataClient::map_historical_bars(
            bar_type,
            vec![
                px_bar(
                    "2026-04-08T11:22:00Z",
                    25200.0,
                    25201.0,
                    25199.5,
                    25200.5,
                    8,
                ),
                px_bar(
                    "2026-04-08T11:22:45Z",
                    25203.0,
                    25204.0,
                    25202.5,
                    25203.5,
                    12,
                ),
            ],
        );

        assert!(
            ProjectXDataClient::next_historical_page_start(bar_type, &bars, Some(start), Some(end))
                .is_none()
        );
    }

    #[rstest::rstest]
    fn build_bars_response_params_sets_data_count_and_error_metadata() {
        let mut params = Params::new();
        params.insert("live".to_string(), json!(false));

        let response_params = ProjectXDataClient::build_bars_response_params(
            Some(params),
            0,
            Some(1),
            Some("request failed"),
            HistoricalBarsMetadata {
                live_history_fallback_used: true,
                requested_live: true,
                history_source_live: false,
            },
        );

        assert_eq!(response_params.get_bool("live"), Some(false));
        assert_eq!(response_params.get_usize("data_count"), Some(0));
        assert_eq!(response_params.get_i64("error_code"), Some(1));
        assert_eq!(
            response_params.get_bool(LIVE_HISTORY_FALLBACK_USED_PARAM),
            Some(true)
        );
        assert_eq!(response_params.get_bool(REQUESTED_LIVE_PARAM), Some(true));
        assert_eq!(response_params.get_str(HISTORY_SOURCE_PARAM), Some("sim"));
        assert_eq!(
            response_params.get_bool(HISTORY_SOURCE_LIVE_PARAM),
            Some(false)
        );
        assert_eq!(
            response_params.get_str("error_message"),
            Some("request failed")
        );
    }

    #[rstest::rstest]
    fn should_retry_live_history_with_sim_only_for_live_error_code_one() {
        let live_request = HistoryRequest::builder(
            ContractId::new("CON.F.US.MES.M26").unwrap(),
            true,
            ts("2026-04-08T11:22:00Z"),
            ts("2026-04-08T11:23:00Z"),
            projectx_client::BarUnit::Minute,
        )
        .build()
        .unwrap();
        let sim_request = HistoryRequest::builder(
            ContractId::new("CON.F.US.MES.M26").unwrap(),
            false,
            ts("2026-04-08T11:22:00Z"),
            ts("2026-04-08T11:23:00Z"),
            projectx_client::BarUnit::Minute,
        )
        .build()
        .unwrap();

        assert!(ProjectXDataClient::should_retry_live_history_with_sim(
            &live_request,
            Some(1),
            true,
        ));
        assert!(!ProjectXDataClient::should_retry_live_history_with_sim(
            &sim_request,
            Some(1),
            true,
        ));
        assert!(!ProjectXDataClient::should_retry_live_history_with_sim(
            &live_request,
            Some(2),
            true,
        ));
        assert!(!ProjectXDataClient::should_retry_live_history_with_sim(
            &live_request,
            Some(1),
            false,
        ));
    }

    #[expect(dead_code)]
    fn resolve_unsubscribe_contract_id_falls_back_when_missing_from_cache() {
        let contracts_by_symbol = Arc::new(ParkingRwLock::new(AHashMap::new()));
        let resolved = ProjectXDataClient::resolve_unsubscribe_contract_id_from_cache(
            &contracts_by_symbol,
            "MESM26",
        );
        assert_eq!(resolved, "CON.F.US.MES.M26");
    }

    #[rstest::rstest]
    fn select_contract_cache_keeps_live_and_sim_contract_aliases_separate() {
        let live_contracts_by_symbol = Arc::new(ParkingRwLock::new(AHashMap::new()));
        let sim_contracts_by_symbol = Arc::new(ParkingRwLock::new(AHashMap::new()));
        let mut live_contract = contract("LIVE-CONTRACT-ID", "MNQM6", true);
        live_contract.symbol_id = SymbolId::new("F.US.MNQ").unwrap();
        let mut sim_contract = contract("SIM-CONTRACT-ID", "MNQM6", true);
        sim_contract.symbol_id = SymbolId::new("F.US.MNQ").unwrap();

        ProjectXDataClient::cache_contract_aliases(&live_contracts_by_symbol, &live_contract);
        ProjectXDataClient::cache_contract_aliases(&sim_contracts_by_symbol, &sim_contract);

        let live_contracts = ProjectXDataClient::select_contract_cache(
            true,
            &live_contracts_by_symbol,
            &sim_contracts_by_symbol,
        );
        let sim_contracts = ProjectXDataClient::select_contract_cache(
            false,
            &live_contracts_by_symbol,
            &sim_contracts_by_symbol,
        );

        assert_eq!(
            ProjectXDataClient::resolve_unsubscribe_contract_id_from_cache(live_contracts, "MNQM6",),
            "LIVE-CONTRACT-ID",
        );
        assert_eq!(
            ProjectXDataClient::resolve_unsubscribe_contract_id_from_cache(sim_contracts, "MNQM6",),
            "SIM-CONTRACT-ID",
        );
    }
}
