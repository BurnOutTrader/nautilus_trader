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

//! Live execution client implementing the `ExecutionClient` trait (v2 PyO3 path).
//!
//! Wraps `RithmicExecutionClient` and wires its event pump into the
//! NautilusTrader execution engine when a live runner sender is installed.

#![allow(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

use std::{
    fmt::{self, Display},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use dashmap::DashMap;
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::try_get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateFillReportsBuilder, GenerateOrderStatusReport, GenerateOrderStatusReports,
        GenerateOrderStatusReportsBuilder, GeneratePositionStatusReports,
        GeneratePositionStatusReportsBuilder, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
        SubmitOrderList,
    },
};
use nautilus_core::{
    UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        ContingencyType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{LIMIT_ORDER_TYPES, Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::RwLock as ParkingRwLock;
use rithmic_rs::{
    OrderSide as RithmicOrderSide, OrderType as RithmicOrderType, RithmicAccount,
    RithmicBracketOrder, RithmicOcoOrderLeg, TimeInForce as RithmicTif, api::RithmicResponse,
    rti::messages::RithmicMessage,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::{
    sync::{RwLock, broadcast},
    task::JoinHandle,
};

use crate::{
    common::enums::ConnectionState,
    config::RithmicExecClientConfig,
    data::live::resolve_contract_exchange,
    execution::{
        ExecutionEvent, OrderRequest, OrderState, RithmicExecutionClient, TrailingStopConfig,
    },
    gateway::{GatewayConfig, PnlEvent, RithmicGateway},
    instruments::parse::{apply_auxiliary_reference_data, response_to_instrument},
    providers::{AccountEvent as ProviderAccountEvent, PositionEvent as ProviderPositionEvent},
    shared_gateway::SharedGatewayLease,
};

fn to_rithmic_side(side: OrderSide) -> anyhow::Result<RithmicOrderSide> {
    match side {
        OrderSide::Buy => Ok(RithmicOrderSide::Buy),
        OrderSide::Sell => Ok(RithmicOrderSide::Sell),
        OrderSide::NoOrderSide => Err(anyhow::anyhow!("Order side must be Buy or Sell")),
    }
}

fn to_rithmic_order_type(order_type: OrderType) -> anyhow::Result<RithmicOrderType> {
    match order_type {
        OrderType::Market => Ok(RithmicOrderType::Market),
        OrderType::Limit => Ok(RithmicOrderType::Limit),
        OrderType::StopMarket | OrderType::TrailingStopMarket => Ok(RithmicOrderType::StopMarket),
        OrderType::StopLimit | OrderType::TrailingStopLimit => Ok(RithmicOrderType::StopLimit),
        other => Err(anyhow::anyhow!(
            "Unsupported order type for Rithmic: {other}"
        )),
    }
}

fn to_rithmic_tif(tif: TimeInForce) -> anyhow::Result<RithmicTif> {
    match tif {
        TimeInForce::Day => Ok(RithmicTif::Day),
        TimeInForce::Gtc => Ok(RithmicTif::Gtc),
        TimeInForce::Ioc => Ok(RithmicTif::Ioc),
        TimeInForce::Fok => Ok(RithmicTif::Fok),
        other => Err(anyhow::anyhow!(
            "Unsupported time-in-force for Rithmic: {other}"
        )),
    }
}

fn first_response_error(responses: &[RithmicResponse]) -> Option<String> {
    responses
        .iter()
        .find_map(|response| response.error.as_ref().map(|e| e.to_string()))
}

fn emit_order_list_rejected(emitter: &ExecutionEventEmitter, orders: &[OrderAny], reason: &str) {
    let ts_event = get_atomic_clock_realtime().get_time_ns();

    for order in orders {
        emitter.emit_order_rejected(order, reason, ts_event, false);
    }
}

#[derive(Clone)]
struct NativeRithmicOcoSpec {
    leg1: OrderAny,
    leg2: OrderAny,
}

#[derive(Clone)]
struct NativeRithmicBracketSpec {
    entry: OrderAny,
    stop: OrderAny,
    target: OrderAny,
    profit_ticks: i32,
    stop_ticks: i32,
}

/// Live execution client for Rithmic implementing the NautilusTrader `ExecutionClient` trait.
///
/// This is the v2 PyO3 entry point — used with `LiveNode` and `RithmicExecClientFactory`.
/// The client:
/// 1. Creates and connects a `RithmicGateway` on `connect()` (order + pnl plants)
/// 2. Spawns `RithmicExecutionClient::spawn_event_pump` to process execution events
/// 3. Re-emits events into the engine when a live runner sender is available
pub struct RithmicLiveExecClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: RithmicExecClientConfig,
    emitter: ExecutionEventEmitter,
    inner: Option<Arc<RithmicExecutionClient>>,
    gateway: Option<SharedGatewayLease>,
    event_task: Option<JoinHandle<()>>,
    is_connected: Arc<AtomicBool>,
    pending_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    order_reports_by_client: Arc<DashMap<ClientOrderId, OrderStatusReport>>,
    order_reports_by_venue: Arc<DashMap<VenueOrderId, OrderStatusReport>>,
    fill_reports: Arc<DashMap<String, FillReport>>,
    position_reports: Arc<DashMap<String, PositionStatusReport>>,
    resolved_exchanges: Arc<ParkingRwLock<AHashMap<String, String>>>,
}

impl fmt::Debug for RithmicLiveExecClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicLiveExecClient))
            .field("client_id", &self.core.client_id)
            .field("account_id", &self.core.account_id)
            .field("is_connected", &self.is_connected.load(Ordering::Relaxed))
            .finish()
    }
}

impl RithmicLiveExecClient {
    /// Creates a new [`RithmicLiveExecClient`].
    #[must_use]
    pub fn new(core: ExecutionClientCore, config: RithmicExecClientConfig) -> Self {
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        Self {
            core,
            clock,
            config,
            emitter,
            inner: None,
            gateway: None,
            event_task: None,
            is_connected: Arc::new(AtomicBool::new(false)),
            pending_tasks: Arc::new(Mutex::new(Vec::new())),
            order_reports_by_client: Arc::new(DashMap::new()),
            order_reports_by_venue: Arc::new(DashMap::new()),
            fill_reports: Arc::new(DashMap::new()),
            position_reports: Arc::new(DashMap::new()),
            resolved_exchanges: Arc::new(ParkingRwLock::new(AHashMap::new())),
        }
    }

    fn require_inner(&self) -> anyhow::Result<Arc<RithmicExecutionClient>> {
        self.inner
            .clone()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient is not connected"))
    }

    /// Spawns an async task on the Rithmic runtime. Errors are logged; the
    /// actual order response arrives via the execution event pump.
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let handle = get_runtime().spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        });
        let mut tasks = self
            .pending_tasks
            .lock()
            .expect("pending_tasks mutex poisoned");
        tasks.retain(|h| !h.is_finished());
        tasks.push(handle);
    }

    fn abort_pending_tasks(&self) {
        let mut tasks = self
            .pending_tasks
            .lock()
            .expect("pending_tasks mutex poisoned");

        for task in tasks.drain(..) {
            task.abort();
        }
    }

    fn clear_cached_reports(&self) {
        self.order_reports_by_client.clear();
        self.order_reports_by_venue.clear();
        self.fill_reports.clear();
        self.position_reports.clear();
    }

    async fn cache_missing_instruments_for_reports(
        &self,
        order_reports: &[OrderStatusReport],
        fill_reports: &[FillReport],
        position_reports: &[PositionStatusReport],
    ) {
        let Some(gateway) = self.gateway.as_ref().map(SharedGatewayLease::gateway) else {
            return;
        };

        let missing_ids = {
            let cache = self.core.cache();
            let mut ids = std::collections::BTreeSet::new();

            for report in order_reports {
                if cache.instrument(&report.instrument_id).is_none() {
                    ids.insert(report.instrument_id);
                }
            }

            for report in fill_reports {
                if cache.instrument(&report.instrument_id).is_none() {
                    ids.insert(report.instrument_id);
                }
            }

            for report in position_reports {
                if cache.instrument(&report.instrument_id).is_none() {
                    ids.insert(report.instrument_id);
                }
            }

            ids.into_iter().collect::<Vec<_>>()
        };

        if missing_ids.is_empty() {
            return;
        }

        for instrument_id in missing_ids {
            if let Err(e) =
                ensure_cached_instrument(&gateway, &self.resolved_exchanges, instrument_id).await
            {
                log::warn!("Failed to cache Rithmic reconciliation instrument: {e}");
            }
        }
    }

    async fn build_current_state_mass_status(
        &self,
        ts_now: UnixNanos,
    ) -> anyhow::Result<ExecutionMassStatus> {
        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(true)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_now)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_fill_reports(fill_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        let open_venue_order_ids: AHashSet<_> = order_reports
            .iter()
            .map(|report| report.venue_order_id)
            .collect();
        let current_fill_reports: Vec<FillReport> = fill_reports
            .into_iter()
            .filter(|report| open_venue_order_ids.contains(&report.venue_order_id))
            .collect();

        self.cache_missing_instruments_for_reports(
            &order_reports,
            &current_fill_reports,
            &position_reports,
        )
        .await;

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            ts_now,
            None,
        );
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(current_fill_reports);
        mass_status.add_position_reports(position_reports);
        Ok(mass_status)
    }

    fn deny_order_list(&self, orders: &[OrderAny], reason: &str) {
        for order in orders {
            self.emitter.emit_order_denied(order, reason);
        }
    }

    fn emit_order_list_submitted(&self, orders: &[OrderAny]) {
        for order in orders {
            self.emitter.emit_order_submitted(order);
        }
    }

    fn whole_quantity(quantity: Quantity) -> anyhow::Result<i32> {
        let quantity_decimal = quantity.as_decimal();

        if quantity_decimal <= Decimal::ZERO {
            anyhow::bail!("Quantity must be positive, received: {quantity_decimal}");
        }

        if quantity_decimal.fract() != Decimal::ZERO {
            anyhow::bail!(
                "Quantity must be a whole number of contracts, received: {quantity_decimal}"
            );
        }

        quantity_decimal.to_i32().ok_or_else(|| {
            anyhow::anyhow!("Quantity exceeds Rithmic i32 contract limit: {quantity_decimal}")
        })
    }

    fn require_positive_tick_distance(
        delta: Decimal,
        tick_size: Decimal,
        label: &str,
    ) -> anyhow::Result<i32> {
        if delta <= Decimal::ZERO {
            anyhow::bail!(
                "UNSUPPORTED_NATIVE_BRACKET_{}_DISTANCE",
                label.to_uppercase()
            );
        }

        let ticks = delta / tick_size;

        if ticks.fract() != Decimal::ZERO {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_{}_TICKS", label.to_uppercase());
        }

        ticks.to_i32().ok_or_else(|| {
            anyhow::anyhow!(
                "UNSUPPORTED_NATIVE_BRACKET_{}_TICKS_OVERFLOW",
                label.to_uppercase()
            )
        })
    }

    fn build_native_oco_spec(
        &self,
        orders: &[OrderAny],
    ) -> anyhow::Result<Option<NativeRithmicOcoSpec>> {
        if orders.len() != 2 {
            return Ok(None);
        }

        let leg1 = &orders[0];
        let leg2 = &orders[1];
        let leg1_id = leg1.client_order_id();
        let leg2_id = leg2.client_order_id();

        let linked_leg1 = leg1
            .linked_order_ids()
            .is_some_and(|ids| ids.len() == 1 && ids[0] == leg2_id);
        let linked_leg2 = leg2
            .linked_order_ids()
            .is_some_and(|ids| ids.len() == 1 && ids[0] == leg1_id);

        if leg1.parent_order_id().is_some()
            || leg2.parent_order_id().is_some()
            || leg1.contingency_type() != Some(ContingencyType::Oco)
            || leg2.contingency_type() != Some(ContingencyType::Oco)
            || !linked_leg1
            || !linked_leg2
        {
            return Ok(None);
        }

        for order in [leg1, leg2] {
            if order.is_quote_quantity() {
                anyhow::bail!("UNSUPPORTED_QUOTE_QUANTITY");
            }

            if order.is_post_only() {
                anyhow::bail!("UNSUPPORTED_POST_ONLY");
            }

            if matches!(
                order.order_type(),
                OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
            ) {
                anyhow::bail!("UNSUPPORTED_TRAILING_STOP");
            }

            Self::whole_quantity(order.quantity())?;
            to_rithmic_order_type(order.order_type())?;
            to_rithmic_tif(order.time_in_force())?;

            if matches!(order.order_type(), OrderType::Limit | OrderType::StopLimit)
                && order.price().is_none()
            {
                anyhow::bail!("UNSUPPORTED_NATIVE_OCO_ORDER_WITHOUT_PRICE");
            }

            if matches!(
                order.order_type(),
                OrderType::StopMarket | OrderType::StopLimit
            ) && order.trigger_price().is_none()
            {
                anyhow::bail!("UNSUPPORTED_NATIVE_OCO_ORDER_WITHOUT_TRIGGER_PRICE");
            }
        }

        Ok(Some(NativeRithmicOcoSpec {
            leg1: leg1.clone(),
            leg2: leg2.clone(),
        }))
    }

    fn build_native_bracket_spec(
        &self,
        orders: &[OrderAny],
    ) -> anyhow::Result<Option<NativeRithmicBracketSpec>> {
        if orders.len() != 3 {
            return Ok(None);
        }

        let entry = &orders[0];

        if entry.contingency_type() != Some(ContingencyType::Oto) {
            return Ok(None);
        }

        let children = [&orders[1], &orders[2]];
        let entry_id = entry.client_order_id();

        if children
            .iter()
            .any(|child| child.parent_order_id() != Some(entry_id))
        {
            return Ok(None);
        }

        if children.iter().any(|child| {
            !matches!(
                child.contingency_type(),
                Some(ContingencyType::Oco | ContingencyType::Ouo)
            )
        }) {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_CHILD_CONTINGENCY");
        }

        if children.iter().any(|child| !child.is_reduce_only()) {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_NON_REDUCE_ONLY_CHILD");
        }

        let stop_candidates: Vec<&OrderAny> = children
            .iter()
            .copied()
            .filter(|child| child.order_type() == OrderType::StopMarket)
            .collect();
        let target_candidates: Vec<&OrderAny> = children
            .iter()
            .copied()
            .filter(|child| child.order_type() == OrderType::Limit)
            .collect();

        if stop_candidates.len() != 1 || target_candidates.len() != 1 {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_CHILD_ORDER_TYPES");
        }

        let stop = stop_candidates[0];
        let target = target_candidates[0];

        if entry.order_type() == OrderType::Market {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_MARKET_ENTRY_FOR_HIGH_LEVEL_RITHMIC");
        }

        if entry.order_type() != OrderType::Limit {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_ENTRY_ORDER_TYPE");
        }

        if entry.is_quote_quantity() {
            anyhow::bail!("UNSUPPORTED_QUOTE_QUANTITY");
        }

        if entry.is_post_only() {
            anyhow::bail!("UNSUPPORTED_POST_ONLY");
        }

        if entry.trigger_price().is_some() {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_TRIGGERED_ENTRY");
        }

        if entry.price().is_none() {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_LIMIT_ENTRY_WITHOUT_PRICE");
        }

        if stop.is_post_only() || target.is_post_only() {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_POST_ONLY_CHILD");
        }

        let entry_qty = Self::whole_quantity(entry.quantity())?;
        let stop_qty = Self::whole_quantity(stop.quantity())?;
        let target_qty = Self::whole_quantity(target.quantity())?;

        if stop_qty != entry_qty || target_qty != entry_qty {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_CHILD_QUANTITY_MISMATCH");
        }

        to_rithmic_tif(entry.time_in_force())?;
        to_rithmic_tif(stop.time_in_force())?;
        to_rithmic_tif(target.time_in_force())?;

        let stop_trigger_price = stop.trigger_price().ok_or_else(|| {
            anyhow::anyhow!("UNSUPPORTED_NATIVE_BRACKET_CHILD_PRICE_CONFIGURATION")
        })?;
        let target_price = target.price().ok_or_else(|| {
            anyhow::anyhow!("UNSUPPORTED_NATIVE_BRACKET_CHILD_PRICE_CONFIGURATION")
        })?;

        if stop.order_side() != target.order_side() || stop.order_side() == entry.order_side() {
            anyhow::bail!("UNSUPPORTED_NATIVE_BRACKET_CHILD_SIDE_CONFIGURATION");
        }

        let cache = self.core.cache();
        let instrument = cache.instrument(&entry.instrument_id()).ok_or_else(|| {
            anyhow::anyhow!(
                "MISSING_INSTRUMENT_FOR_RITHMIC_NATIVE_BRACKET: {}",
                entry.instrument_id()
            )
        })?;

        let tick_size = instrument.price_increment().as_decimal();

        if tick_size <= Decimal::ZERO {
            anyhow::bail!("INVALID_RITHMIC_NATIVE_BRACKET_TICK_SIZE");
        }

        let entry_price = entry
            .price()
            .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_NATIVE_BRACKET_LIMIT_ENTRY_WITHOUT_PRICE"))?
            .as_decimal();
        let stop_price = stop_trigger_price.as_decimal();
        let target_price = target_price.as_decimal();

        let (profit_delta, stop_delta) = if entry.is_buy() {
            (target_price - entry_price, entry_price - stop_price)
        } else {
            (entry_price - target_price, stop_price - entry_price)
        };

        let profit_ticks = Self::require_positive_tick_distance(profit_delta, tick_size, "profit")?;
        let stop_ticks = Self::require_positive_tick_distance(stop_delta, tick_size, "stop")?;

        Ok(Some(NativeRithmicBracketSpec {
            entry: entry.clone(),
            stop: stop.clone(),
            target: target.clone(),
            profit_ticks,
            stop_ticks,
        }))
    }

    fn submit_native_oco_order_list(&self, spec: &NativeRithmicOcoSpec) -> anyhow::Result<()> {
        let inner = self.require_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let account = inner.account().clone();
        let orders = vec![spec.leg1.clone(), spec.leg2.clone()];

        self.emit_order_list_submitted(&orders);

        let emitter = self.emitter.clone();
        let spec = spec.clone();
        self.spawn_task("submit_order_list_oco", async move {
            let result: anyhow::Result<()> = async {
                let (leg1_symbol, leg1_exchange) = resolve_contract_exchange(
                    &gateway,
                    &resolved_exchanges,
                    &spec.leg1.instrument_id(),
                )
                .await?;
                let (leg2_symbol, leg2_exchange) = resolve_contract_exchange(
                    &gateway,
                    &resolved_exchanges,
                    &spec.leg2.instrument_id(),
                )
                .await?;

                let leg1 = RithmicOcoOrderLeg {
                    symbol: leg1_symbol,
                    exchange: leg1_exchange,
                    quantity: Self::whole_quantity(spec.leg1.quantity())?,
                    price: match spec.leg1.order_type() {
                        OrderType::Market | OrderType::StopMarket => 0.0,
                        OrderType::Limit | OrderType::StopLimit => {
                            f64::from(spec.leg1.price().ok_or_else(|| {
                                anyhow::anyhow!("MISSING_LIMIT_PRICE_FOR_OCO_LEG_1")
                            })?)
                        }
                        _ => spec.leg1.price().map_or(0.0, f64::from),
                    },
                    trigger_price: spec.leg1.trigger_price().map(f64::from),
                    transaction_type: to_rithmic_side(spec.leg1.order_side())?.into(),
                    duration: to_rithmic_tif(spec.leg1.time_in_force())?.into(),
                    price_type: to_rithmic_order_type(spec.leg1.order_type())?.into(),
                    user_tag: spec.leg1.client_order_id().to_string(),
                };

                let leg2 = RithmicOcoOrderLeg {
                    symbol: leg2_symbol,
                    exchange: leg2_exchange,
                    quantity: Self::whole_quantity(spec.leg2.quantity())?,
                    price: match spec.leg2.order_type() {
                        OrderType::Market | OrderType::StopMarket => 0.0,
                        OrderType::Limit | OrderType::StopLimit => {
                            f64::from(spec.leg2.price().ok_or_else(|| {
                                anyhow::anyhow!("MISSING_LIMIT_PRICE_FOR_OCO_LEG_2")
                            })?)
                        }
                        _ => spec.leg2.price().map_or(0.0, f64::from),
                    },
                    trigger_price: spec.leg2.trigger_price().map(f64::from),
                    transaction_type: to_rithmic_side(spec.leg2.order_side())?.into(),
                    duration: to_rithmic_tif(spec.leg2.time_in_force())?.into(),
                    price_type: to_rithmic_order_type(spec.leg2.order_type())?.into(),
                    user_tag: spec.leg2.client_order_id().to_string(),
                };

                let handle = gateway
                    .read()
                    .await
                    .order_handle(&account)
                    .ok_or_else(|| anyhow::anyhow!("Order plant not connected"))?;

                let responses = handle
                    .place_oco_order(leg1, leg2)
                    .await
                    .map_err(|e| anyhow::anyhow!("OCO submission failed: {e}"))?;

                if let Some(e) = first_response_error(&responses) {
                    anyhow::bail!("OCO submission failed: {e}");
                }

                Ok(())
            }
            .await;

            if let Err(e) = result {
                emit_order_list_rejected(&emitter, &orders, &e.to_string());
                return Err(e);
            }

            Ok(())
        });

        Ok(())
    }

    fn submit_native_bracket_order_list(
        &self,
        spec: &NativeRithmicBracketSpec,
    ) -> anyhow::Result<()> {
        let inner = self.require_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let account = inner.account().clone();
        let orders = vec![spec.entry.clone(), spec.stop.clone(), spec.target.clone()];

        self.emit_order_list_submitted(&orders);

        let emitter = self.emitter.clone();
        let spec = spec.clone();
        self.spawn_task("submit_order_list_bracket", async move {
            let result: anyhow::Result<()> = async {
                let (symbol, exchange) = resolve_contract_exchange(
                    &gateway,
                    &resolved_exchanges,
                    &spec.entry.instrument_id(),
                )
                .await?;
                let bracket_order = RithmicBracketOrder {
                    action: to_rithmic_side(spec.entry.order_side())?.into(),
                    duration: to_rithmic_tif(spec.entry.time_in_force())?.into(),
                    exchange,
                    localid: spec.entry.client_order_id().to_string(),
                    price_type: to_rithmic_order_type(spec.entry.order_type())?.into(),
                    price: spec.entry.price().map(f64::from),
                    profit_ticks: spec.profit_ticks,
                    quantity: Self::whole_quantity(spec.entry.quantity())?,
                    stop_ticks: spec.stop_ticks,
                    symbol,
                };

                let handle = gateway
                    .read()
                    .await
                    .order_handle(&account)
                    .ok_or_else(|| anyhow::anyhow!("Order plant not connected"))?;

                let responses = handle
                    .place_bracket_order(bracket_order)
                    .await
                    .map_err(|e| anyhow::anyhow!("Bracket submission failed: {e}"))?;

                if let Some(e) = first_response_error(&responses) {
                    anyhow::bail!("Bracket submission failed: {e}");
                }

                Ok(())
            }
            .await;

            if let Err(e) = result {
                emit_order_list_rejected(&emitter, &orders, &e.to_string());
                return Err(e);
            }

            Ok(())
        });

        Ok(())
    }

    /// Builds a `GatewayConfig` for the execution client (order + pnl plants).
    fn gateway_config(&self) -> GatewayConfig {
        let c = &self.config;
        let mut cfg = GatewayConfig::new(
            c.environment,
            c.username.as_str(),
            c.password.as_str(),
            c.system_name.as_str(),
            c.fcm_id.as_deref().unwrap_or(""),
            c.ib_id.as_deref().unwrap_or(""),
            c.account_id.as_str(),
        );
        cfg.app_name = c.app_name.clone();
        cfg.app_version = c.app_version.clone();
        cfg.server = c.server.clone();
        cfg.alt_server = c.alt_server.clone();
        cfg.enable_ticker = true;
        cfg.enable_order = true;
        cfg.enable_pnl = true;
        cfg.enable_history = false;
        cfg
    }

    async fn bootstrap_connection(
        inner: Arc<RithmicExecutionClient>,
        replay_lookback_secs: u64,
    ) -> anyhow::Result<()> {
        inner.subscribe_order_updates().await?;
        inner.subscribe_pnl_updates().await?;
        inner.request_pnl_snapshot().await?;
        inner.query_orders().await?;

        if replay_lookback_secs > 0 {
            let end_sec = (get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000)
                .min(i32::MAX as u64) as i32;
            let start_sec = end_sec.saturating_sub(replay_lookback_secs as i32);

            if let Err(e) = inner.replay_executions(start_sec, end_sec).await {
                if is_empty_replay_error(&e) {
                    log::info!("No historical executions returned for replay window");
                } else {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl ExecutionClient for RithmicLiveExecClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        if let Some(sender) = try_get_exec_event_sender() {
            self.emitter.set_sender(sender);
        } else {
            log::debug!(
                "No execution event sender installed for client_id={}, start() will remain local-only until connect under a live runner",
                self.core.client_id
            );
        }
        self.core.set_started();
        log::info!("Started: client_id={}", self.core.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        log::info!("Stopping: client_id={}", self.core.client_id);

        if let Some(task) = self.event_task.take() {
            task.abort();
        }
        self.abort_pending_tasks();
        self.is_connected.store(false, Ordering::Relaxed);
        self.resolved_exchanges.write().clear();
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        let gateway_config = self.gateway_config();
        let gateway = SharedGatewayLease::acquire(gateway_config.clone());
        let shared_gateway = gateway.gateway();
        let (exec_rx, pnl_rx) = {
            let guard = shared_gateway.read().await;
            let exec_rx = guard.subscribe_execution_events();
            let pnl_rx = guard.subscribe_pnl_events();
            (exec_rx, pnl_rx)
        };
        gateway
            .connect(&gateway_config)
            .await
            .map_err(|e| anyhow::anyhow!("Gateway connect failed: {e}"))?;

        let inner = Arc::new(RithmicExecutionClient::new(
            Arc::clone(&shared_gateway),
            RithmicAccount::new(
                self.config.fcm_id.clone().unwrap_or_default(),
                self.config.ib_id.clone().unwrap_or_default(),
                self.config.account_id.clone(),
            ),
        ));

        let task = get_runtime().spawn(run_execution_event_loop(
            exec_rx,
            pnl_rx,
            ExecutionEventLoopContext {
                gateway: Arc::clone(&shared_gateway),
                inner: Arc::clone(&inner),
                account_id: self.core.account_id,
                emitter: self.emitter.clone(),
                is_connected: Arc::clone(&self.is_connected),
                order_reports_by_client: Arc::clone(&self.order_reports_by_client),
                order_reports_by_venue: Arc::clone(&self.order_reports_by_venue),
                fill_reports: Arc::clone(&self.fill_reports),
                position_reports: Arc::clone(&self.position_reports),
                replay_lookback_secs: self.config.execution_replay_lookback_secs,
            },
        ));

        self.inner = Some(inner);
        self.gateway = Some(gateway);
        self.event_task = Some(task);
        self.is_connected.store(true, Ordering::Relaxed);
        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Self::bootstrap_connection(
            self.require_inner()?,
            self.config.execution_replay_lookback_secs,
        )
        .await?;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.abort_pending_tasks();

        if let Some(task) = self.event_task.take() {
            task.abort();
        }
        self.inner = None;

        if let Some(mut gateway) = self.gateway.take() {
            gateway.release().await;
        }
        self.clear_cached_reports();
        self.is_connected.store(false, Ordering::Relaxed);
        self.resolved_exchanges.write().clear();
        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self
            .core
            .cache()
            .order_owned(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", cmd.client_order_id))?;
        let inner = self.require_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let o = &cmd.order_init;

        let trailing_stop = o.trailing_offset.map(|offset| TrailingStopConfig {
            trail_by_ticks: offset.to_string().parse::<f64>().map_or(0, |f| f as i32),
        });
        self.emitter.emit_order_submitted(&order);

        let emitter = self.emitter.clone();
        let order_for_reject = order;
        let client_order_id = cmd.client_order_id.to_string();
        let instrument_id = cmd.instrument_id;
        let order_side = o.order_side;
        let order_type = o.order_type;
        let time_in_force = o.time_in_force;
        let quantity = f64::from(o.quantity);
        let price = o.price.map(f64::from);
        let stop_price = o.trigger_price.map(f64::from);

        self.spawn_task("submit_order", async move {
            let result: anyhow::Result<()> = async {
                let (symbol, exchange) =
                    resolve_contract_exchange(&gateway, &resolved_exchanges, &instrument_id)
                        .await?;
                let request = OrderRequest {
                    client_order_id,
                    symbol,
                    exchange,
                    side: to_rithmic_side(order_side)?,
                    order_type: to_rithmic_order_type(order_type)?,
                    time_in_force: to_rithmic_tif(time_in_force)?,
                    quantity,
                    price,
                    stop_price,
                    trailing_stop,
                };
                inner.submit_order(request).await.map_err(Into::into)
            }
            .await;

            if let Err(e) = result {
                emitter.emit_order_rejected(
                    &order_for_reject,
                    &e.to_string(),
                    get_atomic_clock_realtime().get_time_ns(),
                    false,
                );
                return Err(e);
            }
            Ok(())
        });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;

        match self.build_native_bracket_spec(&orders) {
            Ok(Some(spec)) => return self.submit_native_bracket_order_list(&spec),
            Ok(None) => {}
            Err(e) => {
                self.deny_order_list(&orders, &e.to_string());
                return Ok(());
            }
        }

        match self.build_native_oco_spec(&orders) {
            Ok(Some(spec)) => return self.submit_native_oco_order_list(&spec),
            Ok(None) => {}
            Err(e) => {
                self.deny_order_list(&orders, &e.to_string());
                return Ok(());
            }
        }

        self.deny_order_list(
            &orders,
            "UNSUPPORTED_ORDER_LIST_FOR_RITHMIC_HIGH_LEVEL_CLIENT",
        );
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let cached_order = self.core.cache().order_owned(&cmd.client_order_id);
        let inner = self.require_inner()?;
        let client_order_id = cmd.client_order_id.to_string();
        let new_qty = cmd.quantity.map(f64::from);
        let new_price = cmd.price.map(f64::from);

        if cmd.trigger_price.is_some() {
            log::warn!(
                "modify_order: trigger_price modification is not supported by rithmic-rs \
                (RithmicModifyOrder has no stop_price field). \
                trigger_price will be ignored for order {}",
                cmd.client_order_id
            );
        }

        let emitter = self.emitter.clone();
        let command = cmd;
        self.spawn_task("modify_order", async move {
            if let Err(e) = inner
                .modify_order(&client_order_id, new_qty, new_price)
                .await
            {
                if let Some(order) = cached_order.as_ref() {
                    emitter.emit_order_modify_rejected(
                        order,
                        order.venue_order_id(),
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                } else {
                    emitter.emit_order_modify_rejected_event(
                        command.strategy_id,
                        command.instrument_id,
                        command.client_order_id,
                        command.venue_order_id,
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                return Err(anyhow::Error::from(e));
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cached_order = self.core.cache().order_owned(&cmd.client_order_id);
        let inner = self.require_inner()?;
        let client_order_id = cmd.client_order_id.to_string();

        let emitter = self.emitter.clone();
        let command = cmd;
        self.spawn_task("cancel_order", async move {
            if let Err(e) = inner.cancel_order(&client_order_id).await {
                if let Some(order) = cached_order.as_ref() {
                    emitter.emit_order_cancel_rejected(
                        order,
                        order.venue_order_id(),
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                } else {
                    emitter.emit_order_cancel_rejected_event(
                        command.strategy_id,
                        command.instrument_id,
                        command.client_order_id,
                        command.venue_order_id,
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                return Err(anyhow::Error::from(e));
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_all_orders(&self, _cmd: CancelAllOrders) -> anyhow::Result<()> {
        let inner = self.require_inner()?;

        self.spawn_task("cancel_all_orders", async move {
            inner.cancel_all_orders().await.map_err(Into::into)
        });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let inner = self.require_inner()?;
        let ids: Vec<String> = cmd
            .cancels
            .iter()
            .map(|o| o.client_order_id.to_string())
            .collect();

        self.spawn_task("batch_cancel_orders", async move {
            let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            inner
                .batch_cancel_orders(&id_refs)
                .await
                .map(|_| ())
                .map_err(Into::into)
        });
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let inner = self.require_inner()?;

        self.spawn_task("query_account", async move {
            inner.request_pnl_snapshot().await.map_err(Into::into)
        });
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        if let Some(report) = self
            .order_reports_by_client
            .get(&cmd.client_order_id)
            .map(|entry| entry.clone())
            .or_else(|| {
                cmd.venue_order_id
                    .and_then(|venue_order_id| self.order_reports_by_venue.get(&venue_order_id))
                    .map(|entry| entry.clone())
            })
        {
            self.emitter.send_order_status_report(report);
            return Ok(());
        }

        if let Some(order) = self.core.cache().order_owned(&cmd.client_order_id)
            && let Some(report) =
                report_from_cache_order(&self.core, &order, self.clock.get_time_ns())
        {
            self.emitter.send_order_status_report(report);
            return Ok(());
        }

        if let Some(local_order) = self
            .require_inner()?
            .get_order(cmd.client_order_id.as_str())
            && let Some(report) = report_from_local_order_state(
                self.core.account_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &local_order,
                to_model_order_status(local_order.status),
                self.clock.get_time_ns(),
                self.clock.get_time_ns(),
            )
        {
            self.emitter.send_order_status_report(report);
            return Ok(());
        }

        let inner = self.require_inner()?;
        self.spawn_task("query_order", async move {
            inner.query_orders().await.map_err(Into::into)
        });
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if let Some(client_order_id) = cmd.client_order_id {
            if let Some(report) = self.order_reports_by_client.get(&client_order_id) {
                return Ok(Some(report.clone()));
            }

            if let Some(order) = self.core.cache().order_owned(&client_order_id) {
                return Ok(report_from_cache_order(
                    &self.core,
                    &order,
                    self.clock.get_time_ns(),
                ));
            }
        }

        if let Some(venue_order_id) = cmd.venue_order_id {
            if let Some(report) = self.order_reports_by_venue.get(&venue_order_id) {
                return Ok(Some(report.clone()));
            }

            let cache_order = {
                let cache = self.core.cache();
                cache
                    .client_order_id(&venue_order_id)
                    .and_then(|client_order_id| cache.order(client_order_id))
                    .map(|order| order.cloned())
            };

            if let Some(order) = cache_order {
                return Ok(report_from_cache_order(
                    &self.core,
                    &order,
                    self.clock.get_time_ns(),
                ));
            }
        }

        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut reports = Vec::new();
        let mut seen = AHashSet::new();

        for entry in self.order_reports_by_client.iter() {
            let report = entry.value().clone();

            if !matches_order_report(&report, cmd) {
                continue;
            }
            let key = order_report_key(&report);

            if seen.insert(key) {
                reports.push(report);
            }
        }

        let cache_orders: Vec<OrderAny> = self
            .core
            .cache()
            .orders(Some(&self.core.venue), None, None, None, None)
            .into_iter()
            .map(|order| order.cloned())
            .collect();

        for order in cache_orders {
            let Some(report) =
                report_from_cache_order(&self.core, &order, self.clock.get_time_ns())
            else {
                continue;
            };

            if !matches_order_report(&report, cmd) {
                continue;
            }
            let key = order_report_key(&report);

            if seen.insert(key) {
                reports.push(report);
            }
        }

        sort_order_reports(&mut reports);
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let mut reports = Vec::new();

        for entry in self.fill_reports.iter() {
            let report = entry.value().clone();

            if cmd
                .instrument_id
                .is_some_and(|instrument_id| report.instrument_id != instrument_id)
            {
                continue;
            }

            if cmd
                .venue_order_id
                .is_some_and(|venue_order_id| report.venue_order_id != venue_order_id)
            {
                continue;
            }

            if !within_time_range(report.ts_event, cmd.start, cmd.end) {
                continue;
            }
            reports.push(report);
        }
        sort_fill_reports(&mut reports);
        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut reports = Vec::new();

        for entry in self.position_reports.iter() {
            let report = entry.value().clone();

            if cmd
                .instrument_id
                .is_some_and(|instrument_id| report.instrument_id != instrument_id)
            {
                continue;
            }

            if !within_time_range(report.ts_last, cmd.start, cmd.end) {
                continue;
            }
            reports.push(report);
        }
        sort_position_reports(&mut reports);
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_now = self.clock.get_time_ns();
        if lookback_mins.is_none() {
            return Ok(Some(self.build_current_state_mass_status(ts_now).await?));
        }

        let start = lookback_mins
            .map(|mins| UnixNanos::from(ts_now.as_u64().saturating_sub(mins * 60 * 1_000_000_000)));

        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(false)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_fill_reports(fill_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        self.cache_missing_instruments_for_reports(
            &order_reports,
            &fill_reports,
            &position_reports,
        )
        .await;

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            ts_now,
            None,
        );
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);
        Ok(Some(mass_status))
    }
}

async fn run_execution_event_loop(
    mut exec_rx: broadcast::Receiver<ExecutionEvent>,
    mut pnl_rx: broadcast::Receiver<PnlEvent>,
    ctx: ExecutionEventLoopContext,
) {
    let ExecutionEventLoopContext {
        gateway,
        inner,
        account_id,
        emitter,
        is_connected,
        order_reports_by_client,
        order_reports_by_venue,
        fill_reports,
        position_reports,
        replay_lookback_secs,
    } = ctx;

    loop {
        tokio::select! {
            maybe_event = exec_rx.recv() => {
                let event = match maybe_event {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log::warn!("Rithmic execution subscriber lagged by {skipped} events");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("Rithmic execution channel closed");
                        is_connected.store(false, Ordering::Relaxed);
                        break;
                    }
                };

                if !event_matches_execution_client(&inner, &event) {
                    continue;
                }

                inner.apply_event(&event);

                match event {
                    ExecutionEvent::ConnectionState(ConnectionState::Reconnecting) => {
                        is_connected.store(false, Ordering::Relaxed);
                        let reconnect_result = {
                            let mut gateway = gateway.write().await;
                            gateway.reconnect_if_needed().await
                        };

                        match reconnect_result {
                            Ok(()) => {
                                is_connected.store(true, Ordering::Relaxed);

                                if let Err(e) = RithmicLiveExecClient::bootstrap_connection(
                                    Arc::clone(&inner),
                                    replay_lookback_secs,
                                ).await {
                                    log::error!("Rithmic execution bootstrap after reconnect failed: {e}");
                                    break;
                                }
                            }
                            Err(e) => {
                                log::error!("Rithmic execution reconnect failed: {e}");
                                break;
                            }
                        }
                    }
                    ExecutionEvent::Reconnected => {
                        log::info!("Rithmic execution gateway reconnected");
                    }
                    ExecutionEvent::Authenticated => {
                        log::info!("Rithmic execution gateway authenticated");
                    }
                    ExecutionEvent::Error(e) => {
                        log::error!("Rithmic execution error: {e}");
                    }
                    other => {
                        process_execution_update(
                            &inner,
                            account_id,
                            &emitter,
                            &order_reports_by_client,
                            &order_reports_by_venue,
                            &fill_reports,
                            other,
                        );
                    }
                }
            }
            maybe_pnl = pnl_rx.recv() => {
                let event = match maybe_pnl {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log::warn!("Rithmic pnl subscriber lagged by {skipped} events");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("Rithmic pnl channel closed");
                        break;
                    }
                };
                process_pnl_update(
                    account_id,
                    inner.account_id(),
                    &emitter,
                    &position_reports,
                    event,
                );
            }
        }
    }
}

fn event_matches_execution_client(inner: &RithmicExecutionClient, event: &ExecutionEvent) -> bool {
    match event {
        ExecutionEvent::ConnectionState(_)
        | ExecutionEvent::Reconnected
        | ExecutionEvent::Authenticated
        | ExecutionEvent::Error(_) => true,
        _ => match event
            .account_id()
            .filter(|account_id| !account_id.is_empty())
        {
            Some(account_id) => account_id == inner.account_id(),
            None => {
                event_client_order_id(event)
                    .is_some_and(|client_order_id| inner.get_order(client_order_id).is_some())
                    || event_venue_order_id(event).is_some_and(|venue_order_id| {
                        inner.get_client_order_id(venue_order_id).is_some()
                    })
            }
        },
    }
}

fn event_client_order_id(event: &ExecutionEvent) -> Option<&str> {
    match event {
        ExecutionEvent::Submitted(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::Accepted(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::Rejected(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::Filled(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::Cancelled(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::Modified(event) => Some(event.client_order_id.as_str()),
        ExecutionEvent::ConnectionState(_)
        | ExecutionEvent::Reconnected
        | ExecutionEvent::Authenticated
        | ExecutionEvent::Error(_) => None,
    }
}

fn event_venue_order_id(event: &ExecutionEvent) -> Option<&str> {
    match event {
        ExecutionEvent::Submitted(event) => event.venue_order_id.as_deref(),
        ExecutionEvent::Accepted(event) => Some(event.venue_order_id.as_str()),
        ExecutionEvent::Filled(event) => Some(event.venue_order_id.as_str()),
        ExecutionEvent::Cancelled(event) => Some(event.venue_order_id.as_str()),
        ExecutionEvent::Modified(event) => Some(event.venue_order_id.as_str()),
        ExecutionEvent::Rejected(_)
        | ExecutionEvent::ConnectionState(_)
        | ExecutionEvent::Reconnected
        | ExecutionEvent::Authenticated
        | ExecutionEvent::Error(_) => None,
    }
}

async fn ensure_cached_instrument(
    gateway: &Arc<RwLock<RithmicGateway>>,
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    instrument_id: InstrumentId,
) -> anyhow::Result<()> {
    let (symbol, exchange) =
        resolve_contract_exchange(gateway, resolved_exchanges, &instrument_id).await?;
    let ticker = {
        let gateway = gateway.read().await;
        gateway.ticker_handle().cloned()
    }
    .ok_or_else(|| anyhow::anyhow!("Ticker handle not available for instrument preload"))?;

    let response = ticker
        .get_reference_data(&symbol, &exchange)
        .await
        .map_err(|e| anyhow::anyhow!("get_reference_data failed for {instrument_id}: {e}"))?;

    if let Some(e) = &response.error {
        anyhow::bail!("Reference data error for {instrument_id}: {e}");
    }

    let RithmicMessage::ResponseReferenceData(ref_data) = &response.message else {
        anyhow::bail!("Unexpected response type for {instrument_id} reference data");
    };

    let ts_init = get_atomic_clock_realtime().get_time_ns();
    let mut instrument = response_to_instrument(ref_data, ts_init)
        .map_err(|e| anyhow::anyhow!("Failed to parse instrument {instrument_id}: {e}"))?;

    if let Ok(aux_resp) = ticker
        .get_auxilliary_reference_data(&symbol, &exchange)
        .await
        && let RithmicMessage::ResponseAuxilliaryReferenceData(aux) = &aux_resp.message
    {
        apply_auxiliary_reference_data(&mut instrument, aux);
    }

    Ok(())
}

struct ExecutionEventLoopContext {
    gateway: Arc<RwLock<RithmicGateway>>,
    inner: Arc<RithmicExecutionClient>,
    account_id: AccountId,
    emitter: ExecutionEventEmitter,
    is_connected: Arc<AtomicBool>,
    order_reports_by_client: Arc<DashMap<ClientOrderId, OrderStatusReport>>,
    order_reports_by_venue: Arc<DashMap<VenueOrderId, OrderStatusReport>>,
    fill_reports: Arc<DashMap<String, FillReport>>,
    position_reports: Arc<DashMap<String, PositionStatusReport>>,
    replay_lookback_secs: u64,
}

fn process_execution_update(
    inner: &RithmicExecutionClient,
    account_id: AccountId,
    emitter: &ExecutionEventEmitter,
    order_reports_by_client: &DashMap<ClientOrderId, OrderStatusReport>,
    order_reports_by_venue: &DashMap<VenueOrderId, OrderStatusReport>,
    fill_reports: &DashMap<String, FillReport>,
    event: ExecutionEvent,
) {
    match event {
        ExecutionEvent::Submitted(event) => {
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                event.venue_order_id.as_deref(),
            );
            let report = coalesce_snapshot_report(
                build_submitted_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = report {
                store_order_report(
                    order_reports_by_client,
                    order_reports_by_venue,
                    report.clone(),
                );

                if !event.context.is_snapshot && local_order.is_none() {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Accepted(event) => {
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            let report = coalesce_snapshot_report(
                build_accept_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = report.clone() {
                store_order_report(order_reports_by_client, order_reports_by_venue, report);
            }

            if !event.context.is_snapshot
                && let Some(report) = report
            {
                emitter.send_order_status_report(report);
            }
        }
        ExecutionEvent::Rejected(event) => {
            let local_order = local_order_for_event(inner, event.client_order_id.as_str(), None);
            let report = coalesce_snapshot_report(
                build_rejected_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = report.clone() {
                store_order_report(order_reports_by_client, order_reports_by_venue, report);
            }

            if !event.context.is_snapshot
                && let Some(report) = report
            {
                emitter.send_order_status_report(report);
            }
        }
        ExecutionEvent::Modified(event) => {
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            let report = coalesce_snapshot_report(
                build_modified_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = report.clone() {
                store_order_report(order_reports_by_client, order_reports_by_venue, report);
            }

            if !event.context.is_snapshot
                && let Some(report) = report
            {
                emitter.send_order_status_report(report);
            }
        }
        ExecutionEvent::Cancelled(event) => {
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            let report = coalesce_snapshot_report(
                build_cancelled_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = report.clone() {
                store_order_report(order_reports_by_client, order_reports_by_venue, report);
            }

            if !event.context.is_snapshot
                && let Some(report) = report
            {
                emitter.send_order_status_report(report);
            }
        }
        ExecutionEvent::Filled(event) => {
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            let fill_report = build_fill_report(
                account_id,
                &event,
                local_order.as_ref(),
                order_reports_by_client,
                order_reports_by_venue,
            );
            let fill_key = fill_report.as_ref().map(fill_report_key);

            if let (Some(key), Some(report)) = (fill_key, fill_report.clone()) {
                fill_reports.insert(key, report);
            }

            let order_report = coalesce_snapshot_report(
                build_fill_status_report(
                    account_id,
                    &event,
                    local_order.as_ref(),
                    order_reports_by_client,
                    order_reports_by_venue,
                    fill_reports,
                ),
                event.context.is_snapshot,
                order_reports_by_client,
                order_reports_by_venue,
            );

            if let Some(report) = order_report.clone() {
                store_order_report(order_reports_by_client, order_reports_by_venue, report);
            }

            if !event.context.is_snapshot
                && let Some(report) = fill_report
            {
                emitter.send_fill_report(report);
            }

            if !event.context.is_snapshot
                && let Some(report) = order_report
            {
                emitter.send_order_status_report(report);
            }
        }
        ExecutionEvent::ConnectionState(_)
        | ExecutionEvent::Reconnected
        | ExecutionEvent::Authenticated
        | ExecutionEvent::Error(_) => {}
    }
}

fn process_pnl_update(
    account_id: AccountId,
    venue_account_id: &str,
    emitter: &ExecutionEventEmitter,
    position_reports: &DashMap<String, PositionStatusReport>,
    event: PnlEvent,
) {
    match event {
        PnlEvent::Account(ProviderAccountEvent::BalanceUpdate(balance)) => {
            if balance.account_id != venue_account_id {
                return;
            }
            let currency = parse_currency(&balance.currency).unwrap_or_else(Currency::USD);
            let total = Money::new(balance.total, currency);
            let locked = Money::new(balance.locked, currency);
            let free = total - locked;
            emitter.emit_account_state(
                vec![AccountBalance::new(total, locked, free)],
                Vec::new(),
                true,
                UnixNanos::from(balance.ts_event),
            );
        }
        PnlEvent::Account(ProviderAccountEvent::Error(e)) => {
            log::error!("Rithmic pnl account error: {e}");
        }
        PnlEvent::Account(ProviderAccountEvent::MarginWarning {
            account_id: warning_account_id,
            message,
        }) => {
            if warning_account_id != venue_account_id {
                return;
            }
            log::warn!("Rithmic pnl margin warning for {account_id}: {message}");
        }
        PnlEvent::Position(
            ProviderPositionEvent::Opened(position) | ProviderPositionEvent::Updated(position),
        ) => {
            if position.account_id != venue_account_id {
                return;
            }
            let instrument_id = make_instrument_id(&position.symbol, &position.exchange);
            let quantity = make_quantity(position.quantity.abs(), 0);
            let position_side = if position.quantity > 0.0 {
                PositionSideSpecified::Long
            } else if position.quantity < 0.0 {
                PositionSideSpecified::Short
            } else {
                PositionSideSpecified::Flat
            };
            let avg_px_open = if position.quantity == 0.0 {
                None
            } else {
                Decimal::from_f64_retain(position.avg_price)
            };
            let report = PositionStatusReport::new(
                account_id,
                instrument_id,
                position_side,
                quantity,
                UnixNanos::from(position.ts_event),
                get_atomic_clock_realtime().get_time_ns(),
                None,
                None,
                avg_px_open,
            );
            position_reports.insert(instrument_id.to_string(), report);
        }
        PnlEvent::Position(ProviderPositionEvent::Closed {
            account_id: closed_account_id,
            symbol,
            exchange,
            ..
        }) => {
            if closed_account_id != venue_account_id {
                return;
            }
            let instrument_id = make_instrument_id(&symbol, &exchange);
            position_reports.remove(&instrument_id.to_string());
        }
        PnlEvent::Position(ProviderPositionEvent::Error(e)) => {
            log::error!("Rithmic pnl position error: {e}");
        }
    }
}

fn parse_currency(value: &str) -> Option<Currency> {
    Currency::from_str(value).ok()
}

fn report_from_cache_order(
    core: &ExecutionClientCore,
    order: &OrderAny,
    ts_init: UnixNanos,
) -> Option<OrderStatusReport> {
    let venue_order_id = order.venue_order_id()?;
    let mut report = OrderStatusReport::new(
        core.account_id,
        order.instrument_id(),
        Some(order.client_order_id()),
        venue_order_id,
        order.order_side(),
        order.order_type(),
        order.time_in_force(),
        order.status(),
        order.quantity(),
        order.filled_qty(),
        order.ts_accepted().unwrap_or(order.ts_init()),
        order.ts_last(),
        ts_init,
        None,
    );
    report.price = order.price();
    report.trigger_price = order.trigger_price();
    report.avg_px = order.avg_px();
    report.post_only = order.is_post_only();
    report.reduce_only = order.is_reduce_only();
    report.venue_position_id = order.position_id();
    report.order_list_id = order.order_list_id();
    report.parent_order_id = order.parent_order_id();
    report.linked_order_ids = order.linked_order_ids().map(|ids| ids.to_vec());
    report.expire_time = order.expire_time();
    sanitize_order_report(report, "cache")
}

fn infer_price_precision(value: f64) -> u8 {
    let s = format!("{value:.12}");
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split('.')
        .nth(1)
        .map_or(0, |fraction| fraction.len() as u8)
}

fn report_from_local_order_state(
    account_id: AccountId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    order: &OrderState,
    order_status: OrderStatus,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Option<OrderStatusReport> {
    let venue_order_id =
        venue_order_id.or_else(|| order.venue_order_id.as_deref().map(VenueOrderId::from))?;
    let instrument_id = make_instrument_id(&order.symbol, &order.exchange);
    let price_precision = order
        .price
        .or(order.trigger_price)
        .map_or(2, infer_price_precision);

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        Some(client_order_id),
        venue_order_id,
        to_model_side(order.side),
        to_model_order_type(order.order_type),
        to_model_tif(order.time_in_force),
        order_status,
        make_quantity(order.quantity, 0),
        make_quantity(order.filled_qty, 0),
        ts_event,
        ts_event,
        ts_init,
        None,
    );
    report.price = resolve_price(order.price, None, None, price_precision);
    report.trigger_price = resolve_price(order.trigger_price, None, None, price_precision);
    sanitize_order_report(report, "local")
}

fn store_order_report(
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
    report: OrderStatusReport,
) {
    if let Some(client_order_id) = report.client_order_id {
        client_reports.insert(client_order_id, report.clone());
    }
    venue_reports.insert(report.venue_order_id, report);
}

fn find_existing_report(
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
    client_order_id: Option<ClientOrderId>,
    venue_order_id: Option<VenueOrderId>,
) -> Option<OrderStatusReport> {
    if let Some(client_order_id) = client_order_id
        && let Some(report) = client_reports.get(&client_order_id)
    {
        return Some(report.clone());
    }

    if let Some(venue_order_id) = venue_order_id
        && let Some(report) = venue_reports.get(&venue_order_id)
    {
        return Some(report.clone());
    }
    None
}

fn coalesce_snapshot_report(
    report: Option<OrderStatusReport>,
    is_snapshot: bool,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let report = sanitize_order_report(report?, if is_snapshot { "snapshot" } else { "live" })?;
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        report.client_order_id,
        Some(report.venue_order_id),
    );
    let Some(previous) = previous else {
        return Some(report);
    };

    if report.filled_qty < previous.filled_qty {
        return None;
    }
    let previous_rank = order_status_rank(previous.order_status);
    let report_rank = order_status_rank(report.order_status);

    if report_rank < previous_rank && report.filled_qty <= previous.filled_qty {
        return None;
    }

    if previous.order_status == OrderStatus::Filled && report.order_status != OrderStatus::Filled {
        return None;
    }

    if previous.order_status == OrderStatus::PartiallyFilled
        && matches!(
            report.order_status,
            OrderStatus::Submitted | OrderStatus::Accepted
        )
    {
        return None;
    }

    if !is_snapshot && is_equivalent_live_report(&previous, &report) {
        return None;
    }

    Some(report)
}

fn order_status_rank(status: OrderStatus) -> u8 {
    match status {
        OrderStatus::Initialized
        | OrderStatus::Submitted
        | OrderStatus::Emulated
        | OrderStatus::Released => 0,
        OrderStatus::Denied | OrderStatus::Rejected => 1,
        OrderStatus::Accepted
        | OrderStatus::PendingUpdate
        | OrderStatus::PendingCancel
        | OrderStatus::Triggered => 2,
        OrderStatus::PartiallyFilled => 3,
        OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Voided => 4,
        OrderStatus::Filled => 5,
    }
}

fn is_equivalent_live_report(previous: &OrderStatusReport, report: &OrderStatusReport) -> bool {
    previous.order_status == report.order_status
        && previous.quantity == report.quantity
        && previous.filled_qty == report.filled_qty
        && previous.price == report.price
        && previous.trigger_price == report.trigger_price
        && previous.avg_px == report.avg_px
        && previous.cancel_reason == report.cancel_reason
}

fn order_report_key(report: &OrderStatusReport) -> String {
    format!(
        "{}:{}",
        report
            .client_order_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        report.venue_order_id
    )
}

fn fill_report_key(report: &FillReport) -> String {
    format!("{}:{}", report.venue_order_id, report.trade_id)
}

fn matches_order_report(report: &OrderStatusReport, cmd: &GenerateOrderStatusReports) -> bool {
    if cmd
        .instrument_id
        .is_some_and(|instrument_id| report.instrument_id != instrument_id)
    {
        return false;
    }

    if cmd.open_only && !report.order_status.is_open() {
        return false;
    }
    within_time_range(report.ts_last, cmd.start, cmd.end)
}

fn within_time_range(
    ts_event: UnixNanos,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> bool {
    if start.is_some_and(|start| ts_event < start) {
        return false;
    }

    if end.is_some_and(|end| ts_event > end) {
        return false;
    }
    true
}

fn sort_order_reports(reports: &mut [OrderStatusReport]) {
    reports.sort_by(|a, b| {
        (
            a.ts_accepted,
            a.ts_last,
            a.client_order_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
            a.venue_order_id.to_string(),
        )
            .cmp(&(
                b.ts_accepted,
                b.ts_last,
                b.client_order_id
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                b.venue_order_id.to_string(),
            ))
    });
}

fn sort_fill_reports(reports: &mut [FillReport]) {
    reports.sort_by(|a, b| {
        (
            a.ts_event,
            a.trade_id.to_string(),
            a.venue_order_id.to_string(),
        )
            .cmp(&(
                b.ts_event,
                b.trade_id.to_string(),
                b.venue_order_id.to_string(),
            ))
    });
}

fn sort_position_reports(reports: &mut [PositionStatusReport]) {
    reports.sort_by(|a, b| {
        (a.ts_last, a.instrument_id.to_string()).cmp(&(b.ts_last, b.instrument_id.to_string()))
    });
}

fn make_instrument_id(symbol: &str, _exchange: &str) -> InstrumentId {
    crate::common::converters::rithmic_instrument_id(symbol)
}

fn make_quantity(value: f64, precision: u8) -> Quantity {
    Quantity::new(value, precision)
}

fn make_price(value: f64, precision: u8) -> Price {
    Price::new(value, precision)
}

fn resolve_instrument_id(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> Option<InstrumentId> {
    previous
        .map(|report| report.instrument_id)
        .or_else(|| local_order.map(|order| make_instrument_id(&order.symbol, &order.exchange)))
        .or_else(|| {
            context
                .symbol
                .as_deref()
                .zip(context.exchange.as_deref())
                .map(|(symbol, exchange)| make_instrument_id(symbol, exchange))
        })
}

fn resolve_order_side(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> OrderSide {
    previous
        .map(|report| report.order_side)
        .or_else(|| local_order.map(|order| to_model_side(order.side)))
        .or_else(|| context.side.map(to_model_side))
        .unwrap_or(OrderSide::Buy)
}

fn resolve_order_type(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> OrderType {
    previous
        .map(|report| report.order_type)
        .or_else(|| local_order.map(|order| to_model_order_type(order.order_type)))
        .or_else(|| context.order_type.map(to_model_order_type))
        .unwrap_or(OrderType::Market)
}

fn resolve_time_in_force(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> TimeInForce {
    previous
        .map(|report| report.time_in_force)
        .or_else(|| local_order.map(|order| to_model_tif(order.time_in_force)))
        .or_else(|| context.time_in_force.map(to_model_tif))
        .unwrap_or(TimeInForce::Day)
}

fn resolve_quantity(
    value: Option<f64>,
    fallback: Option<Quantity>,
    local_value: Option<f64>,
    precision: u8,
) -> Quantity {
    if let Some(fallback) = fallback {
        return fallback;
    }

    if let Some(value) = value {
        return make_quantity(value, precision);
    }

    if let Some(local_value) = local_value {
        return make_quantity(local_value, precision);
    }
    make_quantity(0.0, precision)
}

fn resolve_price(
    value: Option<f64>,
    fallback: Option<Price>,
    local_value: Option<f64>,
    precision: u8,
) -> Option<Price> {
    fallback.or_else(|| {
        value
            .filter(|value| *value > 0.0)
            .map(|value| make_price(value, precision))
            .or_else(|| {
                local_value
                    .filter(|value| *value > 0.0)
                    .map(|value| make_price(value, precision))
            })
    })
}

fn resolve_avg_px(value: Option<f64>) -> Option<Decimal> {
    value.and_then(Decimal::from_f64_retain)
}

fn local_order_for_event(
    inner: &RithmicExecutionClient,
    client_order_id: &str,
    venue_order_id: Option<&str>,
) -> Option<OrderState> {
    inner.get_order(client_order_id).or_else(|| {
        venue_order_id
            .and_then(|venue_order_id| inner.get_client_order_id(venue_order_id))
            .and_then(|client_order_id| inner.get_order(&client_order_id))
    })
}

fn order_type_requires_trigger_price(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
    )
}

fn missing_required_order_pricing(report: &OrderStatusReport) -> Option<&'static str> {
    let missing_price = LIMIT_ORDER_TYPES.contains(&report.order_type) && report.price.is_none();
    let missing_trigger =
        order_type_requires_trigger_price(report.order_type) && report.trigger_price.is_none();

    match (missing_price, missing_trigger) {
        (true, true) => Some("price and trigger_price"),
        (true, false) => Some("price"),
        (false, true) => Some("trigger_price"),
        (false, false) => None,
    }
}

fn sanitize_order_report(report: OrderStatusReport, source: &str) -> Option<OrderStatusReport> {
    let Some(missing) = missing_required_order_pricing(&report) else {
        return Some(report);
    };
    log::warn!(
        "Dropping invalid Rithmic {source} order report: client_order_id={:?}, venue_order_id={}, instrument_id={}, order_type={:?}, order_status={:?}, missing={missing}",
        report.client_order_id,
        report.venue_order_id,
        report.instrument_id,
        report.order_type,
        report.order_status,
    );
    None
}

fn build_submitted_report(
    account_id: AccountId,
    event: &crate::execution::OrderSubmitted,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let venue_order_id = event.venue_order_id.as_deref().map(VenueOrderId::from)?;
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        Some(venue_order_id),
    );
    let instrument_id = resolve_instrument_id(previous.as_ref(), local_order, &event.context)?;
    let quantity_precision = previous
        .as_ref()
        .map_or(0, |report| report.quantity.precision);
    let price_precision = previous
        .as_ref()
        .and_then(|report| report.price.map(|price| price.precision))
        .or_else(|| {
            previous
                .as_ref()
                .and_then(|report| report.trigger_price.map(|price| price.precision))
        })
        .unwrap_or(2);
    Some(OrderStatusReport {
        account_id,
        instrument_id,
        client_order_id: Some(ClientOrderId::from(event.client_order_id.as_str())),
        venue_order_id,
        order_side: resolve_order_side(previous.as_ref(), local_order, &event.context),
        order_type: resolve_order_type(previous.as_ref(), local_order, &event.context),
        time_in_force: resolve_time_in_force(previous.as_ref(), local_order, &event.context),
        order_status: OrderStatus::Submitted,
        quantity: resolve_quantity(
            event.context.quantity,
            previous.as_ref().map(|r| r.quantity),
            local_order.map(|order| order.quantity),
            quantity_precision,
        ),
        filled_qty: resolve_quantity(
            event.context.filled_qty,
            previous.as_ref().map(|r| r.filled_qty),
            local_order.map(|order| order.filled_qty),
            quantity_precision,
        ),
        report_id: UUID4::new(),
        ts_accepted: previous
            .as_ref()
            .map_or_else(|| UnixNanos::from(event.ts_event), |r| r.ts_accepted),
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        order_list_id: previous.as_ref().and_then(|r| r.order_list_id),
        venue_position_id: previous.as_ref().and_then(|r| r.venue_position_id),
        linked_order_ids: previous.as_ref().and_then(|r| r.linked_order_ids.clone()),
        parent_order_id: previous.as_ref().and_then(|r| r.parent_order_id),
        contingency_type: previous
            .as_ref()
            .map(|r| r.contingency_type)
            .unwrap_or_default(),
        expire_time: previous.as_ref().and_then(|r| r.expire_time),
        price: resolve_price(
            event.context.price,
            previous.as_ref().and_then(|r| r.price),
            local_order.and_then(|order| order.price),
            price_precision,
        ),
        trigger_price: resolve_price(
            event.context.trigger_price,
            previous.as_ref().and_then(|r| r.trigger_price),
            local_order.and_then(|order| order.trigger_price),
            price_precision,
        ),
        activation_price: previous.as_ref().and_then(|r| r.activation_price),
        trigger_type: previous.as_ref().and_then(|r| r.trigger_type),
        limit_offset: previous.as_ref().and_then(|r| r.limit_offset),
        trailing_offset: previous.as_ref().and_then(|r| r.trailing_offset),
        trailing_offset_type: previous
            .as_ref()
            .map(|r| r.trailing_offset_type)
            .unwrap_or_default(),
        avg_px: resolve_avg_px(event.context.avg_price)
            .or_else(|| previous.as_ref().and_then(|r| r.avg_px)),
        display_qty: previous.as_ref().and_then(|r| r.display_qty),
        post_only: previous.as_ref().is_some_and(|r| r.post_only),
        reduce_only: previous.as_ref().is_some_and(|r| r.reduce_only),
        cancel_reason: None,
        ts_triggered: previous.as_ref().and_then(|r| r.ts_triggered),
    })
}

fn build_accept_report(
    account_id: AccountId,
    event: &crate::execution::OrderAccepted,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let venue_order_id = VenueOrderId::from(event.venue_order_id.as_str());
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        Some(venue_order_id),
    );
    let instrument_id = resolve_instrument_id(previous.as_ref(), local_order, &event.context)?;
    let quantity_precision = previous
        .as_ref()
        .map_or(0, |report| report.quantity.precision);
    let price_precision = previous
        .as_ref()
        .and_then(|report| report.price.map(|price| price.precision))
        .or_else(|| {
            previous
                .as_ref()
                .and_then(|report| report.trigger_price.map(|price| price.precision))
        })
        .unwrap_or(2);
    Some(OrderStatusReport {
        account_id,
        instrument_id,
        client_order_id: Some(ClientOrderId::from(event.client_order_id.as_str())),
        venue_order_id,
        order_side: resolve_order_side(previous.as_ref(), local_order, &event.context),
        order_type: resolve_order_type(previous.as_ref(), local_order, &event.context),
        time_in_force: resolve_time_in_force(previous.as_ref(), local_order, &event.context),
        order_status: OrderStatus::Accepted,
        quantity: resolve_quantity(
            event.context.quantity,
            previous.as_ref().map(|r| r.quantity),
            local_order.map(|order| order.quantity),
            quantity_precision,
        ),
        filled_qty: resolve_quantity(
            event.context.filled_qty,
            previous.as_ref().map(|r| r.filled_qty),
            local_order.map(|order| order.filled_qty),
            quantity_precision,
        ),
        report_id: UUID4::new(),
        ts_accepted: previous
            .as_ref()
            .map_or_else(|| UnixNanos::from(event.ts_event), |r| r.ts_accepted),
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        order_list_id: previous.as_ref().and_then(|r| r.order_list_id),
        venue_position_id: previous.as_ref().and_then(|r| r.venue_position_id),
        linked_order_ids: previous.as_ref().and_then(|r| r.linked_order_ids.clone()),
        parent_order_id: previous.as_ref().and_then(|r| r.parent_order_id),
        contingency_type: previous
            .as_ref()
            .map(|r| r.contingency_type)
            .unwrap_or_default(),
        expire_time: previous.as_ref().and_then(|r| r.expire_time),
        price: resolve_price(
            event.context.price,
            previous.as_ref().and_then(|r| r.price),
            local_order.and_then(|order| order.price),
            price_precision,
        ),
        trigger_price: resolve_price(
            event.context.trigger_price,
            previous.as_ref().and_then(|r| r.trigger_price),
            local_order.and_then(|order| order.trigger_price),
            price_precision,
        ),
        activation_price: previous.as_ref().and_then(|r| r.activation_price),
        trigger_type: previous.as_ref().and_then(|r| r.trigger_type),
        limit_offset: previous.as_ref().and_then(|r| r.limit_offset),
        trailing_offset: previous.as_ref().and_then(|r| r.trailing_offset),
        trailing_offset_type: previous
            .as_ref()
            .map(|r| r.trailing_offset_type)
            .unwrap_or_default(),
        avg_px: resolve_avg_px(event.context.avg_price)
            .or_else(|| previous.as_ref().and_then(|r| r.avg_px)),
        display_qty: previous.as_ref().and_then(|r| r.display_qty),
        post_only: previous.as_ref().is_some_and(|r| r.post_only),
        reduce_only: previous.as_ref().is_some_and(|r| r.reduce_only),
        cancel_reason: None,
        ts_triggered: previous.as_ref().and_then(|r| r.ts_triggered),
    })
}

fn build_rejected_report(
    account_id: AccountId,
    event: &crate::execution::OrderRejected,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        None,
    );
    let previous = previous.or_else(|| {
        local_order.and_then(|order| {
            report_from_local_order_state(
                account_id,
                ClientOrderId::from(event.client_order_id.as_str()),
                None,
                order,
                OrderStatus::Rejected,
                UnixNanos::from(event.ts_event),
                get_atomic_clock_realtime().get_time_ns(),
            )
        })
    })?;
    Some(OrderStatusReport {
        account_id,
        order_status: OrderStatus::Rejected,
        cancel_reason: Some(event.reason.clone()),
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        report_id: UUID4::new(),
        ..previous
    })
}

fn build_modified_report(
    account_id: AccountId,
    event: &crate::execution::OrderModified,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let base = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        Some(VenueOrderId::from(event.venue_order_id.as_str())),
    )?;
    let quantity_precision = base.quantity.precision;
    let price_precision = base.price.map_or(2, |price| price.precision);
    Some(OrderStatusReport {
        account_id,
        quantity: resolve_quantity(
            event.new_qty,
            Some(base.quantity),
            local_order.map(|order| order.quantity),
            quantity_precision,
        ),
        price: resolve_price(
            event.new_price,
            base.price,
            local_order.and_then(|order| order.price),
            price_precision,
        ),
        trigger_price: base.trigger_price,
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        report_id: UUID4::new(),
        ..base
    })
}

fn build_cancelled_report(
    account_id: AccountId,
    event: &crate::execution::OrderCancelled,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<OrderStatusReport> {
    let base = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        Some(VenueOrderId::from(event.venue_order_id.as_str())),
    )
    .or_else(|| {
        local_order.and_then(|order| {
            report_from_local_order_state(
                account_id,
                ClientOrderId::from(event.client_order_id.as_str()),
                Some(VenueOrderId::from(event.venue_order_id.as_str())),
                order,
                OrderStatus::Canceled,
                UnixNanos::from(event.ts_event),
                get_atomic_clock_realtime().get_time_ns(),
            )
        })
    })?;
    Some(OrderStatusReport {
        account_id,
        order_status: OrderStatus::Canceled,
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        report_id: UUID4::new(),
        ..base
    })
}

fn build_fill_status_report(
    account_id: AccountId,
    event: &crate::execution::OrderFilled,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
    fill_reports: &DashMap<String, FillReport>,
) -> Option<OrderStatusReport> {
    let client_order_id = ClientOrderId::from(event.client_order_id.as_str());
    let venue_order_id = VenueOrderId::from(event.venue_order_id.as_str());
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
    );
    let base = previous;
    let instrument_id = resolve_instrument_id(base.as_ref(), local_order, &event.context)?;
    let quantity_precision = base.as_ref().map_or(0, |report| report.quantity.precision);
    let price_precision = base
        .as_ref()
        .and_then(|report| report.price.map(|price| price.precision))
        .or_else(|| {
            base.as_ref()
                .and_then(|report| report.trigger_price.map(|price| price.precision))
        })
        .unwrap_or_else(|| infer_price_precision(event.fill_price));
    let total_filled = event.context.filled_qty.map_or_else(
        || {
            let previous_filled = base
                .as_ref()
                .map_or(0.0, |report| report.filled_qty.as_f64());
            make_quantity(previous_filled + event.fill_qty, quantity_precision)
        },
        |value| make_quantity(value, quantity_precision),
    );
    let leaves_qty = make_quantity(
        event.context.leaves_qty.unwrap_or(event.leaves_qty),
        quantity_precision,
    );
    let quantity = base.as_ref().map_or_else(
        || {
            make_quantity(
                total_filled.as_f64() + leaves_qty.as_f64(),
                quantity_precision,
            )
        },
        |report| report.quantity,
    );
    let order_side = resolve_order_side(base.as_ref(), local_order, &event.context);
    let order_type = resolve_order_type(base.as_ref(), local_order, &event.context);
    let time_in_force = resolve_time_in_force(base.as_ref(), local_order, &event.context);
    let order_status = if leaves_qty.as_f64() > 0.0 {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::Filled
    };
    let observed_avg = fill_reports
        .iter()
        .filter(|entry| {
            let report = entry.value();
            report.client_order_id == Some(client_order_id)
                || report.venue_order_id == venue_order_id
        })
        .fold((0.0, 0.0), |(qty, notional), entry| {
            let report = entry.value();
            let last_qty = report.last_qty.as_f64();
            (
                qty + last_qty,
                notional + last_qty * report.last_px.as_f64(),
            )
        });
    let avg_px = resolve_avg_px(event.context.avg_price)
        .or_else(|| {
            if observed_avg.0 > 0.0 {
                Decimal::from_f64_retain(observed_avg.1 / observed_avg.0)
            } else {
                None
            }
        })
        .or_else(|| base.as_ref().and_then(|report| report.avg_px));
    let price = resolve_price(
        event.context.price,
        base.as_ref().and_then(|r| r.price),
        local_order.and_then(|order| order.price),
        price_precision,
    )
    .or_else(|| {
        if order_status == OrderStatus::Filled && LIMIT_ORDER_TYPES.contains(&order_type) {
            avg_px.and_then(|avg_px| Price::from_decimal_dp(avg_px, price_precision).ok())
        } else {
            None
        }
    });

    Some(OrderStatusReport {
        account_id,
        instrument_id,
        client_order_id: Some(client_order_id),
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty: total_filled,
        report_id: UUID4::new(),
        ts_accepted: base.as_ref().map_or_else(
            || UnixNanos::from(event.ts_event),
            |report| report.ts_accepted,
        ),
        ts_last: UnixNanos::from(event.ts_event),
        ts_init: get_atomic_clock_realtime().get_time_ns(),
        order_list_id: base.as_ref().and_then(|r| r.order_list_id),
        venue_position_id: base.as_ref().and_then(|r| r.venue_position_id),
        linked_order_ids: base.as_ref().and_then(|r| r.linked_order_ids.clone()),
        parent_order_id: base.as_ref().and_then(|r| r.parent_order_id),
        contingency_type: base
            .as_ref()
            .map(|r| r.contingency_type)
            .unwrap_or_default(),
        expire_time: base.as_ref().and_then(|r| r.expire_time),
        price,
        trigger_price: base.as_ref().and_then(|r| r.trigger_price).or_else(|| {
            local_order.and_then(|order| resolve_price(order.trigger_price, None, None, 2))
        }),
        activation_price: base.as_ref().and_then(|r| r.activation_price),
        trigger_type: base.as_ref().and_then(|r| r.trigger_type),
        limit_offset: base.as_ref().and_then(|r| r.limit_offset),
        trailing_offset: base.as_ref().and_then(|r| r.trailing_offset),
        trailing_offset_type: base
            .as_ref()
            .map(|r| r.trailing_offset_type)
            .unwrap_or_default(),
        avg_px,
        display_qty: base.as_ref().and_then(|r| r.display_qty),
        post_only: base.as_ref().is_some_and(|r| r.post_only),
        reduce_only: base.as_ref().is_some_and(|r| r.reduce_only),
        cancel_reason: None,
        ts_triggered: base.as_ref().and_then(|r| r.ts_triggered),
    })
}

fn build_fill_report(
    account_id: AccountId,
    event: &crate::execution::OrderFilled,
    local_order: Option<&OrderState>,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) -> Option<FillReport> {
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        Some(VenueOrderId::from(event.venue_order_id.as_str())),
    );
    let instrument_id = resolve_instrument_id(previous.as_ref(), local_order, &event.context)?;
    let quantity_precision = previous
        .as_ref()
        .map_or(0, |report| report.filled_qty.precision);
    let price_precision = previous
        .as_ref()
        .and_then(|report| report.price.map(|price| price.precision))
        .or_else(|| {
            previous
                .as_ref()
                .and_then(|report| report.trigger_price.map(|price| price.precision))
        })
        .unwrap_or(2);
    let quote_currency = event
        .currency
        .as_deref()
        .and_then(parse_currency)
        .unwrap_or_else(Currency::USD);
    Some(FillReport::new(
        account_id,
        instrument_id,
        VenueOrderId::from(event.venue_order_id.as_str()),
        TradeId::from(
            event
                .trade_id
                .clone()
                .unwrap_or_else(|| format!("{}-{}", event.venue_order_id, event.ts_event))
                .as_str(),
        ),
        resolve_order_side(previous.as_ref(), local_order, &event.context),
        make_quantity(event.fill_qty, quantity_precision),
        make_price(event.fill_price, price_precision),
        Money::new(event.commission, quote_currency),
        LiquiditySide::NoLiquiditySide,
        Some(ClientOrderId::from(event.client_order_id.as_str())),
        None,
        UnixNanos::from(event.ts_event),
        get_atomic_clock_realtime().get_time_ns(),
        None,
    ))
}

fn to_model_side(side: RithmicOrderSide) -> OrderSide {
    match side {
        RithmicOrderSide::Buy => OrderSide::Buy,
        RithmicOrderSide::Sell => OrderSide::Sell,
        _ => OrderSide::NoOrderSide,
    }
}

fn to_model_order_type(order_type: RithmicOrderType) -> OrderType {
    match order_type {
        RithmicOrderType::Market => OrderType::Market,
        RithmicOrderType::Limit => OrderType::Limit,
        RithmicOrderType::StopMarket => OrderType::StopMarket,
        RithmicOrderType::StopLimit => OrderType::StopLimit,
        _ => OrderType::Market,
    }
}

fn to_model_order_status(status: rithmic_rs::OrderStatus) -> OrderStatus {
    match status {
        rithmic_rs::OrderStatus::Pending => OrderStatus::Submitted,
        rithmic_rs::OrderStatus::Open => OrderStatus::Accepted,
        rithmic_rs::OrderStatus::Partial => OrderStatus::PartiallyFilled,
        rithmic_rs::OrderStatus::Cancelled => OrderStatus::Canceled,
        rithmic_rs::OrderStatus::Rejected => OrderStatus::Rejected,
        rithmic_rs::OrderStatus::Expired => OrderStatus::Expired,
        rithmic_rs::OrderStatus::Complete => OrderStatus::Filled,
        rithmic_rs::OrderStatus::Unknown => OrderStatus::Submitted,
        _ => OrderStatus::Submitted,
    }
}

fn to_model_tif(tif: RithmicTif) -> TimeInForce {
    match tif {
        RithmicTif::Day => TimeInForce::Day,
        RithmicTif::Gtc => TimeInForce::Gtc,
        RithmicTif::Ioc => TimeInForce::Ioc,
        RithmicTif::Fok => TimeInForce::Fok,
        _ => TimeInForce::Day,
    }
}

fn is_empty_replay_error(error: &impl Display) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("replay") && message.contains("no data")
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::OrderFactory,
        live::get_runtime,
        messages::{ExecutionEvent as EngineExecutionEvent, ExecutionReport},
    };
    use nautilus_model::{
        enums::{AccountType, AssetClass},
        events::OrderEventAny,
        identifiers::{ClientId, OrderListId, StrategyId, TraderId},
        instruments::{FuturesContract, InstrumentAny},
        orders::{OrderList, builder::OrderTestBuilder},
    };
    use tokio::{
        sync::{broadcast, mpsc},
        time::timeout,
    };
    use ustr::Ustr;

    use super::*;
    use crate::{
        config::RithmicEnv,
        execution::{OrderAccepted, OrderContext, OrderFilled},
        gateway::GatewayConfig,
        providers::{AccountBalance as ProviderAccountBalance, Position as ProviderPosition},
    };

    fn sample_report(order_status: OrderStatus, filled_qty: &str) -> OrderStatusReport {
        let mut report = OrderStatusReport::new(
            AccountId::from("RITHMIC-001"),
            InstrumentId::from("ESM6.RITHMIC"),
            Some(ClientOrderId::from("O-1")),
            VenueOrderId::from("V-1"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Day,
            order_status,
            Quantity::from("1"),
            Quantity::from(filled_qty),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            None,
        );
        report.price = Some(Price::from("5000.25"));
        report
    }

    fn sample_fill_report(trade_id: &str, ts_event: u64) -> FillReport {
        FillReport::new(
            AccountId::from("RITHMIC-001"),
            InstrumentId::from("ESM6.RITHMIC"),
            VenueOrderId::from("V-1"),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from("1"),
            Price::from("20000"),
            Money::new(0.0, Currency::USD()),
            LiquiditySide::NoLiquiditySide,
            Some(ClientOrderId::from("O-1")),
            None,
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_event + 1),
            None,
        )
    }

    fn sample_position_report(signed_qty: i64, ts_event: u64) -> PositionStatusReport {
        let side = if signed_qty > 0 {
            PositionSideSpecified::Long
        } else if signed_qty < 0 {
            PositionSideSpecified::Short
        } else {
            PositionSideSpecified::Flat
        };

        PositionStatusReport::new(
            AccountId::from("RITHMIC-001"),
            InstrumentId::from("ESM6.RITHMIC"),
            side,
            Quantity::from(signed_qty.abs()),
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_event + 1),
            None,
            None,
            Some(Decimal::from_str("5000.25").expect("decimal")),
        )
    }

    fn sample_cache() -> Rc<RefCell<Cache>> {
        Rc::new(RefCell::new(Cache::default()))
    }

    fn sample_rithmic_instrument_id() -> InstrumentId {
        InstrumentId::from("ESM6.RITHMIC")
    }

    fn sample_rithmic_instrument() -> InstrumentAny {
        InstrumentAny::FuturesContract(FuturesContract::new(
            sample_rithmic_instrument_id(),
            "ESM6".into(),
            AssetClass::Index,
            Some(Ustr::from("XCME")),
            Ustr::from("ES"),
            UnixNanos::default(),
            UnixNanos::from(1),
            Currency::USD(),
            2,
            Price::from("0.25"),
            Quantity::from(1),
            Quantity::from(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UnixNanos::default(),
            UnixNanos::default(),
        ))
    }

    #[expect(clippy::needless_pass_by_value)]
    fn sample_exec_client(cache: Rc<RefCell<Cache>>) -> RithmicLiveExecClient {
        let config = RithmicExecClientConfig::new(
            TraderId::from("TESTER-001"),
            RithmicEnv::Demo,
            "user",
            "pass",
            "TestSystem",
            "ACC-001",
        );
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::new("TESTSYSTEM_ACC_001"),
            Venue::from("RITHMIC"),
            OmsType::Netting,
            AccountId::from("RITHMIC-TESTSYSTEM_ACC_001-ACC-001"),
            AccountType::Margin,
            None,
            Rc::clone(&cache),
        );
        RithmicLiveExecClient::new(core, config)
    }

    #[expect(dead_code)]
    fn sample_order_factory() -> OrderFactory {
        OrderFactory::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            None,
            None,
            Rc::new(RefCell::new(TestClock::new())),
            false,
            true,
        )
    }

    fn sample_bracket_orders(market_entry: bool) -> Vec<OrderAny> {
        let instrument_id = sample_rithmic_instrument_id();
        let trader_id = TraderId::from("TESTER-001");
        let strategy_id = StrategyId::from("S-001");
        let order_list_id = OrderListId::from("OL-BRK");
        let entry_id = ClientOrderId::from("ENTRY-1");
        let sl_id = ClientOrderId::from("SL-1");
        let tp_id = ClientOrderId::from("TP-1");

        let mut entry_builder = OrderTestBuilder::new(if market_entry {
            OrderType::Market
        } else {
            OrderType::Limit
        });
        entry_builder
            .trader_id(trader_id)
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(entry_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .time_in_force(TimeInForce::Day)
            .order_list_id(order_list_id)
            .contingency_type(ContingencyType::Oto)
            .linked_order_ids(vec![sl_id, tp_id]);
        if !market_entry {
            entry_builder.price(Price::from("5000.00"));
        }
        let entry = entry_builder.build();

        let mut sl_builder = OrderTestBuilder::new(OrderType::StopMarket);
        sl_builder
            .trader_id(trader_id)
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(sl_id)
            .side(OrderSide::Sell)
            .quantity(Quantity::from(1))
            .trigger_price(Price::from("4997.50"))
            .time_in_force(TimeInForce::Day)
            .order_list_id(order_list_id)
            .contingency_type(ContingencyType::Oco)
            .linked_order_ids(vec![tp_id])
            .parent_order_id(entry_id)
            .reduce_only(true);
        let sl = sl_builder.build();

        let mut tp_builder = OrderTestBuilder::new(OrderType::Limit);
        tp_builder
            .trader_id(trader_id)
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(tp_id)
            .side(OrderSide::Sell)
            .quantity(Quantity::from(1))
            .price(Price::from("5005.00"))
            .time_in_force(TimeInForce::Day)
            .order_list_id(order_list_id)
            .contingency_type(ContingencyType::Oco)
            .linked_order_ids(vec![sl_id])
            .parent_order_id(entry_id)
            .reduce_only(true);
        let tp = tp_builder.build();

        vec![entry, sl, tp]
    }

    fn sample_oco_orders() -> Vec<OrderAny> {
        let instrument_id = sample_rithmic_instrument_id();
        let trader_id = TraderId::from("TESTER-001");
        let strategy_id = StrategyId::from("S-001");
        let order_list_id = OrderListId::from("OL-001");
        let leg1_id = ClientOrderId::from("OCO-1");
        let leg2_id = ClientOrderId::from("OCO-2");

        let mut leg1_builder = OrderTestBuilder::new(OrderType::Limit);
        leg1_builder
            .trader_id(trader_id)
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(leg1_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("4998.00"))
            .time_in_force(TimeInForce::Day)
            .order_list_id(order_list_id)
            .contingency_type(ContingencyType::Oco)
            .linked_order_ids(vec![leg2_id]);
        let leg1 = leg1_builder.build();

        let mut leg2_builder = OrderTestBuilder::new(OrderType::StopMarket);
        leg2_builder
            .trader_id(trader_id)
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(leg2_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .trigger_price(Price::from("5002.00"))
            .time_in_force(TimeInForce::Day)
            .order_list_id(order_list_id)
            .contingency_type(ContingencyType::Oco)
            .linked_order_ids(vec![leg1_id]);
        let leg2 = leg2_builder.build();

        vec![leg1, leg2]
    }

    fn add_orders_to_cache(
        cache: &Rc<RefCell<Cache>>,
        client_id: ClientId,
        orders: &[OrderAny],
    ) -> OrderList {
        let mut guard = cache.borrow_mut();
        guard
            .add_instrument(sample_rithmic_instrument())
            .expect("instrument should add to cache");
        let order_list = OrderList::from_orders(orders, UnixNanos::default());
        guard
            .add_order_list(order_list.clone())
            .expect("order list should add to cache");

        for order in orders {
            guard
                .add_order(order.clone(), None, Some(client_id), true)
                .expect("order should add to cache");
        }
        order_list
    }

    fn submit_order_list_command(
        client: &RithmicLiveExecClient,
        order_list: OrderList,
        orders: &[OrderAny],
    ) -> SubmitOrderList {
        SubmitOrderList::new(
            client.core.trader_id,
            Some(client.core.client_id),
            StrategyId::from("S-001"),
            order_list,
            orders
                .iter()
                .map(|order| order.init_event().clone())
                .collect(),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn attach_event_sender(
        client: &mut RithmicLiveExecClient,
    ) -> mpsc::UnboundedReceiver<EngineExecutionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        client.emitter.set_sender(tx);
        rx
    }

    fn attach_submit_path(client: &mut RithmicLiveExecClient) {
        let lease = SharedGatewayLease::acquire(client.gateway_config());
        let gateway = lease.gateway();
        client.inner = Some(Arc::new(RithmicExecutionClient::new(
            gateway,
            RithmicAccount::new("fcm", "ib", "ACC-001"),
        )));
        client.gateway = Some(lease);
    }

    async fn recv_order_events(
        rx: &mut mpsc::UnboundedReceiver<EngineExecutionEvent>,
        count: usize,
    ) -> Vec<OrderEventAny> {
        let mut events = Vec::with_capacity(count);

        for _ in 0..count {
            let event = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for engine execution event")
                .expect("execution event channel closed unexpectedly");

            match event {
                EngineExecutionEvent::Order(order_event) => events.push(order_event),
                other => panic!("expected order event, received {other:?}"),
            }
        }
        events
    }

    #[rstest::rstest]
    fn snapshot_report_with_lower_filled_qty_is_dropped() {
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();

        let previous = sample_report(OrderStatus::PartiallyFilled, "1");
        store_order_report(&client_reports, &venue_reports, previous);

        let regressive = sample_report(OrderStatus::Accepted, "0");
        let result =
            coalesce_snapshot_report(Some(regressive), true, &client_reports, &venue_reports);

        assert!(result.is_none());
    }

    #[rstest::rstest]
    fn filled_snapshot_does_not_regress_to_non_filled_status() {
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();

        let previous = sample_report(OrderStatus::Filled, "1");
        store_order_report(&client_reports, &venue_reports, previous);

        let regressive = sample_report(OrderStatus::Accepted, "1");
        let result =
            coalesce_snapshot_report(Some(regressive), true, &client_reports, &venue_reports);

        assert!(result.is_none());
    }

    #[rstest::rstest]
    fn live_submitted_report_regressing_from_accepted_is_dropped() {
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();

        let previous = sample_report(OrderStatus::Accepted, "0");
        store_order_report(&client_reports, &venue_reports, previous);

        let regressive = sample_report(OrderStatus::Submitted, "0");
        let result =
            coalesce_snapshot_report(Some(regressive), false, &client_reports, &venue_reports);

        assert!(result.is_none());
    }

    #[rstest::rstest]
    fn live_submitted_report_for_local_order_is_not_emitted() {
        let account_id = AccountId::from("RITHMIC-001");
        let mut emitter = sample_emitter(account_id);
        let (tx, mut rx) = mpsc::unbounded_channel();
        emitter.set_sender(tx);

        let inner = RithmicExecutionClient::new(
            sample_gateway(),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        );
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let fill_reports = DashMap::new();

        let event = ExecutionEvent::Submitted(crate::execution::OrderSubmitted {
            client_order_id: "CLIENT-LOCAL".to_string(),
            venue_order_id: Some("VENUE-LOCAL".to_string()),
            account_id: "ACC-1".to_string(),
            ts_event: 1,
            context: sample_order_context(),
        });
        inner.apply_event(&event);

        process_execution_update(
            &inner,
            account_id,
            &emitter,
            &client_reports,
            &venue_reports,
            &fill_reports,
            event,
        );

        assert!(client_reports.contains_key(&ClientOrderId::from("CLIENT-LOCAL")));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[rstest::rstest]
    fn snapshot_limit_accept_without_price_is_dropped() {
        let account_id = AccountId::from("RITHMIC-001");
        let emitter = sample_emitter(account_id);
        let inner = RithmicExecutionClient::new(
            sample_gateway(),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        );
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let fill_reports = DashMap::new();
        let mut context = sample_order_context();
        context.is_snapshot = true;
        context.price = None;

        process_execution_update(
            &inner,
            account_id,
            &emitter,
            &client_reports,
            &venue_reports,
            &fill_reports,
            ExecutionEvent::Accepted(OrderAccepted {
                client_order_id: "CLIENT-1".to_string(),
                venue_order_id: "VENUE-1".to_string(),
                account_id: "ACC-1".to_string(),
                ts_event: 1,
                context,
            }),
        );

        assert!(client_reports.is_empty());
        assert!(venue_reports.is_empty());
        assert!(fill_reports.is_empty());
    }

    #[rstest::rstest]
    fn snapshot_limit_fill_without_price_backfills_terminal_order_price() {
        let account_id = AccountId::from("RITHMIC-001");
        let emitter = sample_emitter(account_id);
        let inner = RithmicExecutionClient::new(
            sample_gateway(),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        );
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let fill_reports = DashMap::new();
        let mut context = sample_order_context();
        context.is_snapshot = true;
        context.price = None;
        context.filled_qty = Some(1.0);
        context.leaves_qty = Some(0.0);

        process_execution_update(
            &inner,
            account_id,
            &emitter,
            &client_reports,
            &venue_reports,
            &fill_reports,
            ExecutionEvent::Filled(OrderFilled {
                client_order_id: "CLIENT-2".to_string(),
                account_id: "ACC-1".to_string(),
                venue_order_id: "VENUE-2".to_string(),
                fill_price: 5000.50,
                fill_qty: 1.0,
                leaves_qty: 0.0,
                commission: 0.0,
                ts_event: 2,
                trade_id: Some("TRADE-1".to_string()),
                currency: Some("USD".to_string()),
                context,
            }),
        );

        assert_eq!(fill_reports.len(), 1);
        assert_eq!(client_reports.len(), 1);
        assert_eq!(venue_reports.len(), 1);

        let report = client_reports
            .get(&ClientOrderId::from("CLIENT-2"))
            .expect("expected order report");
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.price, Some(Price::from("5000.50")));
        assert_eq!(report.avg_px, Decimal::from_str("5000.5").ok());
    }

    #[rstest::rstest]
    fn live_market_accept_without_price_uses_local_order_state() {
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let mut context = sample_order_context();
        context.symbol = Some("MNQM6".to_string());
        context.exchange = Some("CME".to_string());
        context.order_type = Some(rithmic_rs::OrderType::Limit);
        context.time_in_force = Some(rithmic_rs::TimeInForce::Day);
        context.price = None;
        context.trigger_price = None;
        context.quantity = Some(1.0);
        context.filled_qty = Some(0.0);

        let report = coalesce_snapshot_report(
            build_accept_report(
                AccountId::from("RITHMIC-001"),
                &OrderAccepted {
                    client_order_id: "CLIENT-MKT-1".to_string(),
                    venue_order_id: "VENUE-MKT-1".to_string(),
                    account_id: "ACC-1".to_string(),
                    ts_event: 10,
                    context,
                },
                Some(&sample_local_market_order_state(
                    "CLIENT-MKT-1",
                    "VENUE-MKT-1",
                )),
                &client_reports,
                &venue_reports,
            ),
            false,
            &client_reports,
            &venue_reports,
        )
        .expect("expected local market order fallback report");

        assert_eq!(report.instrument_id, InstrumentId::from("MNQM6.RITHMIC"));
        assert_eq!(report.order_type, OrderType::Market);
        assert_eq!(report.time_in_force, TimeInForce::Ioc);
        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert!(report.price.is_none());
    }

    #[tokio::test]
    async fn live_fill_emits_fill_report_before_order_status_report() {
        let account_id = AccountId::from("RITHMIC-001");
        let mut emitter = sample_emitter(account_id);
        let (tx, mut rx) = mpsc::unbounded_channel();
        emitter.set_sender(tx);

        let inner = RithmicExecutionClient::new(
            sample_gateway(),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        );
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let fill_reports = DashMap::new();
        let mut accepted = sample_report(OrderStatus::Accepted, "0");
        accepted.client_order_id = Some(ClientOrderId::from("CLIENT-FILL"));
        accepted.venue_order_id = VenueOrderId::from("VENUE-FILL");
        store_order_report(&client_reports, &venue_reports, accepted);
        let mut context = sample_order_context();
        context.filled_qty = Some(1.0);
        context.leaves_qty = Some(0.0);
        context.avg_price = Some(5000.50);

        process_execution_update(
            &inner,
            account_id,
            &emitter,
            &client_reports,
            &venue_reports,
            &fill_reports,
            ExecutionEvent::Filled(OrderFilled {
                client_order_id: "CLIENT-FILL".to_string(),
                account_id: "ACC-1".to_string(),
                venue_order_id: "VENUE-FILL".to_string(),
                fill_price: 5000.50,
                fill_qty: 1.0,
                leaves_qty: 0.0,
                commission: 0.0,
                ts_event: 2,
                trade_id: Some("TRADE-FILL".to_string()),
                currency: Some("USD".to_string()),
                context,
            }),
        );

        let first = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for first execution report")
            .expect("execution event channel closed unexpectedly");
        let second = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for second execution report")
            .expect("execution event channel closed unexpectedly");

        assert!(matches!(
            first,
            EngineExecutionEvent::Report(ExecutionReport::Fill(_))
        ));

        match second {
            EngineExecutionEvent::Report(ExecutionReport::Order(report)) => {
                assert_eq!(report.order_status, OrderStatus::Filled);
            }
            other => panic!("expected order execution report, received {other:?}"),
        }
    }

    #[tokio::test]
    async fn current_state_mass_status_omits_closed_replay_lifecycle() {
        let cache = sample_cache();
        let client = sample_exec_client(cache);

        let mut open_report = sample_report(OrderStatus::PartiallyFilled, "1");
        open_report.client_order_id = Some(ClientOrderId::from("O-OPEN"));
        open_report.venue_order_id = VenueOrderId::from("V-OPEN");
        open_report.quantity = Quantity::from("2");
        open_report.ts_last = UnixNanos::from(10);
        store_order_report(
            &client.order_reports_by_client,
            &client.order_reports_by_venue,
            open_report,
        );

        let mut closed_report = sample_report(OrderStatus::Filled, "1");
        closed_report.client_order_id = Some(ClientOrderId::from("O-CLOSED"));
        closed_report.venue_order_id = VenueOrderId::from("V-CLOSED");
        closed_report.ts_last = UnixNanos::from(5);
        store_order_report(
            &client.order_reports_by_client,
            &client.order_reports_by_venue,
            closed_report,
        );

        let mut open_fill = sample_fill_report("T-OPEN", 11);
        open_fill.venue_order_id = VenueOrderId::from("V-OPEN");
        client
            .fill_reports
            .insert(fill_report_key(&open_fill), open_fill.clone());

        let mut closed_fill = sample_fill_report("T-CLOSED", 6);
        closed_fill.venue_order_id = VenueOrderId::from("V-CLOSED");
        client
            .fill_reports
            .insert(fill_report_key(&closed_fill), closed_fill);

        client
            .position_reports
            .insert("ESM6.RITHMIC".to_string(), sample_position_report(1, 12));

        let mass_status = client
            .generate_mass_status(None)
            .await
            .expect("mass status should generate")
            .expect("mass status should be returned");

        assert_eq!(mass_status.order_reports().len(), 1);
        assert!(
            mass_status
                .order_reports()
                .contains_key(&VenueOrderId::from("V-OPEN"))
        );
        assert!(
            !mass_status
                .order_reports()
                .contains_key(&VenueOrderId::from("V-CLOSED"))
        );

        assert_eq!(mass_status.fill_reports().len(), 1);
        assert!(
            mass_status
                .fill_reports()
                .contains_key(&VenueOrderId::from("V-OPEN"))
        );
        assert!(
            !mass_status
                .fill_reports()
                .contains_key(&VenueOrderId::from("V-CLOSED"))
        );

        assert_eq!(mass_status.position_reports().len(), 1);
    }

    #[tokio::test]
    async fn query_order_emits_cached_report_instead_of_default_stub() {
        let cache = sample_cache();
        let mut client = sample_exec_client(Rc::clone(&cache));
        let mut rx = attach_event_sender(&mut client);
        let report = sample_report(OrderStatus::Accepted, "0");
        client
            .order_reports_by_client
            .insert(ClientOrderId::from("O-1"), report.clone());
        client
            .order_reports_by_venue
            .insert(VenueOrderId::from("V-1"), report.clone());

        client
            .query_order(QueryOrder::new(
                TraderId::from("TESTER-001"),
                Some(client.client_id()),
                StrategyId::from("S-001"),
                report.instrument_id,
                ClientOrderId::from("O-1"),
                Some(VenueOrderId::from("V-1")),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .expect("query_order should succeed");

        let event = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for execution report")
            .expect("execution event channel closed unexpectedly");

        match event {
            EngineExecutionEvent::Report(ExecutionReport::Order(order_report)) => {
                assert_eq!(
                    order_report.client_order_id,
                    Some(ClientOrderId::from("O-1"))
                );
                assert_eq!(order_report.venue_order_id, VenueOrderId::from("V-1"));
                assert_eq!(order_report.order_status, OrderStatus::Accepted);
            }
            other => panic!("expected order execution report, received {other:?}"),
        }
    }

    #[rstest::rstest]
    fn fill_reports_are_sorted_chronologically_for_reconciliation() {
        let mut reports = vec![
            sample_fill_report("T-LATE", 20),
            sample_fill_report("T-EARLY", 10),
        ];

        sort_fill_reports(&mut reports);

        assert_eq!(reports[0].trade_id, TradeId::from("T-EARLY"));
        assert_eq!(reports[1].trade_id, TradeId::from("T-LATE"));
    }

    #[rstest::rstest]
    fn build_native_bracket_spec_calculates_tick_distances() {
        let cache = sample_cache();
        cache
            .borrow_mut()
            .add_instrument(sample_rithmic_instrument())
            .expect("instrument should add to cache");
        let client = sample_exec_client(cache);
        let orders = sample_bracket_orders(false);

        let spec = client
            .build_native_bracket_spec(&orders)
            .expect("bracket spec should validate")
            .expect("bracket spec should match");

        assert_eq!(spec.profit_ticks, 20);
        assert_eq!(spec.stop_ticks, 10);
    }

    #[tokio::test]
    async fn submit_order_list_denies_unsupported_market_entry_bracket() {
        let cache = sample_cache();
        let mut client = sample_exec_client(Rc::clone(&cache));
        let mut rx = attach_event_sender(&mut client);
        let orders = sample_bracket_orders(true);
        let order_list = add_orders_to_cache(&cache, client.core.client_id, &orders);
        let cmd = submit_order_list_command(&client, order_list, &orders);

        client
            .submit_order_list(cmd)
            .expect("unsupported bracket should deny, not error");

        let events = recv_order_events(&mut rx, 3).await;
        assert!(
            events
                .iter()
                .all(|event| matches!(event, OrderEventAny::Denied(_)))
        );

        match &events[0] {
            OrderEventAny::Denied(event) => {
                assert_eq!(
                    event.reason.as_str(),
                    "UNSUPPORTED_NATIVE_BRACKET_MARKET_ENTRY_FOR_HIGH_LEVEL_RITHMIC"
                );
            }
            other => panic!("expected denied event, received {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_order_list_emits_submitted_and_rejected_for_supported_oco() {
        let cache = sample_cache();
        let mut client = sample_exec_client(Rc::clone(&cache));
        let mut rx = attach_event_sender(&mut client);
        attach_submit_path(&mut client);
        let orders = sample_oco_orders();
        let order_list = add_orders_to_cache(&cache, client.core.client_id, &orders);
        let cmd = submit_order_list_command(&client, order_list, &orders);

        client
            .submit_order_list(cmd)
            .expect("supported oco should enter v2 submission path");

        let events = recv_order_events(&mut rx, 4).await;
        assert!(matches!(events[0], OrderEventAny::Submitted(_)));
        assert!(matches!(events[1], OrderEventAny::Submitted(_)));
        assert!(matches!(events[2], OrderEventAny::Rejected(_)));
        assert!(matches!(events[3], OrderEventAny::Rejected(_)));
    }

    #[tokio::test]
    async fn submit_order_list_emits_submitted_and_rejected_for_supported_limit_entry_bracket() {
        let cache = sample_cache();
        let mut client = sample_exec_client(Rc::clone(&cache));
        let mut rx = attach_event_sender(&mut client);
        attach_submit_path(&mut client);
        let orders = sample_bracket_orders(false);
        let order_list = add_orders_to_cache(&cache, client.core.client_id, &orders);
        let cmd = submit_order_list_command(&client, order_list, &orders);

        client
            .submit_order_list(cmd)
            .expect("supported bracket should enter v2 submission path");

        let events = recv_order_events(&mut rx, 6).await;
        assert!(matches!(events[0], OrderEventAny::Submitted(_)));
        assert!(matches!(events[1], OrderEventAny::Submitted(_)));
        assert!(matches!(events[2], OrderEventAny::Submitted(_)));
        assert!(matches!(events[3], OrderEventAny::Rejected(_)));
        assert!(matches!(events[4], OrderEventAny::Rejected(_)));
        assert!(matches!(events[5], OrderEventAny::Rejected(_)));
    }

    fn sample_gateway() -> Arc<RwLock<RithmicGateway>> {
        Arc::new(RwLock::new(RithmicGateway::new(
            GatewayConfig::new(
                RithmicEnv::Demo,
                "user",
                "pass",
                "system",
                "fcm",
                "ib",
                "account",
            )
            .with_app_name("TestApp"),
        )))
    }

    fn sample_emitter(account_id: AccountId) -> ExecutionEventEmitter {
        ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            TraderId::from("TESTER-001"),
            account_id,
            AccountType::Margin,
            None,
        )
    }

    fn sample_order_context() -> OrderContext {
        OrderContext {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            side: Some(rithmic_rs::OrderSide::Buy),
            order_type: Some(rithmic_rs::OrderType::Limit),
            time_in_force: Some(rithmic_rs::TimeInForce::Day),
            quantity: Some(1.0),
            leaves_qty: Some(1.0),
            price: Some(5000.25),
            ..Default::default()
        }
    }

    fn sample_local_market_order_state(client_order_id: &str, venue_order_id: &str) -> OrderState {
        OrderState {
            client_order_id: client_order_id.to_string(),
            venue_order_id: Some(venue_order_id.to_string()),
            symbol: "MNQM6".to_string(),
            exchange: "CME".to_string(),
            side: rithmic_rs::OrderSide::Buy,
            order_type: rithmic_rs::OrderType::Market,
            time_in_force: rithmic_rs::TimeInForce::Ioc,
            price: None,
            trigger_price: None,
            status: rithmic_rs::OrderStatus::Pending,
            quantity: 1.0,
            filled_qty: 0.0,
            leaves_qty: 1.0,
            avg_price: 0.0,
        }
    }

    async fn wait_for(predicate: impl Fn() -> bool, description: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
    }

    #[tokio::test]
    async fn execution_event_loop_filters_reports_by_venue_account() {
        let (exec_tx, _) = broadcast::channel(16);
        let (pnl_tx, _) = broadcast::channel(16);

        let gateway = sample_gateway();
        let inner_a = Arc::new(RithmicExecutionClient::new(
            Arc::clone(&gateway),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        ));
        let inner_b = Arc::new(RithmicExecutionClient::new(
            Arc::clone(&gateway),
            RithmicAccount::new("fcm", "ib", "ACC-2"),
        ));

        let account_id_a = AccountId::from("RITHMIC-TESTSYSTEM_ACC_1-ACC-1");
        let account_id_b = AccountId::from("RITHMIC-TESTSYSTEM_ACC_2-ACC-2");

        let connected_a = Arc::new(AtomicBool::new(true));
        let connected_b = Arc::new(AtomicBool::new(true));
        let order_reports_a = Arc::new(DashMap::new());
        let order_reports_b = Arc::new(DashMap::new());
        let venue_reports_a = Arc::new(DashMap::new());
        let venue_reports_b = Arc::new(DashMap::new());
        let fill_reports_a = Arc::new(DashMap::new());
        let fill_reports_b = Arc::new(DashMap::new());
        let position_reports_a = Arc::new(DashMap::new());
        let position_reports_b = Arc::new(DashMap::new());

        let task_a = get_runtime().spawn(run_execution_event_loop(
            exec_tx.subscribe(),
            pnl_tx.subscribe(),
            ExecutionEventLoopContext {
                gateway: Arc::clone(&gateway),
                inner: Arc::clone(&inner_a),
                account_id: account_id_a,
                emitter: sample_emitter(account_id_a),
                is_connected: Arc::clone(&connected_a),
                order_reports_by_client: Arc::clone(&order_reports_a),
                order_reports_by_venue: Arc::clone(&venue_reports_a),
                fill_reports: Arc::clone(&fill_reports_a),
                position_reports: Arc::clone(&position_reports_a),
                replay_lookback_secs: 0,
            },
        ));

        let task_b = get_runtime().spawn(run_execution_event_loop(
            exec_tx.subscribe(),
            pnl_tx.subscribe(),
            ExecutionEventLoopContext {
                gateway,
                inner: Arc::clone(&inner_b),
                account_id: account_id_b,
                emitter: sample_emitter(account_id_b),
                is_connected: Arc::clone(&connected_b),
                order_reports_by_client: Arc::clone(&order_reports_b),
                order_reports_by_venue: Arc::clone(&venue_reports_b),
                fill_reports: Arc::clone(&fill_reports_b),
                position_reports: Arc::clone(&position_reports_b),
                replay_lookback_secs: 0,
            },
        ));

        exec_tx
            .send(ExecutionEvent::Accepted(OrderAccepted {
                client_order_id: "CLIENT-A".to_string(),
                venue_order_id: "VENUE-A".to_string(),
                account_id: "ACC-1".to_string(),
                ts_event: 1,
                context: sample_order_context(),
            }))
            .unwrap();
        exec_tx
            .send(ExecutionEvent::Accepted(OrderAccepted {
                client_order_id: "CLIENT-B".to_string(),
                venue_order_id: "VENUE-B".to_string(),
                account_id: "ACC-2".to_string(),
                ts_event: 2,
                context: sample_order_context(),
            }))
            .unwrap();
        pnl_tx
            .send(PnlEvent::Account(ProviderAccountEvent::BalanceUpdate(
                ProviderAccountBalance {
                    is_snapshot: true,
                    account_id: "ACC-1".to_string(),
                    currency: "USD".to_string(),
                    total: 100_000.0,
                    available: 75_000.0,
                    locked: 25_000.0,
                    unrealized_pnl: 100.0,
                    realized_pnl: 25.0,
                    ts_event: 3,
                },
            )))
            .unwrap();
        pnl_tx
            .send(PnlEvent::Position(ProviderPositionEvent::Updated(
                ProviderPosition {
                    is_snapshot: true,
                    account_id: "ACC-1".to_string(),
                    symbol: "ESM6".to_string(),
                    exchange: "CME".to_string(),
                    quantity: 2.0,
                    avg_price: 5000.25,
                    unrealized_pnl: 100.0,
                    realized_pnl: 25.0,
                    ts_event: 4,
                },
            )))
            .unwrap();
        pnl_tx
            .send(PnlEvent::Position(ProviderPositionEvent::Updated(
                ProviderPosition {
                    is_snapshot: true,
                    account_id: "ACC-2".to_string(),
                    symbol: "NQM6".to_string(),
                    exchange: "CME".to_string(),
                    quantity: 1.0,
                    avg_price: 19000.50,
                    unrealized_pnl: 50.0,
                    realized_pnl: 10.0,
                    ts_event: 5,
                },
            )))
            .unwrap();

        wait_for(
            || {
                order_reports_a.len() == 1
                    && order_reports_b.len() == 1
                    && position_reports_a.len() == 1
                    && position_reports_b.len() == 1
            },
            "multi-account execution filtering",
        )
        .await;

        assert!(order_reports_a.contains_key(&ClientOrderId::from("CLIENT-A")));
        assert!(!order_reports_a.contains_key(&ClientOrderId::from("CLIENT-B")));
        assert!(order_reports_b.contains_key(&ClientOrderId::from("CLIENT-B")));
        assert!(!order_reports_b.contains_key(&ClientOrderId::from("CLIENT-A")));

        let position_a = position_reports_a
            .get("ESM6.RITHMIC")
            .expect("expected account A position report");
        assert_eq!(position_a.account_id, account_id_a);
        let position_b = position_reports_b
            .get("NQM6.RITHMIC")
            .expect("expected account B position report");
        assert_eq!(position_b.account_id, account_id_b);
        assert!(connected_a.load(Ordering::Relaxed));
        assert!(connected_b.load(Ordering::Relaxed));

        drop(exec_tx);
        drop(pnl_tx);
        task_a.await.unwrap();
        task_b.await.unwrap();
    }

    #[tokio::test]
    async fn execution_event_loop_routes_accountless_fill_to_tracked_order() {
        let (exec_tx, _) = broadcast::channel(16);
        let (pnl_tx, _) = broadcast::channel(16);

        let gateway = sample_gateway();
        let inner_a = Arc::new(RithmicExecutionClient::new(
            Arc::clone(&gateway),
            RithmicAccount::new("fcm", "ib", "ACC-1"),
        ));
        let inner_b = Arc::new(RithmicExecutionClient::new(
            Arc::clone(&gateway),
            RithmicAccount::new("fcm", "ib", "ACC-2"),
        ));

        let account_id_a = AccountId::from("RITHMIC-TESTSYSTEM_ACC_1-ACC-1");
        let account_id_b = AccountId::from("RITHMIC-TESTSYSTEM_ACC_2-ACC-2");

        let connected_a = Arc::new(AtomicBool::new(true));
        let connected_b = Arc::new(AtomicBool::new(true));
        let order_reports_a = Arc::new(DashMap::new());
        let order_reports_b = Arc::new(DashMap::new());
        let venue_reports_a = Arc::new(DashMap::new());
        let venue_reports_b = Arc::new(DashMap::new());
        let fill_reports_a = Arc::new(DashMap::new());
        let fill_reports_b = Arc::new(DashMap::new());
        let position_reports_a = Arc::new(DashMap::new());
        let position_reports_b = Arc::new(DashMap::new());

        let task_a = get_runtime().spawn(run_execution_event_loop(
            exec_tx.subscribe(),
            pnl_tx.subscribe(),
            ExecutionEventLoopContext {
                gateway: Arc::clone(&gateway),
                inner: Arc::clone(&inner_a),
                account_id: account_id_a,
                emitter: sample_emitter(account_id_a),
                is_connected: Arc::clone(&connected_a),
                order_reports_by_client: Arc::clone(&order_reports_a),
                order_reports_by_venue: Arc::clone(&venue_reports_a),
                fill_reports: Arc::clone(&fill_reports_a),
                position_reports: Arc::clone(&position_reports_a),
                replay_lookback_secs: 0,
            },
        ));

        let task_b = get_runtime().spawn(run_execution_event_loop(
            exec_tx.subscribe(),
            pnl_tx.subscribe(),
            ExecutionEventLoopContext {
                gateway,
                inner: Arc::clone(&inner_b),
                account_id: account_id_b,
                emitter: sample_emitter(account_id_b),
                is_connected: Arc::clone(&connected_b),
                order_reports_by_client: Arc::clone(&order_reports_b),
                order_reports_by_venue: Arc::clone(&venue_reports_b),
                fill_reports: Arc::clone(&fill_reports_b),
                position_reports: Arc::clone(&position_reports_b),
                replay_lookback_secs: 0,
            },
        ));

        exec_tx
            .send(ExecutionEvent::Accepted(OrderAccepted {
                client_order_id: "CLIENT-A".to_string(),
                venue_order_id: "VENUE-A".to_string(),
                account_id: "ACC-1".to_string(),
                ts_event: 1,
                context: sample_order_context(),
            }))
            .unwrap();

        wait_for(
            || order_reports_a.contains_key(&ClientOrderId::from("CLIENT-A")),
            "tracked account A order",
        )
        .await;

        let mut fill_context = sample_order_context();
        fill_context.filled_qty = Some(1.0);
        fill_context.leaves_qty = Some(0.0);
        fill_context.avg_price = Some(5001.25);

        exec_tx
            .send(ExecutionEvent::Filled(OrderFilled {
                client_order_id: "CLIENT-A".to_string(),
                account_id: String::new(),
                venue_order_id: "VENUE-A".to_string(),
                fill_price: 5001.25,
                fill_qty: 1.0,
                leaves_qty: 0.0,
                commission: 0.0,
                ts_event: 2,
                trade_id: Some("TRADE-A".to_string()),
                currency: Some("USD".to_string()),
                context: fill_context,
            }))
            .unwrap();

        wait_for(
            || fill_reports_a.len() == 1,
            "accountless fill routed to tracked client",
        )
        .await;

        assert_eq!(fill_reports_a.len(), 1);
        assert_eq!(fill_reports_b.len(), 0);
        assert!(order_reports_a.contains_key(&ClientOrderId::from("CLIENT-A")));
        assert!(!order_reports_b.contains_key(&ClientOrderId::from("CLIENT-A")));

        drop(exec_tx);
        drop(pnl_tx);
        task_a.await.unwrap();
        task_b.await.unwrap();
    }
}
