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
    collections::hash_map::DefaultHasher,
    fmt::{self, Display},
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};
use nautilus_common::{
    clients::ExecutionClient,
    live::{
        get_runtime,
        runner::{try_get_data_event_sender, try_get_exec_event_sender},
        task::TaskHandles,
    },
    messages::{
        DataEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateFillReportsBuilder, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReports,
            GeneratePositionStatusReportsBuilder, ModifyOrder, QueryAccount, QueryOrder,
            SubmitOrder, SubmitOrderList,
        },
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
        PositionSideSpecified, TimeInForce, TrailingOffsetType,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{LIMIT_ORDER_TYPES, Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::RwLock as ParkingRwLock;
use rithmic_rs::{
    OrderSide as RithmicOrderSide, OrderType as RithmicOrderType, RithmicAccount,
    RithmicBracketOrder, RithmicOcoOrderLeg, TimeInForce as RithmicTif,
    rti::messages::RithmicMessage,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::{
    sync::{Mutex as AsyncMutex, RwLock, broadcast},
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

use super::client::{CommandFailureKind, OrderCommandError, first_command_response_error};

const ACCOUNT_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);
const ACCOUNT_REGISTRATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

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

fn replay_window_seconds(now: UnixNanos, replay_lookback_secs: u64) -> anyhow::Result<(i32, i32)> {
    let end_seconds = now.as_u64() / 1_000_000_000;
    let end_seconds = i32::try_from(end_seconds).map_err(|_| {
        anyhow::anyhow!("Current UNIX timestamp exceeds Rithmic i32 replay index: {end_seconds}")
    })?;
    let lookback = i32::try_from(replay_lookback_secs).map_err(|_| {
        anyhow::anyhow!(
            "Execution replay lookback exceeds Rithmic i32 index: {replay_lookback_secs}"
        )
    })?;
    let start_seconds = end_seconds.saturating_sub(lookback).max(0);
    Ok((start_seconds, end_seconds))
}

async fn await_account_registration<F>(
    account_id: AccountId,
    timeout: Duration,
    mut is_registered: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> bool,
{
    let start = Instant::now();

    loop {
        if is_registered() {
            log::info!("Rithmic account {account_id} registered in cache");
            return Ok(());
        }

        let elapsed = start.elapsed();
        if elapsed >= timeout {
            anyhow::bail!(
                "Timed out waiting for Rithmic account {account_id} registration after {timeout:?}"
            );
        }

        let remaining = timeout.saturating_sub(elapsed);
        tokio::time::sleep(ACCOUNT_REGISTRATION_POLL_INTERVAL.min(remaining)).await;
    }
}

async fn await_account_snapshot_observation(
    account_registered: &AtomicBool,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();

    while !account_registered.load(Ordering::Acquire) {
        let elapsed = start.elapsed();
        if elapsed >= timeout {
            anyhow::bail!("Timed out waiting for a fresh Rithmic account snapshot");
        }
        tokio::time::sleep(ACCOUNT_REGISTRATION_POLL_INTERVAL.min(timeout.saturating_sub(elapsed)))
            .await;
    }
    Ok(())
}

fn invalidate_account_readiness(is_ready: &AtomicBool, account_registered: &AtomicBool) {
    is_ready.store(false, Ordering::Release);
    account_registered.store(false, Ordering::Release);
}

fn observe_fresh_account_snapshot(
    is_ready: &AtomicBool,
    account_registered: &AtomicBool,
    awaiting_snapshot: &mut bool,
) {
    account_registered.store(true, Ordering::Release);
    if *awaiting_snapshot {
        *awaiting_snapshot = false;
        is_ready.store(true, Ordering::Release);
    }
}

fn lookback_start(ts_now: UnixNanos, lookback_mins: u64) -> UnixNanos {
    let lookback_ns = lookback_mins
        .checked_mul(60)
        .and_then(|seconds| seconds.checked_mul(1_000_000_000))
        .unwrap_or(u64::MAX);
    UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
}

fn emit_order_list_rejected(emitter: &ExecutionEventEmitter, orders: &[OrderAny], reason: &str) {
    let ts_event = get_atomic_clock_realtime().get_time_ns();

    for order in orders {
        emitter.emit_order_rejected(order, reason, ts_event, false);
    }
}

fn emit_order_list_denied(emitter: &ExecutionEventEmitter, orders: &[OrderAny], reason: &str) {
    for order in orders {
        emitter.emit_order_denied(order, reason);
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
    /// Authoritative execution readiness, including successful bootstrap/reconciliation.
    is_ready: Arc<AtomicBool>,
    /// Owner-thread proof that the execution account was observed in the engine cache.
    account_registered: Arc<AtomicBool>,
    reconciliation_gate: Arc<AsyncMutex<()>>,
    pending_tasks: TaskHandles,
    order_reports_by_client: Arc<DashMap<ClientOrderId, OrderStatusReport>>,
    order_reports_by_venue: Arc<DashMap<VenueOrderId, OrderStatusReport>>,
    fill_reports: Arc<DashMap<String, FillReport>>,
    position_reports: Arc<DashMap<String, PositionStatusReport>>,
    tracked_orders: Arc<DashMap<ClientOrderId, OrderAny>>,
    resolved_exchanges: Arc<ParkingRwLock<AHashMap<String, String>>>,
}

impl fmt::Debug for RithmicLiveExecClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicLiveExecClient))
            .field("client_id", &self.core.client_id)
            .field("account_id", &self.core.account_id)
            .field("is_ready", &self.is_ready.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for RithmicLiveExecClient {
    fn drop(&mut self) {
        self.cleanup_resources();
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
            is_ready: Arc::new(AtomicBool::new(false)),
            account_registered: Arc::new(AtomicBool::new(false)),
            reconciliation_gate: Arc::new(AsyncMutex::new(())),
            pending_tasks: TaskHandles::default(),
            order_reports_by_client: Arc::new(DashMap::new()),
            order_reports_by_venue: Arc::new(DashMap::new()),
            fill_reports: Arc::new(DashMap::new()),
            position_reports: Arc::new(DashMap::new()),
            tracked_orders: Arc::new(DashMap::new()),
            resolved_exchanges: Arc::new(ParkingRwLock::new(AHashMap::new())),
        }
    }

    fn require_inner(&self) -> anyhow::Result<Arc<RithmicExecutionClient>> {
        self.inner
            .clone()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient is not connected"))
    }

    fn require_ready_inner(&self) -> anyhow::Result<Arc<RithmicExecutionClient>> {
        if !self.is_ready.load(Ordering::Acquire) {
            anyhow::bail!("RithmicLiveExecClient is not ready for order commands");
        }
        self.require_inner()
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
        self.pending_tasks.push(handle);
    }

    fn abort_pending_tasks(&self) {
        self.pending_tasks.abort_all();
    }

    fn cleanup_resources(&mut self) {
        if let Some(task) = self.event_task.take() {
            task.abort();
        }
        self.abort_pending_tasks();
        self.inner = None;
        // Dropping the lease schedules its reference-counted asynchronous release.
        self.gateway = None;
        self.is_ready.store(false, Ordering::Release);
        self.account_registered.store(false, Ordering::Release);
        self.resolved_exchanges.write().clear();
        self.tracked_orders.clear();
        self.clear_cached_reports();
        self.core.set_disconnected();
    }

    fn reset_emitter(&mut self) {
        self.emitter = ExecutionEventEmitter::new(
            self.clock,
            self.core.trader_id,
            self.core.account_id,
            self.core.account_type,
            self.core.base_currency,
        );
    }

    fn clear_cached_reports(&self) {
        self.order_reports_by_client.clear();
        self.order_reports_by_venue.clear();
        self.fill_reports.clear();
        self.position_reports.clear();
    }

    fn owns_cached_order(&self, client_order_id: ClientOrderId) -> bool {
        self.core.cache().client_id(&client_order_id) == Some(&self.core.client_id)
    }

    fn refresh_tracked_orders(&self) {
        let orders: Vec<OrderAny> = self
            .core
            .cache()
            .orders(Some(&self.core.venue), None, None, None, None)
            .into_iter()
            .filter(|order| self.owns_cached_order(order.client_order_id()))
            .map(|order| order.cloned())
            .collect();
        self.tracked_orders.clear();

        for order in orders {
            self.tracked_orders.insert(order.client_order_id(), order);
        }
    }

    async fn await_account_registered(&self, timeout: Duration) -> anyhow::Result<()> {
        let account_id = self.core.account_id;
        await_account_registration(account_id, timeout, || {
            // The cache borrow is scoped to this predicate and is dropped before any await.
            self.account_registered.load(Ordering::Acquire)
                && self.core.cache().account(&account_id).is_some()
        })
        .await
    }

    async fn cache_missing_instruments_for_reports(
        &self,
        order_reports: &[OrderStatusReport],
        fill_reports: &[FillReport],
        position_reports: &[PositionStatusReport],
    ) {
        let data_sender = try_get_data_event_sender();
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
            match load_reconciliation_instrument(&gateway, &self.resolved_exchanges, instrument_id)
                .await
            {
                Ok(instrument) => {
                    if let Some(sender) = &data_sender {
                        if let Err(e) = sender.send(DataEvent::Instrument(instrument)) {
                            log::warn!("Failed to publish Rithmic reconciliation instrument: {e}");
                        }
                    } else {
                        log::warn!(
                            "No data event sender is installed; reconciliation instrument \
                             {instrument_id} could not be published to the data engine"
                        );
                    }
                }
                Err(e) => {
                    log::warn!("Failed to load Rithmic reconciliation instrument: {e}");
                }
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

    fn trailing_stop_config(
        order_type: OrderType,
        trailing_offset: Option<Decimal>,
        trailing_offset_type: Option<TrailingOffsetType>,
    ) -> anyhow::Result<Option<TrailingStopConfig>> {
        let is_trailing = matches!(
            order_type,
            OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
        );

        if !is_trailing {
            if trailing_offset.is_some()
                || trailing_offset_type
                    .is_some_and(|offset_type| offset_type != TrailingOffsetType::NoTrailingOffset)
            {
                anyhow::bail!("Trailing offset is only valid for trailing-stop orders");
            }
            return Ok(None);
        }

        let offset_type = trailing_offset_type
            .ok_or_else(|| anyhow::anyhow!("Trailing-stop order requires trailing_offset_type"))?;

        if offset_type != TrailingOffsetType::Ticks {
            anyhow::bail!(
                "Rithmic only supports TICKS trailing offset type, received {offset_type:?}"
            );
        }

        let offset = trailing_offset
            .ok_or_else(|| anyhow::anyhow!("Trailing-stop order requires trailing_offset"))?;

        if offset <= Decimal::ZERO {
            anyhow::bail!("Trailing offset must be positive, received: {offset}");
        }

        if offset.fract() != Decimal::ZERO {
            anyhow::bail!("Trailing offset must be a whole number of ticks, received: {offset}");
        }

        let trail_by_ticks = offset.to_i32().ok_or_else(|| {
            anyhow::anyhow!("Trailing offset exceeds Rithmic i32 tick limit: {offset}")
        })?;
        Ok(Some(TrailingStopConfig { trail_by_ticks }))
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
        let inner = self.require_ready_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let account = inner.account().clone();
        let orders = vec![spec.leg1.clone(), spec.leg2.clone()];

        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;
        let spec = spec.clone();
        self.spawn_task("submit_order_list_oco", async move {
            let prepared = async {
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

                Ok::<_, anyhow::Error>((handle, leg1, leg2))
            }
            .await;
            let (handle, leg1, leg2) = match prepared {
                Ok(prepared) => prepared,
                Err(e) => {
                    emit_order_list_denied(&emitter, &orders, &e.to_string());
                    return Err(e);
                }
            };

            for order in &orders {
                emitter.emit_order_submitted(order);
            }

            let result = match handle.place_oco_order(leg1, leg2).await {
                Ok(responses) => first_command_response_error(&responses).map_or_else(
                    || {
                        responses
                            .iter()
                            .any(|response| {
                                matches!(&response.message, RithmicMessage::ResponseOcoOrder(_))
                            })
                            .then_some(())
                            .ok_or_else(|| {
                                OrderCommandError::unknown("No OCO-order acknowledgement")
                            })
                    },
                    Err,
                ),
                Err(e) => Err(OrderCommandError::from_api(e)),
            };

            if let Err(failure) = result {
                match failure.kind() {
                    CommandFailureKind::Definitive => {
                        emit_order_list_rejected(&emitter, &orders, &failure.to_string());
                    }
                    CommandFailureKind::Unknown => {
                        reconcile_execution_readiness(
                            Arc::clone(&inner),
                            Arc::clone(&is_ready),
                            Arc::clone(&account_registered),
                            Arc::clone(&reconciliation_gate),
                            replay_lookback_secs,
                            "OCO submission",
                        )
                        .await;
                    }
                }
                return Err(anyhow::Error::new(failure));
            }

            Ok(())
        });

        Ok(())
    }

    fn submit_native_bracket_order_list(
        &self,
        spec: &NativeRithmicBracketSpec,
    ) -> anyhow::Result<()> {
        let inner = self.require_ready_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let account = inner.account().clone();
        let orders = vec![spec.entry.clone(), spec.stop.clone(), spec.target.clone()];

        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;
        let spec = spec.clone();
        self.spawn_task("submit_order_list_bracket", async move {
            let prepared = async {
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

                Ok::<_, anyhow::Error>((handle, bracket_order))
            }
            .await;
            let (handle, bracket_order) = match prepared {
                Ok(prepared) => prepared,
                Err(e) => {
                    emit_order_list_denied(&emitter, &orders, &e.to_string());
                    return Err(e);
                }
            };

            for order in &orders {
                emitter.emit_order_submitted(order);
            }

            let result = match handle.place_bracket_order(bracket_order).await {
                Ok(responses) => first_command_response_error(&responses).map_or_else(
                    || {
                        responses
                            .iter()
                            .any(|response| {
                                matches!(&response.message, RithmicMessage::ResponseBracketOrder(_))
                            })
                            .then_some(())
                            .ok_or_else(|| {
                                OrderCommandError::unknown("No bracket-order acknowledgement")
                            })
                    },
                    Err,
                ),
                Err(e) => Err(OrderCommandError::from_api(e)),
            };

            if let Err(failure) = result {
                match failure.kind() {
                    CommandFailureKind::Definitive => {
                        emit_order_list_rejected(&emitter, &orders, &failure.to_string());
                    }
                    CommandFailureKind::Unknown => {
                        reconcile_execution_readiness(
                            Arc::clone(&inner),
                            Arc::clone(&is_ready),
                            Arc::clone(&account_registered),
                            Arc::clone(&reconciliation_gate),
                            replay_lookback_secs,
                            "bracket submission",
                        )
                        .await;
                    }
                }
                return Err(anyhow::Error::new(failure));
            }

            Ok(())
        });

        Ok(())
    }

    /// Builds a `GatewayConfig` for the execution client (order + pnl plants).
    fn gateway_config(&self) -> crate::error::Result<GatewayConfig> {
        let c = &self.config;
        let mut cfg = GatewayConfig::new(
            c.environment,
            c.username.as_str(),
            c.password.as_str(),
            c.system_name.as_str(),
            c.app_name.as_str(),
            c.fcm_id.as_deref().unwrap_or(""),
            c.ib_id.as_deref().unwrap_or(""),
            c.account_id.as_str(),
        )?;
        cfg.app_version = c.app_version.clone();
        cfg.server = c.server.clone();
        cfg.alt_server = c.alt_server.clone();
        cfg.enable_ticker = true;
        cfg.enable_order = true;
        cfg.enable_pnl = true;
        cfg.enable_history = false;
        Ok(cfg)
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
            let (start_sec, end_sec) = replay_window_seconds(
                get_atomic_clock_realtime().get_time_ns(),
                replay_lookback_secs,
            )?;

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
        self.is_ready.load(Ordering::Acquire)
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
        log::info!("Stopping: client_id={}", self.core.client_id);
        self.cleanup_resources();
        self.reset_emitter();
        self.core.set_stopped();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.cleanup_resources();
        self.reset_emitter();
        self.core.set_stopped();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.cleanup_resources();
        self.reset_emitter();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;

        if self.is_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        if self.gateway.is_some() || self.inner.is_some() || self.event_task.is_some() {
            self.disconnect().await?;
        }
        self.refresh_tracked_orders();

        let gateway_config = self.gateway_config()?;
        let gateway = SharedGatewayLease::acquire(gateway_config.clone());
        let shared_gateway = gateway.gateway();
        let (exec_rx, pnl_rx) = {
            let guard = shared_gateway.read().await;
            let exec_rx = guard.subscribe_execution_events();
            let pnl_rx = guard.subscribe_pnl_events();
            (exec_rx, pnl_rx)
        };
        if let Err(e) = gateway.connect(&gateway_config).await {
            return Err(anyhow::Error::new(e).context("Gateway connect failed"));
        }

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
                is_ready: Arc::clone(&self.is_ready),
                account_registered: Arc::clone(&self.account_registered),
                reconciliation_gate: Arc::clone(&self.reconciliation_gate),
                order_reports_by_client: Arc::clone(&self.order_reports_by_client),
                order_reports_by_venue: Arc::clone(&self.order_reports_by_venue),
                fill_reports: Arc::clone(&self.fill_reports),
                position_reports: Arc::clone(&self.position_reports),
                tracked_orders: Arc::clone(&self.tracked_orders),
                replay_lookback_secs: self.config.execution_replay_lookback_secs,
            },
        ));

        self.inner = Some(inner);
        self.gateway = Some(gateway);
        self.event_task = Some(task);
        self.account_registered.store(false, Ordering::Release);
        let bootstrap_result = {
            let _guard = self.reconciliation_gate.lock().await;
            let result = Self::bootstrap_connection(
                self.require_inner()?,
                self.config.execution_replay_lookback_secs,
            )
            .await;

            let result = match result {
                Ok(()) => {
                    self.await_account_registered(ACCOUNT_REGISTRATION_TIMEOUT)
                        .await
                }
                Err(e) => Err(e),
            };

            match result {
                Ok(())
                    if self
                        .event_task
                        .as_ref()
                        .is_some_and(|task| !task.is_finished()) =>
                {
                    self.is_ready.store(true, Ordering::Release);
                    Ok(())
                }
                Ok(()) => Err(anyhow::anyhow!(
                    "Rithmic execution event loop stopped during bootstrap"
                )),
                Err(e) => Err(e),
            }
        };

        if let Err(e) = bootstrap_result {
            self.disconnect().await?;
            return Err(e.context("Rithmic execution bootstrap failed"));
        }

        self.core.set_connected();
        log::info!("Connected and ready: client_id={}", self.core.client_id);
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
        self.is_ready.store(false, Ordering::Release);
        self.account_registered.store(false, Ordering::Release);
        self.tracked_orders.clear();
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
        let inner = self.require_ready_inner()?;
        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RithmicLiveExecClient has no shared gateway lease"))?
            .gateway();
        let resolved_exchanges = Arc::clone(&self.resolved_exchanges);
        let o = &cmd.order_init;

        let preflight = (|| {
            let quantity = Self::whole_quantity(o.quantity)?;
            let side = to_rithmic_side(o.order_side)?;
            let order_type = to_rithmic_order_type(o.order_type)?;
            let time_in_force = to_rithmic_tif(o.time_in_force)?;
            let trailing_stop = Self::trailing_stop_config(
                o.order_type,
                o.trailing_offset,
                o.trailing_offset_type,
            )?;

            if LIMIT_ORDER_TYPES.contains(&o.order_type) && o.price.is_none() {
                anyhow::bail!("Limit/StopLimit order requires price");
            }

            if matches!(o.order_type, OrderType::StopMarket | OrderType::StopLimit)
                && o.trigger_price.is_none()
            {
                anyhow::bail!("Stop order requires trigger_price");
            }

            Ok((quantity, side, order_type, time_in_force, trailing_stop))
        })();
        let (quantity, order_side, order_type, time_in_force, trailing_stop) = match preflight {
            Ok(values) => values,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        self.tracked_orders
            .insert(order.client_order_id(), order.clone());
        let emitter = self.emitter.clone();
        let order_for_events = order;
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;
        let client_order_id = cmd.client_order_id.to_string();
        let instrument_id = cmd.instrument_id;
        let price = o.price.map(f64::from);
        let stop_price = o.trigger_price.map(f64::from);

        self.spawn_task("submit_order", async move {
            let (symbol, exchange) = match resolve_contract_exchange(
                &gateway,
                &resolved_exchanges,
                &instrument_id,
            )
            .await
            {
                Ok(contract) => contract,
                Err(e) => {
                    emitter.emit_order_denied(&order_for_events, &e.to_string());
                    return Err(e);
                }
            };
            let request = OrderRequest {
                client_order_id: client_order_id.clone(),
                symbol,
                exchange,
                side: order_side,
                order_type,
                time_in_force,
                quantity: f64::from(quantity),
                price,
                stop_price,
                trailing_stop,
            };
            emitter.emit_order_submitted(&order_for_events);

            match inner.submit_order_classified(request).await {
                Ok(()) => Ok(()),
                Err(failure) => {
                    match failure.kind() {
                        CommandFailureKind::Definitive => {
                            emitter.emit_order_rejected(
                                &order_for_events,
                                &failure.to_string(),
                                get_atomic_clock_realtime().get_time_ns(),
                                false,
                            );
                        }
                        CommandFailureKind::Unknown => {
                            reconcile_execution_readiness(
                                Arc::clone(&inner),
                                Arc::clone(&is_ready),
                                Arc::clone(&account_registered),
                                Arc::clone(&reconciliation_gate),
                                replay_lookback_secs,
                                &format!("submit {client_order_id}"),
                            )
                            .await;
                        }
                    }
                    Err(anyhow::Error::new(failure))
                }
            }
        });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;

        for order in &orders {
            self.tracked_orders
                .insert(order.client_order_id(), order.clone());
        }

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
        let inner = self.require_ready_inner()?;
        let client_order_id = cmd.client_order_id.to_string();
        let new_price = cmd.price.map(f64::from);

        if cmd.trigger_price.is_some() {
            let reason = "Rithmic does not support trigger-price modification";

            if let Some(order) = cached_order.as_ref() {
                self.emitter.emit_order_modify_rejected(
                    order,
                    order.venue_order_id(),
                    reason,
                    self.clock.get_time_ns(),
                );
            } else {
                self.emitter.emit_order_modify_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    cmd.venue_order_id,
                    reason,
                    self.clock.get_time_ns(),
                );
            }
            return Ok(());
        }

        let new_qty = match cmd.quantity.map(Self::whole_quantity).transpose() {
            Ok(quantity) => quantity.map(f64::from),
            Err(e) => {
                if let Some(order) = cached_order.as_ref() {
                    self.emitter.emit_order_modify_rejected(
                        order,
                        order.venue_order_id(),
                        &e.to_string(),
                        self.clock.get_time_ns(),
                    );
                } else {
                    self.emitter.emit_order_modify_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        cmd.venue_order_id,
                        &e.to_string(),
                        self.clock.get_time_ns(),
                    );
                }
                return Ok(());
            }
        };

        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;
        let command = cmd;
        self.spawn_task("modify_order", async move {
            match inner
                .modify_order_classified(&client_order_id, new_qty, new_price)
                .await
            {
                Ok(()) => Ok(()),
                Err(failure) => {
                    match failure.kind() {
                        CommandFailureKind::Definitive => {
                            if let Some(order) = cached_order.as_ref() {
                                emitter.emit_order_modify_rejected(
                                    order,
                                    order.venue_order_id(),
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            } else {
                                emitter.emit_order_modify_rejected_event(
                                    command.strategy_id,
                                    command.instrument_id,
                                    command.client_order_id,
                                    command.venue_order_id,
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            }
                        }
                        CommandFailureKind::Unknown => {
                            reconcile_execution_readiness(
                                Arc::clone(&inner),
                                Arc::clone(&is_ready),
                                Arc::clone(&account_registered),
                                Arc::clone(&reconciliation_gate),
                                replay_lookback_secs,
                                &format!("modify {client_order_id}"),
                            )
                            .await;
                        }
                    }
                    Err(anyhow::Error::new(failure))
                }
            }
        });
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cached_order = self.core.cache().order_owned(&cmd.client_order_id);
        let inner = self.require_ready_inner()?;
        let client_order_id = cmd.client_order_id.to_string();

        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;
        let command = cmd;
        self.spawn_task("cancel_order", async move {
            match inner.cancel_order_classified(&client_order_id).await {
                Ok(()) => Ok(()),
                Err(failure) => {
                    match failure.kind() {
                        CommandFailureKind::Definitive => {
                            if let Some(order) = cached_order.as_ref() {
                                emitter.emit_order_cancel_rejected(
                                    order,
                                    order.venue_order_id(),
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            } else {
                                emitter.emit_order_cancel_rejected_event(
                                    command.strategy_id,
                                    command.instrument_id,
                                    command.client_order_id,
                                    command.venue_order_id,
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            }
                        }
                        CommandFailureKind::Unknown => {
                            reconcile_execution_readiness(
                                Arc::clone(&inner),
                                Arc::clone(&is_ready),
                                Arc::clone(&account_registered),
                                Arc::clone(&reconciliation_gate),
                                replay_lookback_secs,
                                &format!("cancel {client_order_id}"),
                            )
                            .await;
                        }
                    }
                    Err(anyhow::Error::new(failure))
                }
            }
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let inner = self.require_ready_inner()?;
        let cached_orders: Vec<OrderAny> = self
            .core
            .cache()
            .orders(Some(&self.core.venue), None, None, None, None)
            .into_iter()
            .filter(|order| self.owns_cached_order(order.client_order_id()))
            .filter(|order| order.instrument_id() == cmd.instrument_id)
            .filter(|order| {
                cmd.order_side == OrderSide::NoOrderSide || order.order_side() == cmd.order_side
            })
            .map(|order| order.cloned())
            .collect();
        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;

        self.spawn_task("cancel_all_orders", async move {
            match inner.cancel_all_orders_classified().await {
                Ok(()) => Ok(()),
                Err(failure) => {
                    match failure.kind() {
                        CommandFailureKind::Definitive => {
                            for order in &cached_orders {
                                emitter.emit_order_cancel_rejected(
                                    order,
                                    order.venue_order_id(),
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            }
                        }
                        CommandFailureKind::Unknown => {
                            reconcile_execution_readiness(
                                Arc::clone(&inner),
                                Arc::clone(&is_ready),
                                Arc::clone(&account_registered),
                                Arc::clone(&reconciliation_gate),
                                replay_lookback_secs,
                                "cancel all orders",
                            )
                            .await;
                        }
                    }
                    Err(anyhow::Error::new(failure))
                }
            }
        });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let inner = self.require_ready_inner()?;
        let cancels: Vec<(CancelOrder, Option<OrderAny>)> = cmd
            .cancels
            .into_iter()
            .map(|cancel| {
                let order = self.core.cache().order_owned(&cancel.client_order_id);
                (cancel, order)
            })
            .collect();
        let emitter = self.emitter.clone();
        let is_ready = Arc::clone(&self.is_ready);
        let account_registered = Arc::clone(&self.account_registered);
        let reconciliation_gate = Arc::clone(&self.reconciliation_gate);
        let replay_lookback_secs = self.config.execution_replay_lookback_secs;

        self.spawn_task("batch_cancel_orders", async move {
            let mut failures = Vec::new();
            let mut has_unknown = false;

            for (cancel, order) in cancels {
                let client_order_id = cancel.client_order_id.to_string();

                if let Err(failure) = inner.cancel_order_classified(&client_order_id).await {
                    match failure.kind() {
                        CommandFailureKind::Definitive => {
                            if let Some(order) = order.as_ref() {
                                emitter.emit_order_cancel_rejected(
                                    order,
                                    order.venue_order_id(),
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            } else {
                                emitter.emit_order_cancel_rejected_event(
                                    cancel.strategy_id,
                                    cancel.instrument_id,
                                    cancel.client_order_id,
                                    cancel.venue_order_id,
                                    &failure.to_string(),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            }
                        }
                        CommandFailureKind::Unknown => has_unknown = true,
                    }
                    failures.push(format!("{client_order_id}: {failure}"));
                }
            }

            if has_unknown {
                reconcile_execution_readiness(
                    Arc::clone(&inner),
                    Arc::clone(&is_ready),
                    Arc::clone(&account_registered),
                    Arc::clone(&reconciliation_gate),
                    replay_lookback_secs,
                    "batch cancellation",
                )
                .await;
            }

            if failures.is_empty() {
                Ok(())
            } else {
                anyhow::bail!("Batch cancellation failures: {}", failures.join("; "))
            }
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
            && let Some(order_status) = to_model_order_status(local_order.status)
            && let Some(report) = report_from_local_order_state(
                self.core.account_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &local_order,
                order_status,
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
            .filter(|order| self.owns_cached_order(order.client_order_id()))
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

        let start = lookback_mins.map(|mins| lookback_start(ts_now, mins));

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
        is_ready,
        account_registered,
        reconciliation_gate,
        order_reports_by_client,
        order_reports_by_venue,
        fill_reports,
        position_reports,
        tracked_orders,
        replay_lookback_secs,
    } = ctx;
    let reconnect_timeout = tokio::time::sleep(ACCOUNT_REGISTRATION_TIMEOUT);
    tokio::pin!(reconnect_timeout);
    let mut awaiting_reconnect_account_snapshot = false;

    loop {
        tokio::select! {
            () = &mut reconnect_timeout, if awaiting_reconnect_account_snapshot => {
                log::error!(
                    "Timed out waiting for a fresh Rithmic account snapshot after reconnect"
                );
                break;
            }
            maybe_event = exec_rx.recv() => {
                let event = match maybe_event {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        log::warn!("Rithmic execution subscriber lagged by {skipped} events");
                        let reconciliation = start_event_loop_reconciliation(
                            Arc::clone(&inner),
                            Arc::clone(&is_ready),
                            Arc::clone(&account_registered),
                            Arc::clone(&reconciliation_gate),
                            replay_lookback_secs,
                            "execution stream lag",
                        ).await;
                        match reconciliation {
                            Some(true) => {
                                awaiting_reconnect_account_snapshot = true;
                                reconnect_timeout.as_mut().reset(
                                    tokio::time::Instant::now() + ACCOUNT_REGISTRATION_TIMEOUT,
                                );
                            }
                            Some(false) => {}
                            None => break,
                        }
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("Rithmic execution channel closed");
                        is_ready.store(false, Ordering::Release);
                        break;
                    }
                };

                if !event_matches_execution_client(&inner, &event) {
                    continue;
                }

                if !inner.apply_event(&event) {
                    continue;
                }

                match event {
                    ExecutionEvent::ConnectionState(ConnectionState::Reconnecting) => {
                        invalidate_account_readiness(&is_ready, &account_registered);
                        awaiting_reconnect_account_snapshot = false;
                        let reconnect_result = {
                            let mut gateway = gateway.write().await;
                            gateway.reconnect_if_needed().await
                        };

                        match reconnect_result {
                            Ok(()) => {
                                let bootstrap_result = {
                                    let _guard = reconciliation_gate.lock().await;
                                    let result = RithmicLiveExecClient::bootstrap_connection(
                                        Arc::clone(&inner),
                                        replay_lookback_secs,
                                    ).await;

                                    match result {
                                        Ok(()) => {
                                            awaiting_reconnect_account_snapshot = true;
                                            reconnect_timeout.as_mut().reset(
                                                tokio::time::Instant::now()
                                                    + ACCOUNT_REGISTRATION_TIMEOUT,
                                            );
                                            Ok(())
                                        }
                                        Err(e) => Err(e),
                                    }
                                };

                                if let Err(e) = bootstrap_result {
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
                            ExecutionUpdateStores {
                                order_reports_by_client: &order_reports_by_client,
                                order_reports_by_venue: &order_reports_by_venue,
                                fill_reports: &fill_reports,
                                tracked_orders: &tracked_orders,
                            },
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
                        let reconciliation = start_event_loop_reconciliation(
                            Arc::clone(&inner),
                            Arc::clone(&is_ready),
                            Arc::clone(&account_registered),
                            Arc::clone(&reconciliation_gate),
                            replay_lookback_secs,
                            "PnL stream lag",
                        ).await;
                        match reconciliation {
                            Some(true) => {
                                awaiting_reconnect_account_snapshot = true;
                                reconnect_timeout.as_mut().reset(
                                    tokio::time::Instant::now() + ACCOUNT_REGISTRATION_TIMEOUT,
                                );
                            }
                            Some(false) => {}
                            None => break,
                        }
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("Rithmic pnl channel closed");
                        is_ready.store(false, Ordering::Release);
                        break;
                    }
                };
                let observed_account_snapshot = process_pnl_update(
                    account_id,
                    inner.account_id(),
                    &emitter,
                    &position_reports,
                    event,
                );
                if observed_account_snapshot {
                    let was_awaiting_snapshot = awaiting_reconnect_account_snapshot;
                    observe_fresh_account_snapshot(
                        &is_ready,
                        &account_registered,
                        &mut awaiting_reconnect_account_snapshot,
                    );
                    if was_awaiting_snapshot {
                        log::info!(
                            "Rithmic execution readiness restored after fresh account snapshot"
                        );
                    }
                }
            }
        }
    }

    is_ready.store(false, Ordering::Release);
    account_registered.store(false, Ordering::Release);
}

async fn reconcile_execution_readiness(
    inner: Arc<RithmicExecutionClient>,
    is_ready: Arc<AtomicBool>,
    account_registered: Arc<AtomicBool>,
    reconciliation_gate: Arc<AsyncMutex<()>>,
    replay_lookback_secs: u64,
    reason: &str,
) -> bool {
    single_flight_reconciliation(is_ready, reconciliation_gate, reason, || async move {
        account_registered.store(false, Ordering::Release);
        RithmicLiveExecClient::bootstrap_connection(inner, replay_lookback_secs).await?;
        await_account_snapshot_observation(&account_registered, ACCOUNT_REGISTRATION_TIMEOUT).await
    })
    .await
}

/// Starts reconciliation from inside the event loop without waiting on the account snapshot that
/// the same loop must consume. `Some(true)` means readiness must remain false until that snapshot;
/// `Some(false)` means another single-flight caller already restored readiness.
async fn start_event_loop_reconciliation(
    inner: Arc<RithmicExecutionClient>,
    is_ready: Arc<AtomicBool>,
    account_registered: Arc<AtomicBool>,
    reconciliation_gate: Arc<AsyncMutex<()>>,
    replay_lookback_secs: u64,
    reason: &str,
) -> Option<bool> {
    let initiated_from_ready = is_ready.swap(false, Ordering::AcqRel);
    log::warn!("Rithmic execution state is stale after {reason}; reconciling");
    let _guard = reconciliation_gate.lock().await;

    if !initiated_from_ready && is_ready.load(Ordering::Acquire) {
        log::debug!("Rithmic execution readiness was already restored after {reason}");
        return Some(false);
    }

    account_registered.store(false, Ordering::Release);
    match RithmicLiveExecClient::bootstrap_connection(inner, replay_lookback_secs).await {
        Ok(()) => Some(true),
        Err(e) => {
            log::error!("Rithmic execution reconciliation failed after {reason}: {e}");
            None
        }
    }
}

async fn single_flight_reconciliation<F, Fut>(
    is_ready: Arc<AtomicBool>,
    reconciliation_gate: Arc<AsyncMutex<()>>,
    reason: &str,
    reconcile: F,
) -> bool
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let initiated_from_ready = is_ready.swap(false, Ordering::AcqRel);
    log::warn!("Rithmic execution state is stale after {reason}; reconciling");
    let _guard = reconciliation_gate.lock().await;

    if !initiated_from_ready && is_ready.load(Ordering::Acquire) {
        log::debug!("Rithmic execution readiness was already restored after {reason}");
        return true;
    }

    match reconcile().await {
        Ok(()) => {
            is_ready.store(true, Ordering::Release);
            log::info!("Rithmic execution readiness restored after {reason}");
            true
        }
        Err(e) => {
            log::error!("Rithmic execution reconciliation failed after {reason}: {e}");
            false
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

async fn load_reconciliation_instrument(
    gateway: &Arc<RwLock<RithmicGateway>>,
    resolved_exchanges: &Arc<ParkingRwLock<AHashMap<String, String>>>,
    instrument_id: InstrumentId,
) -> anyhow::Result<InstrumentAny> {
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
        apply_auxiliary_reference_data(&mut instrument, aux).map_err(|e| {
            anyhow::anyhow!("Failed to apply auxiliary data for {instrument_id}: {e}")
        })?;
    }

    Ok(InstrumentAny::FuturesContract(instrument))
}

struct ExecutionEventLoopContext {
    gateway: Arc<RwLock<RithmicGateway>>,
    inner: Arc<RithmicExecutionClient>,
    account_id: AccountId,
    emitter: ExecutionEventEmitter,
    is_ready: Arc<AtomicBool>,
    account_registered: Arc<AtomicBool>,
    reconciliation_gate: Arc<AsyncMutex<()>>,
    order_reports_by_client: Arc<DashMap<ClientOrderId, OrderStatusReport>>,
    order_reports_by_venue: Arc<DashMap<VenueOrderId, OrderStatusReport>>,
    fill_reports: Arc<DashMap<String, FillReport>>,
    position_reports: Arc<DashMap<String, PositionStatusReport>>,
    tracked_orders: Arc<DashMap<ClientOrderId, OrderAny>>,
    replay_lookback_secs: u64,
}

#[derive(Clone, Copy)]
struct ExecutionUpdateStores<'a> {
    order_reports_by_client: &'a DashMap<ClientOrderId, OrderStatusReport>,
    order_reports_by_venue: &'a DashMap<VenueOrderId, OrderStatusReport>,
    fill_reports: &'a DashMap<String, FillReport>,
    tracked_orders: &'a DashMap<ClientOrderId, OrderAny>,
}

fn process_execution_update(
    inner: &RithmicExecutionClient,
    account_id: AccountId,
    emitter: &ExecutionEventEmitter,
    stores: ExecutionUpdateStores<'_>,
    event: ExecutionEvent,
) {
    let ExecutionUpdateStores {
        order_reports_by_client,
        order_reports_by_venue,
        fill_reports,
        tracked_orders,
    } = stores;

    match event {
        ExecutionEvent::Submitted(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                event.venue_order_id.as_deref(),
            );
            if let (Some(order), Some(venue_order_id)) = (
                tracked_order.as_ref(),
                event
                    .venue_order_id
                    .as_deref()
                    .and_then(make_venue_order_id),
            ) {
                seed_tracked_order_report(
                    account_id,
                    order,
                    venue_order_id,
                    UnixNanos::from(event.ts_event),
                    order_reports_by_client,
                    order_reports_by_venue,
                );
            }
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

                if !event.context.is_snapshot && tracked_order.is_none() && local_order.is_none() {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Accepted(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            if let (Some(order), Some(venue_order_id)) = (
                tracked_order.as_ref(),
                make_venue_order_id(&event.venue_order_id),
            ) {
                seed_tracked_order_report(
                    account_id,
                    order,
                    venue_order_id,
                    UnixNanos::from(event.ts_event),
                    order_reports_by_client,
                    order_reports_by_venue,
                );
            }
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
                if let Some(order) = tracked_order.as_ref() {
                    emitter.emit_order_accepted(order, report.venue_order_id, report.ts_last);
                } else {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Rejected(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
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

            if !event.context.is_snapshot {
                if let Some(order) = tracked_order.as_ref().filter(|_| report.is_some()) {
                    emitter.emit_order_rejected(
                        order,
                        &event.reason,
                        UnixNanos::from(event.ts_event),
                        false,
                    );
                } else if let Some(report) = report {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Modified(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            if let (Some(order), Some(venue_order_id)) = (
                tracked_order.as_ref(),
                make_venue_order_id(&event.venue_order_id),
            ) {
                seed_tracked_order_report(
                    account_id,
                    order,
                    venue_order_id,
                    UnixNanos::from(event.ts_event),
                    order_reports_by_client,
                    order_reports_by_venue,
                );
            }
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
                if let Some(order) = tracked_order.as_ref() {
                    emitter.emit_order_updated(
                        order,
                        report.venue_order_id,
                        report.quantity,
                        report.price,
                        report.trigger_price,
                        None,
                        report.ts_last,
                    );
                } else {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Cancelled(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            if let (Some(order), Some(venue_order_id)) = (
                tracked_order.as_ref(),
                make_venue_order_id(&event.venue_order_id),
            ) {
                seed_tracked_order_report(
                    account_id,
                    order,
                    venue_order_id,
                    UnixNanos::from(event.ts_event),
                    order_reports_by_client,
                    order_reports_by_venue,
                );
            }
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
                if let Some(order) = tracked_order.as_ref() {
                    emitter.emit_order_canceled(order, Some(report.venue_order_id), report.ts_last);
                } else {
                    emitter.send_order_status_report(report);
                }
            }
        }
        ExecutionEvent::Filled(event) => {
            let tracked_order = make_client_order_id(&event.client_order_id)
                .and_then(|client_order_id| tracked_orders.get(&client_order_id))
                .map(|order| order.clone());
            let local_order = local_order_for_event(
                inner,
                event.client_order_id.as_str(),
                Some(event.venue_order_id.as_str()),
            );
            if let (Some(order), Some(venue_order_id)) = (
                tracked_order.as_ref(),
                make_venue_order_id(&event.venue_order_id),
            ) {
                seed_tracked_order_report(
                    account_id,
                    order,
                    venue_order_id,
                    UnixNanos::from(event.ts_event),
                    order_reports_by_client,
                    order_reports_by_venue,
                );
            }
            let fill_report = build_fill_report(
                account_id,
                &event,
                local_order.as_ref(),
                order_reports_by_client,
                order_reports_by_venue,
            );
            let is_new_fill = fill_report.as_ref().is_some_and(|report| {
                match fill_reports.entry(fill_report_key(report)) {
                    Entry::Vacant(entry) => {
                        entry.insert(report.clone());
                        true
                    }
                    Entry::Occupied(_) => false,
                }
            });

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
                && is_new_fill
                && let Some(report) = fill_report
            {
                if let Some(order) = tracked_order.as_ref() {
                    emitter.emit_order_filled(
                        order,
                        report.venue_order_id,
                        report.venue_position_id,
                        report.trade_id,
                        report.last_qty,
                        report.last_px,
                        report.commission.currency,
                        Some(report.commission),
                        report.liquidity_side,
                        report.ts_event,
                    );
                } else {
                    emitter.send_fill_report(report);
                }
            }

            if !event.context.is_snapshot
                && tracked_order.is_none()
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
) -> bool {
    match event {
        PnlEvent::Account(ProviderAccountEvent::BalanceUpdate(balance)) => {
            if balance.account_id != venue_account_id {
                return false;
            }
            let Some(currency) = parse_wire_currency(&balance.currency, "PnL balance") else {
                return false;
            };
            let Some(total) = make_money(balance.total, currency) else {
                return false;
            };
            let Some(locked) = make_money(balance.locked, currency) else {
                return false;
            };
            let Some(free) = make_money(balance.available, currency) else {
                return false;
            };
            let Ok(account_balance) = AccountBalance::new_checked(total, locked, free) else {
                log::warn!(
                    "Ignoring inconsistent Rithmic PnL balance for {account_id}: total={total}, locked={locked}, free={free}"
                );
                return false;
            };
            emitter.emit_account_state(
                vec![account_balance],
                Vec::new(),
                true,
                UnixNanos::from(balance.ts_event),
            );
            balance.is_snapshot
        }
        PnlEvent::Account(ProviderAccountEvent::Error(e)) => {
            log::error!("Rithmic pnl account error: {e}");
            false
        }
        PnlEvent::Account(ProviderAccountEvent::MarginWarning {
            account_id: warning_account_id,
            message,
        }) => {
            if warning_account_id != venue_account_id {
                return false;
            }
            log::warn!("Rithmic pnl margin warning for {account_id}: {message}");
            false
        }
        PnlEvent::Position(
            ProviderPositionEvent::Opened(position) | ProviderPositionEvent::Updated(position),
        ) => {
            if position.account_id != venue_account_id {
                return false;
            }
            let Some(instrument_id) = make_instrument_id(&position.symbol, &position.exchange)
            else {
                return false;
            };
            let Some(quantity) = make_quantity(position.quantity.abs(), 0) else {
                return false;
            };
            let position_side = if position.quantity > 0.0 {
                PositionSideSpecified::Long
            } else if position.quantity < 0.0 {
                PositionSideSpecified::Short
            } else {
                PositionSideSpecified::Flat
            };
            let avg_px_open = if position.quantity == 0.0 {
                None
            } else if let Some(avg_px) = Decimal::from_f64_retain(position.avg_price) {
                Some(avg_px)
            } else {
                log::warn!(
                    "Ignoring Rithmic position with invalid average price: symbol={} exchange={} avg_price={}",
                    position.symbol,
                    position.exchange,
                    position.avg_price
                );
                return false;
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
            false
        }
        PnlEvent::Position(ProviderPositionEvent::Closed {
            account_id: closed_account_id,
            symbol,
            exchange,
            ..
        }) => {
            if closed_account_id != venue_account_id {
                return false;
            }
            let Some(instrument_id) = make_instrument_id(&symbol, &exchange) else {
                return false;
            };
            position_reports.remove(&instrument_id.to_string());
            false
        }
        PnlEvent::Position(ProviderPositionEvent::Error(e)) => {
            log::error!("Rithmic pnl position error: {e}");
            false
        }
    }
}

fn parse_currency(value: &str) -> Option<Currency> {
    Currency::from_str(value).ok()
}

fn parse_wire_currency(value: &str, source: &str) -> Option<Currency> {
    match parse_currency(value) {
        Some(currency) => Some(currency),
        None => {
            log::warn!("Ignoring Rithmic {source} with invalid currency {value:?}");
            None
        }
    }
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

fn seed_tracked_order_report(
    account_id: AccountId,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
    client_reports: &DashMap<ClientOrderId, OrderStatusReport>,
    venue_reports: &DashMap<VenueOrderId, OrderStatusReport>,
) {
    if client_reports.contains_key(&order.client_order_id())
        || venue_reports.contains_key(&venue_order_id)
    {
        return;
    }

    let mut report = OrderStatusReport::new(
        account_id,
        order.instrument_id(),
        Some(order.client_order_id()),
        venue_order_id,
        order.order_side(),
        order.order_type(),
        order.time_in_force(),
        OrderStatus::Submitted,
        order.quantity(),
        order.filled_qty(),
        ts_event,
        ts_event,
        get_atomic_clock_realtime().get_time_ns(),
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

    if let Some(report) = sanitize_order_report(report, "tracked") {
        store_order_report(client_reports, venue_reports, report);
    }
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
    let venue_order_id = venue_order_id.or_else(|| {
        order
            .venue_order_id
            .as_deref()
            .and_then(make_venue_order_id)
    })?;
    let instrument_id = make_instrument_id(&order.symbol, &order.exchange)?;
    let order_side = to_model_side(order.side)?;
    let order_type = to_model_order_type(order.order_type)?;
    let time_in_force = to_model_tif(order.time_in_force)?;
    let quantity = make_quantity(order.quantity, 0)?;
    let filled_qty = make_quantity(order.filled_qty, 0)?;
    let price_precision = order
        .price
        .or(order.trigger_price)
        .map_or(2, infer_price_precision);

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        Some(client_order_id),
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
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

/// Builds a bounded fallback trade ID when Rithmic omits `fill_id`.
///
/// The 64-bit digest makes same-timestamp fills with different immutable fields distinct while
/// keeping the identifier short. As with any bounded digest, a theoretical hash collision can
/// still cause two otherwise distinct fills to share an ID.
fn fallback_fill_trade_id(event: &crate::execution::OrderFilled) -> Option<TradeId> {
    let mut hasher = DefaultHasher::new();
    event.venue_order_id.hash(&mut hasher);
    event.client_order_id.hash(&mut hasher);
    event.account_id.hash(&mut hasher);
    event.ts_event.hash(&mut hasher);
    event.fill_price.to_bits().hash(&mut hasher);
    event.fill_qty.to_bits().hash(&mut hasher);
    event.commission.to_bits().hash(&mut hasher);
    event.currency.hash(&mut hasher);
    make_trade_id(&format!("RITHMIC-{:016x}", hasher.finish()))
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

fn make_instrument_id(symbol: &str, exchange: &str) -> Option<InstrumentId> {
    match crate::common::converters::rithmic_instrument_id(symbol, exchange) {
        Ok(instrument_id) => Some(instrument_id),
        Err(e) => {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid instrument identity \
                 symbol={symbol:?} exchange={exchange:?}: {e}"
            );
            None
        }
    }
}

fn make_client_order_id(value: &str) -> Option<ClientOrderId> {
    ClientOrderId::new_checked(value)
        .map_err(|e| {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid client order ID {value:?}: {e}"
            );
        })
        .ok()
}

fn make_venue_order_id(value: &str) -> Option<VenueOrderId> {
    VenueOrderId::new_checked(value)
        .map_err(|e| {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid venue order ID {value:?}: {e}"
            );
        })
        .ok()
}

fn make_trade_id(value: &str) -> Option<TradeId> {
    TradeId::new_checked(value)
        .map_err(|e| {
            log::warn!("Ignoring Rithmic execution payload with invalid trade ID {value:?}: {e}");
        })
        .ok()
}

fn make_quantity(value: f64, precision: u8) -> Option<Quantity> {
    Quantity::new_checked(value, precision)
        .map_err(|e| {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid quantity {value} at precision {precision}: {e}"
            );
        })
        .ok()
}

fn make_price(value: f64, precision: u8) -> Option<Price> {
    Price::new_checked(value, precision)
        .map_err(|e| {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid price {value} at precision {precision}: {e}"
            );
        })
        .ok()
}

fn make_money(value: f64, currency: Currency) -> Option<Money> {
    Money::new_checked(value, currency)
        .map_err(|e| {
            log::warn!(
                "Ignoring Rithmic execution payload with invalid money amount {value} {currency}: {e}"
            );
        })
        .ok()
}

fn resolve_instrument_id(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> Option<InstrumentId> {
    previous
        .map(|report| report.instrument_id)
        .or_else(|| {
            local_order.and_then(|order| make_instrument_id(&order.symbol, &order.exchange))
        })
        .or_else(|| {
            context
                .symbol
                .as_deref()
                .zip(context.exchange.as_deref())
                .and_then(|(symbol, exchange)| make_instrument_id(symbol, exchange))
        })
}

fn resolve_order_side(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> Option<OrderSide> {
    previous
        .map(|report| report.order_side)
        .or_else(|| local_order.and_then(|order| to_model_side(order.side)))
        .or_else(|| context.side.and_then(to_model_side))
}

fn resolve_order_type(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> Option<OrderType> {
    previous
        .map(|report| report.order_type)
        .or_else(|| local_order.and_then(|order| to_model_order_type(order.order_type)))
        .or_else(|| context.order_type.and_then(to_model_order_type))
}

fn resolve_time_in_force(
    previous: Option<&OrderStatusReport>,
    local_order: Option<&OrderState>,
    context: &crate::execution::OrderContext,
) -> Option<TimeInForce> {
    previous
        .map(|report| report.time_in_force)
        .or_else(|| local_order.and_then(|order| to_model_tif(order.time_in_force)))
        .or_else(|| context.time_in_force.and_then(to_model_tif))
}

fn resolve_quantity(
    value: Option<f64>,
    fallback: Option<Quantity>,
    local_value: Option<f64>,
    precision: u8,
) -> Option<Quantity> {
    if let Some(fallback) = fallback {
        return Some(fallback);
    }

    if let Some(value) = value {
        return make_quantity(value, precision);
    }

    if let Some(local_value) = local_value {
        return make_quantity(local_value, precision);
    }
    None
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
            .and_then(|value| make_price(value, precision))
            .or_else(|| {
                local_value
                    .filter(|value| *value > 0.0)
                    .and_then(|value| make_price(value, precision))
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
    let venue_order_id = event
        .venue_order_id
        .as_deref()
        .and_then(make_venue_order_id)?;
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
    );
    let instrument_id = resolve_instrument_id(previous.as_ref(), local_order, &event.context)?;
    let order_side = resolve_order_side(previous.as_ref(), local_order, &event.context)?;
    let order_type = resolve_order_type(previous.as_ref(), local_order, &event.context)?;
    let time_in_force = resolve_time_in_force(previous.as_ref(), local_order, &event.context)?;
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
        client_order_id: Some(client_order_id),
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status: OrderStatus::Submitted,
        quantity: resolve_quantity(
            event.context.quantity,
            previous.as_ref().map(|r| r.quantity),
            local_order.map(|order| order.quantity),
            quantity_precision,
        )?,
        filled_qty: resolve_quantity(
            event.context.filled_qty,
            previous.as_ref().map(|r| r.filled_qty),
            local_order.map(|order| order.filled_qty),
            quantity_precision,
        )?,
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
    let venue_order_id = make_venue_order_id(&event.venue_order_id)?;
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
    );
    let instrument_id = resolve_instrument_id(previous.as_ref(), local_order, &event.context)?;
    let order_side = resolve_order_side(previous.as_ref(), local_order, &event.context)?;
    let order_type = resolve_order_type(previous.as_ref(), local_order, &event.context)?;
    let time_in_force = resolve_time_in_force(previous.as_ref(), local_order, &event.context)?;
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
        client_order_id: Some(client_order_id),
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status: OrderStatus::Accepted,
        quantity: resolve_quantity(
            event.context.quantity,
            previous.as_ref().map(|r| r.quantity),
            local_order.map(|order| order.quantity),
            quantity_precision,
        )?,
        filled_qty: resolve_quantity(
            event.context.filled_qty,
            previous.as_ref().map(|r| r.filled_qty),
            local_order.map(|order| order.filled_qty),
            quantity_precision,
        )?,
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
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let previous = find_existing_report(client_reports, venue_reports, Some(client_order_id), None);
    let previous = previous.or_else(|| {
        local_order.and_then(|order| {
            report_from_local_order_state(
                account_id,
                client_order_id,
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
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let venue_order_id = make_venue_order_id(&event.venue_order_id)?;
    let base = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
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
        )?,
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
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let venue_order_id = make_venue_order_id(&event.venue_order_id)?;
    let base = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
    )
    .or_else(|| {
        local_order.and_then(|order| {
            report_from_local_order_state(
                account_id,
                client_order_id,
                Some(venue_order_id),
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
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let venue_order_id = make_venue_order_id(&event.venue_order_id)?;
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
    let total_filled_value = event.context.filled_qty.unwrap_or_else(|| {
        let previous_filled = base
            .as_ref()
            .map_or(0.0, |report| report.filled_qty.as_f64());
        previous_filled + event.fill_qty
    });
    let total_filled = make_quantity(total_filled_value, quantity_precision)?;
    let leaves_value = event
        .context
        .leaves_qty
        .or(event.leaves_qty)
        .or_else(|| {
            base.as_ref()
                .map(|report| report.quantity.as_f64() - total_filled.as_f64())
        })
        .or_else(|| local_order.map(|order| order.quantity - total_filled.as_f64()))?;
    let leaves_qty = make_quantity(leaves_value, quantity_precision)?;
    let quantity = match base.as_ref() {
        Some(report) => report.quantity,
        None => make_quantity(
            total_filled.as_f64() + leaves_qty.as_f64(),
            quantity_precision,
        )?,
    };
    let order_side = resolve_order_side(base.as_ref(), local_order, &event.context)?;
    let order_type = resolve_order_type(base.as_ref(), local_order, &event.context)?;
    let time_in_force = resolve_time_in_force(base.as_ref(), local_order, &event.context)?;
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
    let client_order_id = make_client_order_id(&event.client_order_id)?;
    let venue_order_id = make_venue_order_id(&event.venue_order_id)?;
    let previous = find_existing_report(
        client_reports,
        venue_reports,
        Some(client_order_id),
        Some(venue_order_id),
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
    let quote_currency = parse_wire_currency(event.currency.as_deref().unwrap_or(""), "fill")?;
    let trade_id = match event.trade_id.as_deref() {
        Some(raw_trade_id) => make_trade_id(raw_trade_id)?,
        None => fallback_fill_trade_id(event)?,
    };
    let order_side = resolve_order_side(previous.as_ref(), local_order, &event.context)?;
    let last_qty = make_quantity(event.fill_qty, quantity_precision)?;
    let last_px = make_price(event.fill_price, price_precision)?;
    let commission = make_money(event.commission, quote_currency)?;
    Some(FillReport::new(
        account_id,
        instrument_id,
        venue_order_id,
        trade_id,
        order_side,
        last_qty,
        last_px,
        commission,
        LiquiditySide::NoLiquiditySide,
        Some(client_order_id),
        None,
        UnixNanos::from(event.ts_event),
        get_atomic_clock_realtime().get_time_ns(),
        None,
    ))
}

fn to_model_side(side: RithmicOrderSide) -> Option<OrderSide> {
    match side {
        RithmicOrderSide::Buy => Some(OrderSide::Buy),
        RithmicOrderSide::Sell => Some(OrderSide::Sell),
        _ => None,
    }
}

fn to_model_order_type(order_type: RithmicOrderType) -> Option<OrderType> {
    match order_type {
        RithmicOrderType::Market => Some(OrderType::Market),
        RithmicOrderType::Limit => Some(OrderType::Limit),
        RithmicOrderType::StopMarket => Some(OrderType::StopMarket),
        RithmicOrderType::StopLimit => Some(OrderType::StopLimit),
        _ => None,
    }
}

fn to_model_order_status(status: rithmic_rs::OrderStatus) -> Option<OrderStatus> {
    match status {
        rithmic_rs::OrderStatus::Pending => Some(OrderStatus::Submitted),
        rithmic_rs::OrderStatus::Open => Some(OrderStatus::Accepted),
        rithmic_rs::OrderStatus::Partial => Some(OrderStatus::PartiallyFilled),
        rithmic_rs::OrderStatus::Cancelled => Some(OrderStatus::Canceled),
        rithmic_rs::OrderStatus::Rejected => Some(OrderStatus::Rejected),
        rithmic_rs::OrderStatus::Expired => Some(OrderStatus::Expired),
        rithmic_rs::OrderStatus::Complete => Some(OrderStatus::Filled),
        rithmic_rs::OrderStatus::Unknown => None,
        _ => None,
    }
}

fn to_model_tif(tif: RithmicTif) -> Option<TimeInForce> {
    match tif {
        RithmicTif::Day => Some(TimeInForce::Day),
        RithmicTif::Gtc => Some(TimeInForce::Gtc),
        RithmicTif::Ioc => Some(TimeInForce::Ioc),
        RithmicTif::Fok => Some(TimeInForce::Fok),
        _ => None,
    }
}

fn is_empty_replay_error(error: &impl Display) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("replay") && message.contains("no data")
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use nautilus_common::{
        cache::Cache,
        live::get_runtime,
        messages::{ExecutionEvent as EngineExecutionEvent, ExecutionReport},
    };
    use nautilus_model::{
        enums::{AccountType, AssetClass},
        events::OrderEventAny,
        identifiers::{ClientId, OrderListId, StrategyId, TraderId},
        instruments::{FuturesContract, InstrumentAny},
        orders::{OrderList, builder::OrderTestBuilder, stubs::TestOrderEventStubs},
    };
    use tokio::{
        sync::{broadcast, mpsc},
        time::timeout,
    };
    use ustr::Ustr;

    use super::*;
    use crate::{
        config::RithmicEnv,
        execution::{OrderAccepted, OrderCancelled, OrderContext, OrderFilled},
        gateway::GatewayConfig,
        providers::{AccountBalance as ProviderAccountBalance, Position as ProviderPosition},
    };

    fn sample_report(order_status: OrderStatus, filled_qty: &str) -> OrderStatusReport {
        let mut report = OrderStatusReport::new(
            AccountId::from("RITHMIC-001"),
            InstrumentId::from("ESM6.CME.RITHMIC"),
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
            InstrumentId::from("ESM6.CME.RITHMIC"),
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
            InstrumentId::from("ESM6.CME.RITHMIC"),
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
        InstrumentId::from("ESM6.CME.RITHMIC")
    }

    fn sample_rithmic_instrument() -> InstrumentAny {
        InstrumentAny::FuturesContract(FuturesContract::new(
            sample_rithmic_instrument_id(),
            "ESM6".into(),
            AssetClass::Index,
            Some(Ustr::from("CME")),
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
            "NautilusTrader",
        )
        .expect("valid execution config");
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

    #[rstest::rstest]
    fn replay_window_uses_checked_i32_conversion() {
        let now = UnixNanos::from(1_700_000_000_000_000_000_u64);

        assert_eq!(
            replay_window_seconds(now, 60).unwrap(),
            (1_699_999_940, 1_700_000_000)
        );

        let timestamp_overflow = UnixNanos::from((i32::MAX as u64 + 1) * 1_000_000_000);
        assert!(replay_window_seconds(timestamp_overflow, 60).is_err());
        assert!(replay_window_seconds(now, i32::MAX as u64 + 1).is_err());
    }

    #[tokio::test]
    async fn account_registration_polling_is_bounded_and_rechecks() {
        let attempts = Cell::new(0_u8);
        await_account_registration(
            AccountId::from("RITHMIC-001"),
            Duration::from_millis(100),
            || {
                attempts.set(attempts.get() + 1);
                attempts.get() >= 2
            },
        )
        .await
        .unwrap();
        assert_eq!(attempts.get(), 2);

        assert!(
            await_account_registration(AccountId::from("RITHMIC-001"), Duration::ZERO, || false,)
                .await
                .is_err()
        );
    }

    #[rstest::rstest]
    fn mass_status_lookback_saturates_at_epoch_without_overflow() {
        assert_eq!(
            lookback_start(UnixNanos::from(120_000_000_000_u64), 1),
            UnixNanos::from(60_000_000_000_u64)
        );
        assert_eq!(
            lookback_start(UnixNanos::from(120_000_000_000_u64), u64::MAX),
            UnixNanos::default()
        );
    }

    #[rstest::rstest]
    fn unknown_local_order_status_is_not_invented() {
        assert_eq!(
            to_model_order_status(rithmic_rs::OrderStatus::Unknown),
            None
        );
    }

    #[rstest::rstest]
    fn reconnect_readiness_requires_fresh_account_snapshot() {
        let is_ready = AtomicBool::new(true);
        let account_registered = AtomicBool::new(true);
        invalidate_account_readiness(&is_ready, &account_registered);
        assert!(!is_ready.load(Ordering::Acquire));
        assert!(!account_registered.load(Ordering::Acquire));

        let mut awaiting_snapshot = false;
        observe_fresh_account_snapshot(&is_ready, &account_registered, &mut awaiting_snapshot);
        assert!(account_registered.load(Ordering::Acquire));
        assert!(!is_ready.load(Ordering::Acquire));

        invalidate_account_readiness(&is_ready, &account_registered);
        awaiting_snapshot = true;
        observe_fresh_account_snapshot(&is_ready, &account_registered, &mut awaiting_snapshot);
        assert!(account_registered.load(Ordering::Acquire));
        assert!(is_ready.load(Ordering::Acquire));
        assert!(!awaiting_snapshot);
    }

    #[rstest::rstest]
    fn fallback_fill_trade_id_distinguishes_same_timestamp_fills() {
        let base = OrderFilled {
            client_order_id: "CLIENT-1".to_string(),
            account_id: "ACC-1".to_string(),
            venue_order_id: "VENUE-1".to_string(),
            fill_price: 5000.25,
            fill_qty: 1.0,
            leaves_qty: Some(1.0),
            commission: 0.0,
            ts_event: 42,
            trade_id: None,
            currency: Some("USD".to_string()),
            context: sample_order_context(),
        };
        let first = fallback_fill_trade_id(&base).expect("valid fallback trade ID");
        let mut distinct = base;
        distinct.fill_qty = 2.0;
        let second = fallback_fill_trade_id(&distinct).expect("valid fallback trade ID");

        assert_ne!(first, second);
        assert!(first.to_string().starts_with("RITHMIC-"));
        assert!(first.to_string().len() <= 24);
    }

    #[tokio::test]
    async fn connect_validates_config_before_acquiring_gateway() {
        let cache = sample_cache();
        let mut client = sample_exec_client(cache);
        client.config.username.clear();

        let e = client
            .connect()
            .await
            .expect_err("invalid config should fail before gateway acquisition");

        assert!(e.to_string().contains("username"));
        assert!(client.gateway.is_none());
        assert!(client.inner.is_none());
        assert!(!client.is_ready.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn reconciliation_is_single_flight() {
        let is_ready = Arc::new(AtomicBool::new(true));
        let gate = Arc::new(AsyncMutex::new(()));
        let attempts = Arc::new(AtomicUsize::new(0));

        let first_attempts = Arc::clone(&attempts);
        let first = tokio::spawn(single_flight_reconciliation(
            Arc::clone(&is_ready),
            Arc::clone(&gate),
            "first lag",
            move || async move {
                first_attempts.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(())
            },
        ));

        while is_ready.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let second_attempts = Arc::clone(&attempts);
        let second = tokio::spawn(single_flight_reconciliation(
            Arc::clone(&is_ready),
            Arc::clone(&gate),
            "simultaneous lag",
            move || async move {
                second_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));

        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
        assert!(is_ready.load(Ordering::Acquire));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[rstest::rstest]
    fn trailing_stop_requires_positive_whole_tick_offsets() {
        let config = RithmicLiveExecClient::trailing_stop_config(
            OrderType::TrailingStopMarket,
            Some(Decimal::from(12)),
            Some(TrailingOffsetType::Ticks),
        )
        .unwrap()
        .unwrap();
        assert_eq!(config.trail_by_ticks, 12);

        assert!(
            RithmicLiveExecClient::trailing_stop_config(
                OrderType::TrailingStopMarket,
                Some(Decimal::from(12)),
                Some(TrailingOffsetType::Price),
            )
            .is_err()
        );
        assert!(
            RithmicLiveExecClient::trailing_stop_config(
                OrderType::TrailingStopMarket,
                Some(Decimal::from_str("1.5").unwrap()),
                Some(TrailingOffsetType::Ticks),
            )
            .is_err()
        );
        assert!(
            RithmicLiveExecClient::trailing_stop_config(
                OrderType::TrailingStopMarket,
                Some(Decimal::ZERO),
                Some(TrailingOffsetType::Ticks),
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    fn execution_reports_use_exchange_qualified_instrument_ids() {
        assert_eq!(
            make_instrument_id("ESM6", "CME").unwrap().to_string(),
            "ESM6.CME.RITHMIC"
        );
        assert_ne!(
            make_instrument_id("ESM6", "CME"),
            make_instrument_id("ESM6", "CBOT")
        );
        assert!(make_instrument_id("BAD.SYMBOL", "CME").is_none());
    }

    #[rstest::rstest]
    fn venue_numeric_identifier_and_currency_boundaries_are_checked() {
        assert!(make_quantity(f64::NAN, 0).is_none());
        assert!(make_quantity(f64::INFINITY, 0).is_none());
        assert!(make_price(f64::NEG_INFINITY, 2).is_none());
        assert!(make_money(f64::NAN, Currency::USD()).is_none());
        assert!(make_client_order_id("").is_none());
        assert!(make_venue_order_id("").is_none());
        assert!(make_trade_id("").is_none());
        assert!(parse_wire_currency("USD", "test").is_some());
        assert!(parse_wire_currency("", "test").is_none());
        assert!(parse_wire_currency("NOT_A_CURRENCY", "test").is_none());
    }

    #[rstest::rstest]
    fn pnl_balance_uses_venue_available_and_drops_inconsistent_values() {
        let account_id = AccountId::from("RITHMIC-001");
        let mut emitter = sample_emitter(account_id);
        let (tx, mut rx) = mpsc::unbounded_channel();
        emitter.set_sender(tx);
        let positions = DashMap::new();
        let balance = |available| {
            PnlEvent::Account(ProviderAccountEvent::BalanceUpdate(
                ProviderAccountBalance {
                    is_snapshot: true,
                    account_id: "ACC-1".to_string(),
                    currency: "USD".to_string(),
                    total: 100.0,
                    available,
                    locked: 20.0,
                    unrealized_pnl: 0.0,
                    realized_pnl: 0.0,
                    ts_event: 1,
                },
            ))
        };

        process_pnl_update(account_id, "ACC-1", &emitter, &positions, balance(80.0));
        let EngineExecutionEvent::Account(state) = rx.try_recv().unwrap() else {
            panic!("expected account state event");
        };
        assert_eq!(state.balances[0].free.as_f64(), 80.0);

        process_pnl_update(account_id, "ACC-1", &emitter, &positions, balance(70.0));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[rstest::rstest]
    fn incomplete_or_malformed_external_update_is_dropped() {
        let client_reports = DashMap::new();
        let venue_reports = DashMap::new();
        let context = crate::execution::OrderContext {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            ..Default::default()
        };
        let incomplete = OrderAccepted {
            client_order_id: "EXTERNAL-1".to_string(),
            venue_order_id: "VENUE-1".to_string(),
            account_id: "ACC-1".to_string(),
            ts_event: 1,
            context,
        };

        assert!(
            build_accept_report(
                AccountId::from("RITHMIC-001"),
                &incomplete,
                None,
                &client_reports,
                &venue_reports,
            )
            .is_none()
        );

        let mut malformed = incomplete;
        malformed.client_order_id.clear();
        assert!(
            build_accept_report(
                AccountId::from("RITHMIC-001"),
                &malformed,
                None,
                &client_reports,
                &venue_reports,
            )
            .is_none()
        );
    }

    #[rstest::rstest]
    fn reset_and_dispose_clear_execution_resources() {
        let mut client = sample_exec_client(sample_cache());
        client.core.set_started();
        client.core.set_connected();
        client.is_ready.store(true, Ordering::Release);
        client.event_task = Some(get_runtime().spawn(std::future::pending()));
        client
            .pending_tasks
            .push(get_runtime().spawn(std::future::pending()));
        client.order_reports_by_client.insert(
            ClientOrderId::from("O-1"),
            sample_report(OrderStatus::Accepted, "0"),
        );

        client.reset().unwrap();

        assert!(!client.is_connected());
        assert!(client.core.is_stopped());
        assert!(client.event_task.is_none());
        assert!(client.inner.is_none());
        assert!(client.gateway.is_none());
        assert!(client.pending_tasks.is_empty());
        assert!(client.order_reports_by_client.is_empty());

        client.core.set_started();
        client.is_ready.store(true, Ordering::Release);
        client.event_task = Some(get_runtime().spawn(std::future::pending()));
        client.dispose().unwrap();

        assert!(!client.is_connected());
        assert!(client.core.is_stopped());
        assert!(client.event_task.is_none());
    }

    fn sample_tracked_order(client_order_id: &str) -> OrderAny {
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        builder
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id(StrategyId::from("S-001"))
            .instrument_id(sample_rithmic_instrument_id())
            .client_order_id(ClientOrderId::from(client_order_id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("5000.25"))
            .time_in_force(TimeInForce::Day);
        builder.build()
    }

    fn sample_accepted_tracked_order(
        client_order_id: &str,
        venue_order_id: &str,
        account_id: AccountId,
    ) -> OrderAny {
        let mut order = sample_tracked_order(client_order_id);
        let submitted = TestOrderEventStubs::submitted(&order, account_id);
        order
            .apply(submitted)
            .expect("submitted event should apply");
        let accepted =
            TestOrderEventStubs::accepted(&order, account_id, VenueOrderId::from(venue_order_id));
        order.apply(accepted).expect("accepted event should apply");
        order
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
        let lease = SharedGatewayLease::acquire(
            client
                .gateway_config()
                .expect("valid execution gateway config"),
        );
        let gateway = lease.gateway();
        client.inner = Some(Arc::new(RithmicExecutionClient::new(
            gateway,
            RithmicAccount::new("fcm", "ib", "ACC-001"),
        )));
        client.gateway = Some(lease);
        client.is_ready.store(true, Ordering::Release);
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
            ExecutionUpdateStores {
                order_reports_by_client: &client_reports,
                order_reports_by_venue: &venue_reports,
                fill_reports: &fill_reports,
                tracked_orders: &DashMap::new(),
            },
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
            ExecutionUpdateStores {
                order_reports_by_client: &client_reports,
                order_reports_by_venue: &venue_reports,
                fill_reports: &fill_reports,
                tracked_orders: &DashMap::new(),
            },
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
            ExecutionUpdateStores {
                order_reports_by_client: &client_reports,
                order_reports_by_venue: &venue_reports,
                fill_reports: &fill_reports,
                tracked_orders: &DashMap::new(),
            },
            ExecutionEvent::Filled(OrderFilled {
                client_order_id: "CLIENT-2".to_string(),
                account_id: "ACC-1".to_string(),
                venue_order_id: "VENUE-2".to_string(),
                fill_price: 5000.50,
                fill_qty: 1.0,
                leaves_qty: Some(0.0),
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
    fn snapshot_fill_deduplicates_overlapping_live_fill() {
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
        let tracked_orders = DashMap::new();
        let stores = ExecutionUpdateStores {
            order_reports_by_client: &client_reports,
            order_reports_by_venue: &venue_reports,
            fill_reports: &fill_reports,
            tracked_orders: &tracked_orders,
        };
        let mut context = sample_order_context();
        context.is_snapshot = true;
        context.filled_qty = Some(1.0);
        context.leaves_qty = Some(0.0);
        let snapshot = OrderFilled {
            client_order_id: "EXTERNAL-SNAPSHOT".to_string(),
            account_id: "ACC-1".to_string(),
            venue_order_id: "VENUE-SNAPSHOT".to_string(),
            fill_price: 5000.25,
            fill_qty: 1.0,
            leaves_qty: Some(0.0),
            commission: 0.0,
            ts_event: 2,
            trade_id: Some("TRADE-SNAPSHOT".to_string()),
            currency: Some("USD".to_string()),
            context,
        };
        process_execution_update(
            &inner,
            account_id,
            &emitter,
            stores,
            ExecutionEvent::Filled(snapshot.clone()),
        );
        let mut live = snapshot;
        live.context.is_snapshot = false;
        process_execution_update(
            &inner,
            account_id,
            &emitter,
            stores,
            ExecutionEvent::Filled(live),
        );

        assert_eq!(fill_reports.len(), 1);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
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

        assert_eq!(
            report.instrument_id,
            InstrumentId::from("MNQM6.CME.RITHMIC")
        );
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
        let tracked_orders = DashMap::new();
        let stores = ExecutionUpdateStores {
            order_reports_by_client: &client_reports,
            order_reports_by_venue: &venue_reports,
            fill_reports: &fill_reports,
            tracked_orders: &tracked_orders,
        };
        let fill_event = ExecutionEvent::Filled(OrderFilled {
            client_order_id: "CLIENT-FILL".to_string(),
            account_id: "ACC-1".to_string(),
            venue_order_id: "VENUE-FILL".to_string(),
            fill_price: 5000.50,
            fill_qty: 1.0,
            leaves_qty: Some(0.0),
            commission: 0.0,
            ts_event: 2,
            trade_id: Some("TRADE-FILL".to_string()),
            currency: Some("USD".to_string()),
            context,
        });

        process_execution_update(&inner, account_id, &emitter, stores, fill_event.clone());
        process_execution_update(&inner, account_id, &emitter, stores, fill_event);

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
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn tracked_updates_emit_typed_order_events_instead_of_reports() {
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
        let tracked_orders = DashMap::new();
        tracked_orders.insert(
            ClientOrderId::from("TRACKED-1"),
            sample_tracked_order("TRACKED-1"),
        );
        let stores = ExecutionUpdateStores {
            order_reports_by_client: &client_reports,
            order_reports_by_venue: &venue_reports,
            fill_reports: &fill_reports,
            tracked_orders: &tracked_orders,
        };

        process_execution_update(
            &inner,
            account_id,
            &emitter,
            stores,
            ExecutionEvent::Accepted(OrderAccepted {
                client_order_id: "TRACKED-1".to_string(),
                venue_order_id: "VENUE-TRACKED-1".to_string(),
                account_id: "ACC-1".to_string(),
                ts_event: 1,
                context: sample_order_context(),
            }),
        );
        assert_eq!(rx.len(), 1, "tracked accept should emit one order event");
        process_execution_update(
            &inner,
            account_id,
            &emitter,
            stores,
            ExecutionEvent::Cancelled(OrderCancelled {
                client_order_id: "TRACKED-1".to_string(),
                account_id: "ACC-1".to_string(),
                venue_order_id: "VENUE-TRACKED-1".to_string(),
                ts_event: 2,
                context: sample_order_context(),
            }),
        );
        assert_eq!(rx.len(), 2, "tracked cancel should emit one order event");
        let mut fill_context = sample_order_context();
        fill_context.filled_qty = Some(1.0);
        fill_context.leaves_qty = None;
        process_execution_update(
            &inner,
            account_id,
            &emitter,
            stores,
            ExecutionEvent::Filled(OrderFilled {
                client_order_id: "TRACKED-1".to_string(),
                account_id: "ACC-1".to_string(),
                venue_order_id: "VENUE-TRACKED-1".to_string(),
                fill_price: 5000.25,
                fill_qty: 1.0,
                leaves_qty: None,
                commission: 0.0,
                ts_event: 3,
                trade_id: Some("TRADE-TRACKED-1".to_string()),
                currency: Some("USD".to_string()),
                context: fill_context,
            }),
        );
        assert_eq!(rx.len(), 3, "tracked fill should emit one order event");

        let events = recv_order_events(&mut rx, 3).await;
        assert!(matches!(events[0], OrderEventAny::Accepted(_)));
        assert!(matches!(events[1], OrderEventAny::Canceled(_)));
        assert!(matches!(events[2], OrderEventAny::Filled(_)));
    }

    #[tokio::test]
    async fn cache_reconciliation_isolated_by_execution_client() {
        let cache = sample_cache();
        let client = sample_exec_client(Rc::clone(&cache));
        let account_id = client.core.account_id;
        let own_order = sample_accepted_tracked_order("OWN-1", "OWN-V-1", account_id);
        let sibling_order = sample_accepted_tracked_order("SIBLING-1", "SIBLING-V-1", account_id);

        {
            let mut guard = cache.borrow_mut();
            guard
                .add_instrument(sample_rithmic_instrument())
                .expect("instrument should add to cache");
            guard
                .add_order(own_order, None, Some(client.core.client_id), true)
                .expect("own order should add to cache");
            guard
                .add_order(
                    sibling_order,
                    None,
                    Some(ClientId::new("SIBLING_EXEC")),
                    true,
                )
                .expect("sibling order should add to cache");
        }

        client.refresh_tracked_orders();
        assert!(
            client
                .tracked_orders
                .contains_key(&ClientOrderId::from("OWN-1"))
        );
        assert!(
            !client
                .tracked_orders
                .contains_key(&ClientOrderId::from("SIBLING-1"))
        );

        let cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .open_only(false)
            .build()
            .expect("valid reports command");
        let reports = client
            .generate_order_status_reports(&cmd)
            .await
            .expect("reports should generate");
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].client_order_id,
            Some(ClientOrderId::from("OWN-1"))
        );
        assert_eq!(reports[0].account_id, client.core.account_id);
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

        client.position_reports.insert(
            "ESM6.CME.RITHMIC".to_string(),
            sample_position_report(1, 12),
        );

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
    async fn submit_order_list_denies_oco_when_order_plant_is_unavailable() {
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

        let events = recv_order_events(&mut rx, 2).await;
        assert!(
            events
                .iter()
                .all(|event| matches!(event, OrderEventAny::Denied(_)))
        );
    }

    #[tokio::test]
    async fn submit_order_list_denies_bracket_when_order_plant_is_unavailable() {
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

        let events = recv_order_events(&mut rx, 3).await;
        assert!(
            events
                .iter()
                .all(|event| matches!(event, OrderEventAny::Denied(_)))
        );
    }

    fn sample_gateway() -> Arc<RwLock<RithmicGateway>> {
        Arc::new(RwLock::new(RithmicGateway::new(
            GatewayConfig::new(
                RithmicEnv::Demo,
                "user",
                "pass",
                "system",
                "TestApp",
                "fcm",
                "ib",
                "account",
            )
            .expect("valid gateway config"),
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
                is_ready: Arc::clone(&connected_a),
                account_registered: Arc::new(AtomicBool::new(true)),
                reconciliation_gate: Arc::new(AsyncMutex::new(())),
                order_reports_by_client: Arc::clone(&order_reports_a),
                order_reports_by_venue: Arc::clone(&venue_reports_a),
                fill_reports: Arc::clone(&fill_reports_a),
                position_reports: Arc::clone(&position_reports_a),
                tracked_orders: Arc::new(DashMap::new()),
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
                is_ready: Arc::clone(&connected_b),
                account_registered: Arc::new(AtomicBool::new(true)),
                reconciliation_gate: Arc::new(AsyncMutex::new(())),
                order_reports_by_client: Arc::clone(&order_reports_b),
                order_reports_by_venue: Arc::clone(&venue_reports_b),
                fill_reports: Arc::clone(&fill_reports_b),
                position_reports: Arc::clone(&position_reports_b),
                tracked_orders: Arc::new(DashMap::new()),
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
            .get("ESM6.CME.RITHMIC")
            .expect("expected account A position report");
        assert_eq!(position_a.account_id, account_id_a);
        let position_b = position_reports_b
            .get("NQM6.CME.RITHMIC")
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
                is_ready: Arc::clone(&connected_a),
                account_registered: Arc::new(AtomicBool::new(true)),
                reconciliation_gate: Arc::new(AsyncMutex::new(())),
                order_reports_by_client: Arc::clone(&order_reports_a),
                order_reports_by_venue: Arc::clone(&venue_reports_a),
                fill_reports: Arc::clone(&fill_reports_a),
                position_reports: Arc::clone(&position_reports_a),
                tracked_orders: Arc::new(DashMap::new()),
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
                is_ready: Arc::clone(&connected_b),
                account_registered: Arc::new(AtomicBool::new(true)),
                reconciliation_gate: Arc::new(AsyncMutex::new(())),
                order_reports_by_client: Arc::clone(&order_reports_b),
                order_reports_by_venue: Arc::clone(&venue_reports_b),
                fill_reports: Arc::clone(&fill_reports_b),
                position_reports: Arc::clone(&position_reports_b),
                tracked_orders: Arc::new(DashMap::new()),
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
                leaves_qty: Some(0.0),
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
