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

//! Live execution client implementation for ProjectX.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet, mapref::entry::Entry};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender, task::TaskHandles},
    messages::execution::{
        CancelOrder, GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
        GeneratePositionStatusReports, ModifyOrder, QueryAccount, SubmitOrder,
        cancel::CancelAllOrders,
    },
};
use nautilus_core::{MUTEX_POISONED, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce,
    },
    events::{
        AccountState, OrderAccepted, OrderCanceled, OrderEventAny, OrderExpired, OrderFillVoided,
        OrderFilled, OrderRejected,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Symbol, TradeId, TraderId,
        Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order as _, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use parking_lot::RwLock as ParkingRwLock;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    common::{
        consts::PROJECTX_VENUE,
        enums::ProjectXHub,
        symbols::{databento_to_projectx_symbol, projectx_to_databento_symbol},
    },
    config::{
        ProjectXExecClientConfig, canonicalize_projectx_account_id, projectx_account_id_from_raw,
    },
    http::{client::ProjectXHttpClient, error::ProjectXHttpError},
    websocket::client::{ProjectXWsClient, ProjectXWsEvent},
};
use projectx_client::{
    Account, AccountId as PxAccountId, CancelOrder as PxCancelOrder, ContractId,
    ModifyOrder as PxModifyOrder, Order, OrderSearch, OrderStatus as ProjectXOrderStatus,
    OrderType as ProjectXOrderType, PlaceOrder, Position, Side, Timestamp as ProjectXTimestamp,
    Trade, TradeSearch,
};
use rust_decimal::prelude::ToPrimitive;

const MAX_TRACKED_TRADE_IDS: usize = 65_536;
const TRADE_ID_PRUNE_BATCH: usize = 4_096;
const MAX_PENDING_TRADE_ORDERS: usize = 1_024;
const MAX_PENDING_TRADES_PER_ORDER: usize = 64;

#[derive(Clone, Copy, Debug)]
struct ProjectXOrderMeta {
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    order_side: OrderSide,
    order_type: OrderType,
}

#[derive(Debug)]
struct ReconciliationSnapshot {
    open_orders: Vec<Order>,
    recent_trade_orders: Vec<Order>,
    positions: Vec<Position>,
    trades: Vec<Trade>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OpenOrderState {
    status: i32,
    fill_volume: i64,
    filled_price: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PositionState {
    size: i64,
    type_: i32,
    average_price: Decimal,
}

type AccountIdCache = Arc<ParkingRwLock<AHashMap<i64, AccountId>>>;
type SubscribedAccountIdCache = Arc<ParkingRwLock<AHashSet<i64>>>;

#[derive(Debug)]
pub struct ProjectXExecutionClient {
    core: ExecutionClientCore,
    emitter: ExecutionEventEmitter,
    http_client: ProjectXHttpClient,
    ws_user: Option<ProjectXWsClient>,
    account_id_num: Arc<Mutex<Option<i64>>>,
    account_ids: AccountIdCache,
    subscribed_account_ids: SubscribedAccountIdCache,
    ws_event_task: Option<tokio::task::JoinHandle<()>>,
    pending_tasks: TaskHandles,
    order_meta_by_client: Arc<DashMap<ClientOrderId, ProjectXOrderMeta>>,
    venue_to_client: Arc<DashMap<i64, ClientOrderId>>,
    last_order_state: Arc<DashMap<ClientOrderId, OpenOrderState>>,
    last_order_status: Arc<DashMap<ClientOrderId, i32>>,
    seen_trade_ids: Arc<DashSet<i64>>,
    voided_trade_ids: Arc<DashSet<i64>>,
    pending_trades_by_order_id: Arc<DashMap<i64, Vec<Trade>>>,
    open_order_state: Arc<DashMap<i64, OpenOrderState>>,
    open_position_state: Arc<DashMap<i64, PositionState>>,
    execution_stale: Arc<AtomicBool>,
    reconciliation_in_progress: Arc<AtomicBool>,
    margin_support_warned: Arc<AtomicBool>,
}

#[derive(Clone)]
struct ReconciliationContext {
    http_client: ProjectXHttpClient,
    ws_user: Option<ProjectXWsClient>,
    emitter: ExecutionEventEmitter,
    account_ids: AccountIdCache,
    subscribed_account_ids: SubscribedAccountIdCache,
    account_issuer: Venue,
    account_type: AccountType,
    base_currency: Option<Currency>,
    order_meta_by_client: Arc<DashMap<ClientOrderId, ProjectXOrderMeta>>,
    venue_to_client: Arc<DashMap<i64, ClientOrderId>>,
    seen_trade_ids: Arc<DashSet<i64>>,
    voided_trade_ids: Arc<DashSet<i64>>,
    open_order_state: Arc<DashMap<i64, OpenOrderState>>,
    open_position_state: Arc<DashMap<i64, PositionState>>,
    execution_stale: Arc<AtomicBool>,
    reconciliation_in_progress: Arc<AtomicBool>,
    instrument_aliases: Arc<HashMap<String, InstrumentId>>,
}

struct ReconciliationFlagGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for ReconciliationFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

struct AccountStateRefreshContext {
    http_client: ProjectXHttpClient,
    emitter: ExecutionEventEmitter,
    account_id_num: Arc<Mutex<Option<i64>>>,
    account_ids: AccountIdCache,
    subscribed_account_ids: SubscribedAccountIdCache,
    account_id: AccountId,
    base_currency: Option<Currency>,
    account_type: AccountType,
    margin_support_warned: Arc<AtomicBool>,
}

impl AccountStateRefreshContext {
    fn cache_account_ids(&self, accounts: &[Account]) -> anyhow::Result<Vec<i64>> {
        let mut ids = Vec::with_capacity(accounts.len());
        let mut mapped = AHashMap::with_capacity(accounts.len());

        for account in accounts {
            ids.push(i64::from(account.id.get()));
            let cached_account_id = projectx_account_id_from_raw(account.name.as_str())?;
            mapped.insert(i64::from(account.id.get()), cached_account_id);
        }
        ids.sort_unstable();
        ids.dedup();
        *self.account_ids.write() = mapped;
        Ok(ids)
    }

    fn account_num_from_account_id(&self, account_id: AccountId) -> Option<i64> {
        self.account_ids
            .read()
            .iter()
            .find_map(|(key, value)| (*value == account_id).then_some(*key))
    }

    fn select_subscribed_account_id_num(&self, ids: &[i64]) -> anyhow::Result<i64> {
        let configured = self
            .account_num_from_account_id(self.account_id)
            .or_else(|| {
                ProjectXExecutionClient::parse_account_num_from_account_id(self.account_id)
            });

        if let Some(account_id_num) = configured {
            if ids.contains(&account_id_num) {
                return Ok(account_id_num);
            }

            anyhow::bail!(
                "Configured ProjectX account {} was not returned by /api/Account/search",
                self.account_id
            );
        }

        if ids.len() == 1 {
            return Ok(ids[0]);
        }

        anyhow::bail!(
            "ProjectX login returned multiple accounts; configure a specific account_id to select one"
        )
    }

    fn bind_selected_account(&self, account_id_num: i64) {
        self.account_ids
            .write()
            .insert(account_id_num, self.account_id);
        let mut subscribed_account_ids = self.subscribed_account_ids.write();
        subscribed_account_ids.clear();
        subscribed_account_ids.insert(account_id_num);
        *self.account_id_num.lock().expect(MUTEX_POISONED) = Some(account_id_num);
    }

    async fn resolve_all_account_ids_num(&self) -> anyhow::Result<Vec<i64>> {
        let accounts = self.http_client.search_accounts().await?;

        if accounts.is_empty() {
            anyhow::bail!("No ProjectX accounts returned by /api/Account/search");
        }

        let ids = self.cache_account_ids(&accounts)?;
        let selected = self.select_subscribed_account_id_num(&ids)?;
        let account = accounts
            .iter()
            .find(|account| i64::from(account.id.get()) == selected)
            .ok_or_else(|| anyhow::anyhow!("Selected ProjectX account {selected} disappeared"))?;
        if !account.can_trade {
            anyhow::bail!("Selected ProjectX account {selected} is not permitted to trade");
        }
        if !account.is_visible {
            anyhow::bail!("Selected ProjectX account {selected} is not visible to this login");
        }
        self.bind_selected_account(selected);
        Ok(vec![selected])
    }

    async fn resolve_account_id_num(&self) -> anyhow::Result<i64> {
        if let Some(id) = *self.account_id_num.lock().expect(MUTEX_POISONED) {
            return Ok(id);
        }

        let ids = self.resolve_all_account_ids_num().await?;
        Ok(ids[0])
    }

    async fn refresh(&self) -> anyhow::Result<()> {
        let account_id = self.resolve_account_id_num().await?;
        let mut accounts = self.http_client.search_accounts().await?;

        if accounts.is_empty() {
            anyhow::bail!("No ProjectX accounts returned by /api/Account/search");
        }
        self.cache_account_ids(&accounts)?;
        self.bind_selected_account(account_id);

        let selected_index = accounts
            .iter()
            .position(|account| i64::from(account.id.get()) == account_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Selected ProjectX account {account_id} was not returned by /api/Account/search"
                )
            })?;
        let account = accounts.swap_remove(selected_index);
        if !account.can_trade {
            anyhow::bail!("Selected ProjectX account {account_id} is not permitted to trade");
        }
        if !account.is_visible {
            anyhow::bail!("Selected ProjectX account {account_id} is not visible to this login");
        }
        let currency = self.base_currency.unwrap_or_else(Currency::USD);
        let raw_balance = account.balance.ok_or_else(|| {
            anyhow::anyhow!(
                "ProjectX account {} snapshot omitted the account balance",
                account.id
            )
        })?;
        let total = Money::from_decimal(raw_balance, currency)?;
        let locked = Money::from_decimal(Decimal::ZERO, currency)?;
        let free = total;
        let balance = AccountBalance::new(total, locked, free);

        if self.account_type == AccountType::Margin
            && !self.margin_support_warned.swap(true, Ordering::SeqCst)
        {
            log::info!(
                "ProjectX account snapshots expose balance but not detailed margin/free-margin fields; emitting a margin account with empty margins",
            );
        }

        self.emitter.emit_account_state(
            vec![balance],
            Vec::new(),
            true,
            get_atomic_clock_realtime().get_time_ns(),
        );
        Ok(())
    }
}

impl ReconciliationContext {
    async fn reconcile(&self, reason: &str) -> bool {
        self.execution_stale.store(true, Ordering::SeqCst);

        if self
            .reconciliation_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            log::debug!("ProjectX reconciliation already in progress ({reason})");
            return false;
        }

        let _flag_guard = ReconciliationFlagGuard {
            flag: Arc::clone(&self.reconciliation_in_progress),
        };
        let account_ids_num = self
            .subscribed_account_ids
            .read()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut reconciled = !account_ids_num.is_empty();

        if account_ids_num.is_empty() {
            log::warn!("ProjectX cannot reconcile {reason}: no subscribed account IDs");
        }

        for account_id_num in &account_ids_num {
            match ProjectXExecutionClient::fetch_runtime_reconciliation_snapshot_from_http(
                &self.http_client,
                *account_id_num,
            )
            .await
            {
                Ok(snapshot) => {
                    if let Err(e) =
                        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
                            snapshot,
                            &self.account_ids,
                            self.account_issuer,
                            self.base_currency,
                            &self.emitter,
                            &self.order_meta_by_client,
                            &self.venue_to_client,
                            &self.seen_trade_ids,
                            &self.voided_trade_ids,
                            &self.open_order_state,
                            &self.open_position_state,
                            get_atomic_clock_realtime().get_time_ns(),
                            true,
                            Some(&self.instrument_aliases),
                        )
                    {
                        reconciled = false;
                        log::warn!(
                            "ProjectX reconciliation snapshot was incomplete for account {account_id_num} ({reason}): {e}"
                        );
                    }
                }
                Err(e) => {
                    reconciled = false;
                    log::warn!(
                        "ProjectX reconciliation failed for account {account_id_num} ({reason}): {e}"
                    );
                }
            }
        }

        if reconciled
            && let Err(e) = ProjectXExecutionClient::refresh_account_state_with_context(
                &self.http_client,
                &account_ids_num,
                &self.account_ids,
                self.account_issuer,
                self.account_type,
                self.base_currency,
                &self.emitter,
            )
            .await
        {
            reconciled = false;
            log::warn!("ProjectX account refresh failed after reconciliation ({reason}): {e}");
        }

        if reconciled
            && !self
                .ws_user
                .as_ref()
                .is_some_and(ProjectXWsClient::is_connected)
        {
            reconciled = false;
            log::warn!(
                "ProjectX reconciliation completed while the user stream was disconnected ({reason}); execution remains fenced"
            );
        }

        if reconciled {
            self.execution_stale.store(false, Ordering::SeqCst);
            log::info!("ProjectX reconciliation completed ({reason})");
        }
        reconciled
    }
}

impl ProjectXExecutionClient {
    fn normalize_projectx_symbol_key(value: &str) -> String {
        value.trim().to_ascii_uppercase()
    }

    fn cache_instrument_aliases(
        aliases: &mut HashMap<String, InstrumentId>,
        instrument: &InstrumentAny,
    ) {
        let instrument_id = instrument.id();
        aliases.insert(
            Self::normalize_projectx_symbol_key(instrument_id.symbol.inner().as_str()),
            instrument_id,
        );
        aliases.insert(
            Self::normalize_projectx_symbol_key(instrument.raw_symbol().as_str()),
            instrument_id,
        );

        if let Some(contract_id) = Self::cached_contract_id_from_instrument(instrument) {
            aliases.insert(
                Self::normalize_projectx_symbol_key(contract_id),
                instrument_id,
            );
        }

        if let InstrumentAny::FuturesContract(inst) = instrument
            && let Some(info) = inst.info.as_ref()
        {
            if let Some(name) = info.get_str("projectx_name") {
                aliases.insert(Self::normalize_projectx_symbol_key(name), instrument_id);
            }

            if let Some(symbol_id) = info.get_str("projectx_symbol_id") {
                aliases.insert(
                    Self::normalize_projectx_symbol_key(symbol_id),
                    instrument_id,
                );
            }
        }
    }

    fn instrument_aliases_from_cache(cache: &Cache) -> HashMap<String, InstrumentId> {
        let mut aliases = HashMap::new();

        for instrument in cache.instruments(&PROJECTX_VENUE, None) {
            Self::cache_instrument_aliases(&mut aliases, instrument);
        }
        aliases
    }

    /// Creates a new [`ProjectXExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the transport client cannot be created.
    pub fn new(
        core: ExecutionClientCore,
        config: ProjectXExecClientConfig,
    ) -> anyhow::Result<Self> {
        let mut core = core;
        core.set_account_id(canonicalize_projectx_account_id(core.account_id)?);
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let http_client = ProjectXHttpClient::from_config(config.transport)?;

        Ok(Self {
            core,
            emitter,
            http_client,
            ws_user: None,
            account_id_num: Arc::new(Mutex::new(None)),
            account_ids: Arc::new(ParkingRwLock::new(AHashMap::new())),
            subscribed_account_ids: Arc::new(ParkingRwLock::new(AHashSet::new())),
            ws_event_task: None,
            pending_tasks: TaskHandles::default(),
            order_meta_by_client: Arc::new(DashMap::new()),
            venue_to_client: Arc::new(DashMap::new()),
            last_order_state: Arc::new(DashMap::new()),
            last_order_status: Arc::new(DashMap::new()),
            seen_trade_ids: Arc::new(DashSet::new()),
            voided_trade_ids: Arc::new(DashSet::new()),
            pending_trades_by_order_id: Arc::new(DashMap::new()),
            open_order_state: Arc::new(DashMap::new()),
            open_position_state: Arc::new(DashMap::new()),
            execution_stale: Arc::new(AtomicBool::new(true)),
            reconciliation_in_progress: Arc::new(AtomicBool::new(false)),
            margin_support_warned: Arc::new(AtomicBool::new(false)),
        })
    }

    fn parse_order_side(side: OrderSide) -> anyhow::Result<Side> {
        match side {
            OrderSide::Buy => Ok(Side::Bid),
            OrderSide::Sell => Ok(Side::Ask),
            _ => anyhow::bail!("Unsupported ProjectX order side: {side:?}"),
        }
    }

    fn parse_order_type(order_type: OrderType) -> anyhow::Result<ProjectXOrderType> {
        match order_type {
            OrderType::Limit => Ok(ProjectXOrderType::Limit),
            OrderType::Market => Ok(ProjectXOrderType::Market),
            OrderType::StopMarket => Ok(ProjectXOrderType::Stop),
            _ => anyhow::bail!("Unsupported ProjectX order type: {order_type:?}"),
        }
    }

    fn quantity_to_i64(quantity: Quantity) -> anyhow::Result<i64> {
        let value = quantity.as_decimal();

        if value <= Decimal::ZERO {
            anyhow::bail!("ProjectX quantity must be positive, was {value}");
        }

        if !value.fract().is_zero() {
            anyhow::bail!("ProjectX quantity must be whole contracts, was {value}");
        }
        value
            .to_i64()
            .ok_or_else(|| anyhow::anyhow!("ProjectX quantity is outside the i64 range: {value}"))
    }

    fn price_to_decimal(price: Option<Price>) -> Option<Decimal> {
        price.map(|p| p.as_decimal())
    }

    fn validate_order_capabilities(order: &OrderAny) -> anyhow::Result<()> {
        if order.time_in_force() != TimeInForce::Gtc {
            anyhow::bail!(
                "ProjectX supports only GTC orders, received {:?}",
                order.time_in_force()
            );
        }
        if order.is_post_only() {
            anyhow::bail!("ProjectX post-only orders are not supported");
        }
        if order.is_reduce_only() {
            anyhow::bail!("ProjectX reduce-only orders are not supported");
        }
        if order.is_quote_quantity() {
            anyhow::bail!("ProjectX quote-quantity orders are not supported");
        }

        match order.order_type() {
            OrderType::Limit if order.price().is_none() => {
                anyhow::bail!("ProjectX limit orders require a limit price");
            }
            OrderType::StopMarket if order.trigger_price().is_none() => {
                anyhow::bail!("ProjectX stop-market orders require a trigger price");
            }
            OrderType::Limit | OrderType::Market | OrderType::StopMarket => {}
            unsupported => anyhow::bail!(
                "ProjectX cannot preserve {unsupported:?} order semantics; the order was not submitted"
            ),
        }
        Ok(())
    }

    fn build_place_order_request(
        &self,
        order: &OrderAny,
        account_id_num: i64,
    ) -> anyhow::Result<PlaceOrder> {
        Self::validate_order_capabilities(order)?;
        let account_id = Self::to_client_account_id(account_id_num)?;
        let order_type = Self::parse_order_type(order.order_type())?;
        let side = Self::parse_order_side(order.order_side())?;
        let quantity = i32::try_from(Self::quantity_to_i64(order.quantity())?)?;
        let contract_id = Self::to_client_contract_id(
            &self.contract_id_from_instrument_id(order.instrument_id()),
        )?;
        let mut builder = PlaceOrder::builder(account_id, contract_id, order_type, side, quantity);

        if let Some(limit_price) = Self::price_to_decimal(order.price()) {
            builder = builder.limit_price(limit_price);
        }
        if let Some(stop_price) = Self::price_to_decimal(order.trigger_price()) {
            builder = builder.stop_price(stop_price);
        }

        builder
            .custom_tag(order.client_order_id().to_string())
            .build()
            .map_err(anyhow::Error::from)
    }

    fn to_client_account_id(value: i64) -> anyhow::Result<PxAccountId> {
        let value = i32::try_from(value)?;
        PxAccountId::new(value).map_err(anyhow::Error::from)
    }

    fn to_client_order_id(value: i64) -> anyhow::Result<projectx_client::OrderId> {
        projectx_client::OrderId::new(value).map_err(anyhow::Error::from)
    }

    fn to_client_contract_id(value: &str) -> anyhow::Result<ContractId> {
        ContractId::new(value).map_err(anyhow::Error::from)
    }

    fn parse_venue_order_id(order_id: VenueOrderId) -> anyhow::Result<i64> {
        order_id
            .as_str()
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("Invalid ProjectX venue order id '{order_id}'"))
    }

    fn ensure_response_account(
        expected_account_id: i64,
        actual_account_id: i64,
        entity: &str,
        entity_id: i64,
    ) -> anyhow::Result<()> {
        if actual_account_id != expected_account_id {
            anyhow::bail!(
                "ProjectX {entity} {entity_id} belongs to account {actual_account_id}, expected selected account {expected_account_id}"
            );
        }
        Ok(())
    }

    fn parse_ts(value: &ProjectXTimestamp) -> anyhow::Result<UnixNanos> {
        let nanos = u64::try_from(value.as_jiff().as_nanosecond()).map_err(|e| {
            anyhow::anyhow!("ProjectX timestamp {value} is outside the UnixNanos range: {e}")
        })?;
        Ok(UnixNanos::from(nanos))
    }

    fn projectx_timestamp_from_unix_nanos(
        value: UnixNanos,
        field: &str,
    ) -> anyhow::Result<ProjectXTimestamp> {
        let timestamp = jiff::Timestamp::from_nanosecond(i128::from(value.as_u64()))
            .map_err(|e| anyhow::anyhow!("Invalid ProjectX {field} timestamp {value}: {e}"))?;
        Ok(ProjectXTimestamp::from(timestamp))
    }

    fn try_parse_client_order_id(tag: &str) -> Option<ClientOrderId> {
        ClientOrderId::new_checked(tag).ok()
    }

    fn instrument_id_from_contract_id(
        contract_id: &str,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<InstrumentId> {
        let key = Self::normalize_projectx_symbol_key(contract_id);

        if let Some(aliases) = aliases
            && let Some(instrument_id) = aliases.get(&key)
        {
            return Ok(*instrument_id);
        }

        let symbol =
            projectx_to_databento_symbol(contract_id).unwrap_or_else(|_| contract_id.to_string());
        let symbol = Symbol::new_checked(symbol).map_err(|e| {
            anyhow::anyhow!("Invalid ProjectX contract identifier `{contract_id}`: {e}")
        })?;
        Ok(InstrumentId::new(symbol, *PROJECTX_VENUE))
    }

    fn cached_contract_id_from_instrument(instrument: &InstrumentAny) -> Option<&str> {
        match instrument {
            InstrumentAny::FuturesContract(inst) => inst
                .info
                .as_ref()
                .and_then(|info| info.get_str("projectx_contract_id")),
            _ => None,
        }
    }

    fn contract_id_from_instrument_id(&self, instrument_id: InstrumentId) -> String {
        if let Some(contract_id) = self
            .core
            .cache()
            .instrument(&instrument_id)
            .and_then(Self::cached_contract_id_from_instrument)
        {
            return contract_id.to_string();
        }

        databento_to_projectx_symbol(instrument_id.symbol.inner().as_str())
            .unwrap_or_else(|_| instrument_id.symbol.inner().to_string())
    }

    fn map_order_side(side: Side) -> anyhow::Result<OrderSide> {
        match side {
            Side::Bid => Ok(OrderSide::Buy),
            Side::Ask => Ok(OrderSide::Sell),
            Side::Unknown(code) => anyhow::bail!("Unknown ProjectX order side code: {code}"),
            _ => anyhow::bail!("Unsupported ProjectX order side: {side:?}"),
        }
    }

    fn map_order_type(order_type: ProjectXOrderType) -> anyhow::Result<OrderType> {
        match order_type {
            ProjectXOrderType::Limit => Ok(OrderType::Limit),
            ProjectXOrderType::Market => Ok(OrderType::Market),
            ProjectXOrderType::StopLimit => Ok(OrderType::StopLimit),
            ProjectXOrderType::Stop => Ok(OrderType::StopMarket),
            ProjectXOrderType::TrailingStop => Ok(OrderType::TrailingStopMarket),
            ProjectXOrderType::JoinBid | ProjectXOrderType::JoinAsk => {
                anyhow::bail!("ProjectX {order_type:?} orders have no exact Nautilus order type")
            }
            ProjectXOrderType::Unknown(code) => {
                anyhow::bail!("Unknown ProjectX order type code: {code}")
            }
            _ => anyhow::bail!("Unsupported ProjectX order type: {order_type:?}"),
        }
    }

    fn map_order_status(order: &Order) -> anyhow::Result<OrderStatus> {
        let filled = i64::from(order.fill_volume.unwrap_or(0));
        let size = i64::from(order.size);

        match order.status {
            ProjectXOrderStatus::Filled => Ok(OrderStatus::Filled),
            ProjectXOrderStatus::Cancelled => Ok(OrderStatus::Canceled),
            ProjectXOrderStatus::Expired => Ok(OrderStatus::Expired),
            ProjectXOrderStatus::Rejected => Ok(OrderStatus::Rejected),
            ProjectXOrderStatus::PendingCancellation => Ok(OrderStatus::PendingCancel),
            ProjectXOrderStatus::Open | ProjectXOrderStatus::Suspended => {
                if size > 0 && filled >= size {
                    Ok(OrderStatus::Filled)
                } else if filled > 0 {
                    Ok(OrderStatus::PartiallyFilled)
                } else {
                    Ok(OrderStatus::Accepted)
                }
            }
            ProjectXOrderStatus::Pending => {
                if size > 0 && filled >= size {
                    Ok(OrderStatus::Filled)
                } else if filled > 0 {
                    Ok(OrderStatus::PartiallyFilled)
                } else {
                    Ok(OrderStatus::Submitted)
                }
            }
            ProjectXOrderStatus::None => {
                anyhow::bail!("ProjectX order has no lifecycle status")
            }
            ProjectXOrderStatus::Unknown(code) => {
                anyhow::bail!("Unknown ProjectX order status code: {code}")
            }
            _ => anyhow::bail!("Unsupported ProjectX order status: {:?}", order.status),
        }
    }

    fn weighted_trade_avg_px_by_order_id(
        trades: &[Trade],
    ) -> anyhow::Result<AHashMap<i64, Decimal>> {
        let mut totals = AHashMap::<i64, (Decimal, i64)>::new();

        for trade in trades {
            if trade.voided || trade.size <= 0 {
                continue;
            }

            let order_id = trade.order_id.get();
            let entry = totals.entry(order_id).or_insert((Decimal::ZERO, 0));
            let notional = trade
                .price
                .checked_mul(Decimal::from(trade.size))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "ProjectX trade {} notional overflow for price={} size={}",
                        trade.id.get(),
                        trade.price,
                        trade.size
                    )
                })?;
            entry.0 = entry.0.checked_add(notional).ok_or_else(|| {
                anyhow::anyhow!("ProjectX order {order_id} aggregate trade notional overflow")
            })?;
            entry.1 = entry
                .1
                .checked_add(i64::from(trade.size))
                .ok_or_else(|| anyhow::anyhow!("ProjectX order {order_id} fill size overflow"))?;
        }

        let mut weighted = AHashMap::with_capacity(totals.len());

        for (order_id, (notional, qty)) in totals {
            if qty > 0 {
                let average = notional.checked_div(Decimal::from(qty)).ok_or_else(|| {
                    anyhow::anyhow!("ProjectX order {order_id} weighted price overflow")
                })?;
                weighted.insert(order_id, average);
            }
        }

        Ok(weighted)
    }

    fn reconciliation_start_timestamp() -> ProjectXTimestamp {
        let now = jiff::Timestamp::now();
        let start = now.checked_sub(jiff::Span::new().hours(24)).unwrap_or(now);
        ProjectXTimestamp::from(start)
    }

    fn lookback_start_unix_nanos(lookback_mins: u64) -> anyhow::Result<UnixNanos> {
        let lookback_ns = lookback_mins
            .checked_mul(60)
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .ok_or_else(|| {
                anyhow::anyhow!("ProjectX lookback {lookback_mins} minutes exceeds timestamp range")
            })?;
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        Ok(UnixNanos::from(now.saturating_sub(lookback_ns)))
    }

    fn backfill_order_filled_price(order: &mut Order, weighted_avg_px: &AHashMap<i64, Decimal>) {
        if order.filled_price.is_some() || order.fill_volume.unwrap_or(0) <= 0 {
            return;
        }

        order.filled_price = weighted_avg_px.get(&order.id.get()).copied();
    }

    fn backfill_orders_filled_prices(orders: &mut [Order], trades: &[Trade]) -> anyhow::Result<()> {
        let weighted_avg_px = Self::weighted_trade_avg_px_by_order_id(trades)?;

        for order in orders {
            Self::backfill_order_filled_price(order, &weighted_avg_px);
        }
        Ok(())
    }

    fn map_position_side(position: &Position) -> anyhow::Result<PositionSideSpecified> {
        if position.size < 0 {
            anyhow::bail!(
                "ProjectX position {} has invalid negative size {}",
                position.id,
                position.size
            );
        }
        if position.size == 0 {
            return Ok(PositionSideSpecified::Flat);
        }
        match position.position_type {
            projectx_client::PositionType::Long => Ok(PositionSideSpecified::Long),
            projectx_client::PositionType::Short => Ok(PositionSideSpecified::Short),
            projectx_client::PositionType::Undefined => {
                anyhow::bail!("Non-flat ProjectX position has undefined side")
            }
            projectx_client::PositionType::Unknown(code) => {
                anyhow::bail!("Unknown ProjectX position side code: {code}")
            }
            _ => anyhow::bail!(
                "Unsupported ProjectX position side: {:?}",
                position.position_type
            ),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn map_order_report_with_context(
        order: Order,
        account_id: AccountId,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        ts_init: UnixNanos,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<OrderStatusReport> {
        let ts_last = Self::parse_ts(&order.update_timestamp)?;
        let ts_accepted = Self::parse_ts(&order.creation_timestamp)?;
        let instrument_id =
            Self::instrument_id_from_contract_id(order.contract_id.as_ref(), aliases)?;
        let venue_order_id = VenueOrderId::new(order.id.to_string());
        let client_order_id = order
            .custom_tag
            .as_deref()
            .and_then(Self::try_parse_client_order_id)
            .or_else(|| venue_to_client.get(&order.id.get()).map(|value| *value));

        let mut report = OrderStatusReport::new(
            account_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            Self::map_order_side(order.side)?,
            Self::map_order_type(order.order_type)?,
            TimeInForce::Gtc,
            Self::map_order_status(&order)?,
            Quantity::from_decimal(Decimal::from(order.size))?,
            Quantity::from_decimal(Decimal::from(order.fill_volume.unwrap_or(0)))?,
            ts_accepted,
            ts_last,
            ts_init,
            None,
        );

        if let Some(limit_price) = order.limit_price {
            report = report.with_price(Price::from_decimal(limit_price)?);
        }

        if let Some(stop_price) = order.stop_price {
            report = report.with_trigger_price(Price::from_decimal(stop_price)?);
        }

        if let Some(avg_price) = order.filled_price {
            report = report.with_avg_px(avg_price);
        }

        if report.order_status == OrderStatus::Canceled {
            report.cancel_reason = Some("ProjectX canceled".to_string());
        }

        Ok(report)
    }

    fn map_user_order_report(
        order: &Order,
        meta: &ProjectXOrderMeta,
        account_id: AccountId,
        client_order_id: ClientOrderId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let ts_last = Self::parse_ts(&order.update_timestamp)?;
        let ts_accepted = Self::parse_ts(&order.creation_timestamp)?;
        let venue_order_id = VenueOrderId::new(order.id.to_string());
        let mut report = OrderStatusReport::new(
            account_id,
            meta.instrument_id,
            Some(client_order_id),
            venue_order_id,
            meta.order_side,
            meta.order_type,
            TimeInForce::Gtc,
            Self::map_order_status(order)?,
            Quantity::from_decimal(Decimal::from(order.size))?,
            Quantity::from_decimal(Decimal::from(order.fill_volume.unwrap_or(0)))?,
            ts_accepted,
            ts_last,
            ts_init,
            None,
        );

        if let Some(limit_price) = order.limit_price {
            report = report.with_price(Price::from_decimal(limit_price)?);
        }

        if let Some(stop_price) = order.stop_price {
            report = report.with_trigger_price(Price::from_decimal(stop_price)?);
        }

        if let Some(avg_price) = order.filled_price {
            report = report.with_avg_px(avg_price);
        }

        if report.order_status == OrderStatus::Canceled {
            report.cancel_reason = Some("ProjectX canceled".to_string());
        }

        Ok(report)
    }

    fn trade_commission(trade: &Trade, currency: Currency) -> anyhow::Result<Money> {
        let total = trade
            .fees
            .checked_add(trade.commissions.unwrap_or(Decimal::ZERO))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "ProjectX trade {} fee and commission total overflow",
                    trade.id.get()
                )
            })?;
        Money::from_decimal(total, currency).map_err(anyhow::Error::from)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn map_fill_report_with_context(
        trade: Trade,
        account_id: AccountId,
        base_currency: Option<Currency>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        ts_init: UnixNanos,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<FillReport> {
        let ts_event = Self::parse_ts(&trade.creation_timestamp)?;
        let instrument_id =
            Self::instrument_id_from_contract_id(trade.contract_id.as_ref(), aliases)?;
        let venue_order_id = VenueOrderId::new(trade.order_id.to_string());
        let client_order_id = venue_to_client
            .get(&trade.order_id.get())
            .map(|value| *value);
        let commission_currency = base_currency.unwrap_or_else(Currency::USD);
        let commission = Self::trade_commission(&trade, commission_currency)?;

        Ok(FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::new(format!("PX-{}", trade.id.get())),
            Self::map_order_side(trade.side)?,
            Quantity::from_decimal(Decimal::from(trade.size))?,
            Price::from_decimal(trade.price)?,
            commission,
            LiquiditySide::NoLiquiditySide,
            client_order_id,
            None,
            ts_event,
            ts_init,
            None,
        ))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn map_position_report_with_context(
        position: Position,
        account_id: AccountId,
        ts_init: UnixNanos,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<PositionStatusReport> {
        let ts_last = Self::parse_ts(&position.creation_timestamp)?;
        let avg_px_open = (position.size != 0).then_some(position.average_price);
        Ok(PositionStatusReport::new(
            account_id,
            Self::instrument_id_from_contract_id(position.contract_id.as_ref(), aliases)?,
            Self::map_position_side(&position)?,
            Quantity::from(position.size.unsigned_abs()),
            ts_last,
            ts_init,
            None,
            None,
            avg_px_open,
        ))
    }

    fn should_refresh_submit_snapshot(order_type: OrderType) -> bool {
        matches!(order_type, OrderType::Market | OrderType::MarketToLimit)
    }

    #[allow(clippy::too_many_arguments)]
    async fn refresh_submit_snapshot(
        http_client: &ProjectXHttpClient,
        account_id: i64,
        client_order_id: ClientOrderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        base_currency: Option<Currency>,
        emitter: &ExecutionEventEmitter,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        open_order_state: &DashMap<i64, OpenOrderState>,
        open_position_state: &DashMap<i64, PositionState>,
        aliases: &HashMap<String, InstrumentId>,
    ) -> anyhow::Result<()> {
        let delay_ms = 1_000_u64;
        tokio::time::sleep(StdDuration::from_millis(delay_ms)).await;

        Self::reconcile_account_snapshot_with_context(
            http_client,
            account_id,
            account_ids,
            account_issuer,
            base_currency,
            emitter,
            order_meta_by_client,
            venue_to_client,
            seen_trade_ids,
            voided_trade_ids,
            open_order_state,
            open_position_state,
            get_atomic_clock_realtime().get_time_ns(),
            Some(aliases),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "ProjectX post-submit snapshot refresh failed for account {account_id} order {client_order_id} after {delay_ms}ms: {e}"
            )
        })
    }

    fn handle_user_accounts_event(
        accounts: Vec<Account>,
        emitter: &ExecutionEventEmitter,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        account_type: AccountType,
        base_currency: Option<Currency>,
    ) -> anyhow::Result<()> {
        let currency = base_currency.unwrap_or_else(Currency::USD);

        for account in accounts {
            if !account.can_trade {
                anyhow::bail!(
                    "ProjectX account {} is no longer permitted to trade",
                    account.id
                );
            }
            if !account.is_visible {
                anyhow::bail!(
                    "ProjectX account {} is no longer visible to this login",
                    account.id
                );
            }
            let ts_now = get_atomic_clock_realtime().get_time_ns();
            let raw_balance = account.balance.ok_or_else(|| {
                anyhow::anyhow!(
                    "ProjectX account {} update omitted the account balance",
                    account.id
                )
            })?;
            let total = Money::from_decimal(raw_balance, currency)?;
            let locked = Money::from_decimal(Decimal::ZERO, currency)?;
            let free = total;
            let state = AccountState::new(
                Self::account_id_from_num_with_context(
                    i64::from(account.id.get()),
                    account_ids,
                    account_issuer,
                ),
                account_type,
                vec![AccountBalance::new(total, locked, free)],
                Vec::new(),
                true,
                UUID4::new(),
                ts_now,
                ts_now,
                base_currency,
            );
            emitter.send_account_state(state);
        }
        Ok(())
    }

    fn map_order_report(
        &self,
        order: Order,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let account_id = self.account_id_from_num(i64::from(order.account_id.get()));
        let cache = self.core.cache();
        let aliases = Self::instrument_aliases_from_cache(&cache);
        Self::map_order_report_with_context(
            order,
            account_id,
            &self.venue_to_client,
            ts_init,
            Some(&aliases),
        )
    }

    fn map_fill_report(&self, trade: Trade, ts_init: UnixNanos) -> anyhow::Result<FillReport> {
        let account_id = self.account_id_from_num(i64::from(trade.account_id.get()));
        let cache = self.core.cache();
        let aliases = Self::instrument_aliases_from_cache(&cache);
        Self::map_fill_report_with_context(
            trade,
            account_id,
            self.core.base_currency,
            &self.venue_to_client,
            ts_init,
            Some(&aliases),
        )
    }

    fn map_position_report(
        &self,
        position: Position,
        ts_init: UnixNanos,
    ) -> anyhow::Result<PositionStatusReport> {
        let account_id = self.account_id_from_num(i64::from(position.account_id.get()));
        let cache = self.core.cache();
        let aliases = Self::instrument_aliases_from_cache(&cache);
        Self::map_position_report_with_context(position, account_id, ts_init, Some(&aliases))
    }

    fn build_current_state_mass_status(
        &self,
        ts_now: UnixNanos,
        order_reports: Vec<OrderStatusReport>,
        fill_reports: Vec<FillReport>,
        position_reports: Vec<PositionStatusReport>,
    ) -> ExecutionMassStatus {
        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *PROJECTX_VENUE,
            ts_now,
            None,
        );
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);
        mass_status
    }

    fn remember_order_meta(&self, order: &OrderAny) {
        self.order_meta_by_client.insert(
            order.client_order_id(),
            ProjectXOrderMeta {
                strategy_id: order.strategy_id(),
                instrument_id: order.instrument_id(),
                order_side: order.order_side(),
                order_type: order.order_type(),
            },
        );
    }

    fn resolve_venue_order_id_from_cache(&self, cmd: &ModifyOrder) -> Option<VenueOrderId> {
        if cmd.venue_order_id.is_some() {
            return cmd.venue_order_id;
        }
        self.core
            .cache()
            .order(&cmd.client_order_id)
            .and_then(|o| o.venue_order_id())
    }

    fn resolve_venue_cancel_order_id_from_cache(&self, cmd: &CancelOrder) -> Option<VenueOrderId> {
        if cmd.venue_order_id.is_some() {
            return cmd.venue_order_id;
        }
        self.core
            .cache()
            .order(&cmd.client_order_id)
            .and_then(|o| o.venue_order_id())
    }

    fn command_readiness_error(&self) -> Option<&'static str> {
        if !self.core.is_connected() {
            Some("ProjectX execution client is not connected")
        } else if !self
            .ws_user
            .as_ref()
            .is_some_and(ProjectXWsClient::is_connected)
        {
            Some("ProjectX user stream is not connected; command rejected")
        } else if self.reconciliation_in_progress.load(Ordering::SeqCst) {
            Some("ProjectX reconciliation in progress; command rejected")
        } else if self.execution_stale.load(Ordering::SeqCst) {
            Some("ProjectX execution state is stale after reconnect; command rejected")
        } else {
            None
        }
    }

    fn account_id_from_num_with_context(
        account_id_num: i64,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
    ) -> AccountId {
        account_ids
            .read()
            .get(&account_id_num)
            .copied()
            .unwrap_or_else(|| AccountId::new(format!("{account_issuer}-{account_id_num}")))
    }

    fn account_id_from_num(&self, account_id_num: i64) -> AccountId {
        Self::account_id_from_num_with_context(
            account_id_num,
            &self.account_ids,
            self.core.account_id.get_issuer(),
        )
    }

    fn account_num_from_account_id(&self, account_id: AccountId) -> Option<i64> {
        self.account_ids
            .read()
            .iter()
            .find_map(|(key, value)| (*value == account_id).then_some(*key))
    }

    fn parse_account_num_from_account_id(account_id: AccountId) -> Option<i64> {
        account_id.get_issuers_id().parse::<i64>().ok()
    }

    fn account_state_context(&self) -> AccountStateRefreshContext {
        AccountStateRefreshContext {
            http_client: self.http_client.clone(),
            emitter: self.emitter.clone(),
            account_id_num: Arc::clone(&self.account_id_num),
            account_ids: Arc::clone(&self.account_ids),
            subscribed_account_ids: Arc::clone(&self.subscribed_account_ids),
            account_id: self.core.account_id,
            base_currency: self.core.base_currency,
            account_type: self.core.account_type,
            margin_support_warned: Arc::clone(&self.margin_support_warned),
        }
    }

    fn reconciliation_context(&self) -> ReconciliationContext {
        let instrument_aliases = {
            let cache = self.core.cache();
            Arc::new(Self::instrument_aliases_from_cache(&cache))
        };
        ReconciliationContext {
            http_client: self.http_client.clone(),
            ws_user: self.ws_user.clone(),
            emitter: self.emitter.clone(),
            account_ids: Arc::clone(&self.account_ids),
            subscribed_account_ids: Arc::clone(&self.subscribed_account_ids),
            account_issuer: self.core.account_id.get_issuer(),
            account_type: self.core.account_type,
            base_currency: self.core.base_currency,
            order_meta_by_client: Arc::clone(&self.order_meta_by_client),
            venue_to_client: Arc::clone(&self.venue_to_client),
            seen_trade_ids: Arc::clone(&self.seen_trade_ids),
            voided_trade_ids: Arc::clone(&self.voided_trade_ids),
            open_order_state: Arc::clone(&self.open_order_state),
            open_position_state: Arc::clone(&self.open_position_state),
            execution_stale: Arc::clone(&self.execution_stale),
            reconciliation_in_progress: Arc::clone(&self.reconciliation_in_progress),
            instrument_aliases,
        }
    }

    fn is_ambiguous_mutation_error(error: &ProjectXHttpError) -> bool {
        matches!(
            error,
            ProjectXHttpError::Client(projectx_client::Error::AmbiguousMutation { .. })
        )
    }

    fn bound_trade_dedup_state(seen_trade_ids: &DashSet<i64>, voided_trade_ids: &DashSet<i64>) {
        if seen_trade_ids.len() <= MAX_TRACKED_TRADE_IDS {
            return;
        }

        let mut ids = seen_trade_ids
            .iter()
            .map(|entry| *entry)
            .collect::<Vec<_>>();
        ids.sort_unstable();

        for trade_id in ids.into_iter().take(TRADE_ID_PRUNE_BATCH) {
            seen_trade_ids.remove(&trade_id);
            voided_trade_ids.remove(&trade_id);
        }
    }

    fn buffer_pending_trade(
        pending_trades_by_order_id: &DashMap<i64, Vec<Trade>>,
        trade: Trade,
    ) -> bool {
        let order_id = trade.order_id.get();

        if !pending_trades_by_order_id.contains_key(&order_id)
            && pending_trades_by_order_id.len() >= MAX_PENDING_TRADE_ORDERS
        {
            log::warn!(
                "ProjectX cannot buffer unmatched trade {} because the pending-order buffer is full; reconciliation is required",
                trade.id.get()
            );
            return false;
        }

        let mut pending = pending_trades_by_order_id.entry(order_id).or_default();
        if pending.len() >= MAX_PENDING_TRADES_PER_ORDER {
            log::warn!(
                "ProjectX cannot buffer unmatched trade {} because order {order_id} reached the per-order buffer limit; reconciliation is required",
                trade.id.get()
            );
            return false;
        }
        pending.push(trade);
        true
    }

    fn ensure_account_id_num_known(&self, account_id_num: i64) -> anyhow::Result<i64> {
        let subscribed_account_ids = self.subscribed_account_ids.read();
        if subscribed_account_ids.is_empty() || subscribed_account_ids.contains(&account_id_num) {
            return Ok(account_id_num);
        }

        let known = subscribed_account_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        anyhow::bail!(
            "ProjectX account {account_id_num} is not subscribed under current login (known: {known})"
        );
    }

    fn resolve_account_id_num_param(
        &self,
        params: Option<&nautilus_core::Params>,
    ) -> anyhow::Result<Option<i64>> {
        let Some(params) = params else {
            return Ok(None);
        };

        for key in ["account_id_num", "account_id"] {
            if let Some(account_id_num) = params.get_i64(key) {
                return Ok(Some(self.ensure_account_id_num_known(account_id_num)?));
            }
        }

        let account_raw = ["account_id_num", "account_id"]
            .into_iter()
            .find_map(|key| params.get_str(key));
        let Some(account_raw) = account_raw else {
            return Ok(None);
        };

        if let Ok(account_id_num) = account_raw.parse::<i64>() {
            return Ok(Some(self.ensure_account_id_num_known(account_id_num)?));
        }

        let account_id = projectx_account_id_from_raw(account_raw)?;

        if let Some(account_id_num) = self
            .account_num_from_account_id(account_id)
            .or_else(|| Self::parse_account_num_from_account_id(account_id))
        {
            return Ok(Some(self.ensure_account_id_num_known(account_id_num)?));
        }

        anyhow::bail!("Could not resolve ProjectX account override `{account_raw}`")
    }

    fn resolve_target_account_id_num(
        &self,
        params: Option<&nautilus_core::Params>,
        order_account_id: Option<AccountId>,
    ) -> anyhow::Result<i64> {
        if let Some(account_id) = order_account_id {
            let canonical = canonicalize_projectx_account_id(account_id)?;
            let account_id_num = self
                .account_num_from_account_id(canonical)
                .or_else(|| Self::parse_account_num_from_account_id(canonical))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "ProjectX order account {account_id} does not resolve to a subscribed provider account"
                    )
                })?;
            return self.ensure_account_id_num_known(account_id_num);
        }

        if let Some(account_id_num) = self.resolve_account_id_num_param(params)? {
            return Ok(account_id_num);
        }

        let subscribed_account_ids = self.subscribed_account_ids.read();

        if subscribed_account_ids.len() == 1
            && let Some(id) = subscribed_account_ids.iter().next().copied()
        {
            return Ok(id);
        }

        if let Some(account_id_num) = *self.account_id_num.lock().expect(MUTEX_POISONED) {
            return self.ensure_account_id_num_known(account_id_num);
        }

        anyhow::bail!(
            "ProjectX requires explicit account selection for multi-account login (`account_id_num` or `account_id`)"
        )
    }

    async fn fetch_reconciliation_snapshot_from_http(
        http_client: &ProjectXHttpClient,
        account_id: i64,
    ) -> anyhow::Result<ReconciliationSnapshot> {
        let expected_account_id = account_id;
        let account_id = Self::to_client_account_id(expected_account_id)?;
        let open_orders = http_client.search_open_orders(account_id).await?;
        let positions = http_client.search_open_positions(account_id).await?;
        let start_timestamp = Self::reconciliation_start_timestamp();
        let trades = http_client
            .search_trades(&TradeSearch::new(account_id, start_timestamp, None)?)
            .await?;
        for order in &open_orders {
            Self::ensure_response_account(
                expected_account_id,
                i64::from(order.account_id.get()),
                "order",
                order.id.get(),
            )?;
        }
        for position in &positions {
            Self::ensure_response_account(
                expected_account_id,
                i64::from(position.account_id.get()),
                "position",
                i64::from(position.id.get()),
            )?;
        }
        for trade in &trades {
            Self::ensure_response_account(
                expected_account_id,
                i64::from(trade.account_id.get()),
                "trade",
                trade.id.get(),
            )?;
        }

        Ok(ReconciliationSnapshot {
            open_orders,
            recent_trade_orders: Vec::new(),
            positions,
            trades,
        })
    }

    async fn fetch_recent_trade_orders_from_http(
        http_client: &ProjectXHttpClient,
        account_id: i64,
        trades: &[Trade],
        open_orders: &[Order],
    ) -> anyhow::Result<Vec<Order>> {
        let expected_account_id = account_id;
        let account_id = Self::to_client_account_id(expected_account_id)?;
        let open_order_ids: HashSet<i64> = open_orders.iter().map(|order| order.id.get()).collect();
        let trade_order_ids: HashSet<i64> = trades
            .iter()
            .filter(|trade| !trade.voided && trade.size > 0)
            .map(|trade| trade.order_id.get())
            .filter(|order_id| !open_order_ids.contains(order_id))
            .collect();

        if trade_order_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut orders = http_client
            .search_orders(&OrderSearch::new(
                account_id,
                Self::reconciliation_start_timestamp(),
                None,
            )?)
            .await?;
        for order in &orders {
            Self::ensure_response_account(
                expected_account_id,
                i64::from(order.account_id.get()),
                "order",
                order.id.get(),
            )?;
        }
        orders.retain(|order| trade_order_ids.contains(&order.id.get()));
        Ok(orders)
    }

    async fn fetch_runtime_reconciliation_snapshot_from_http(
        http_client: &ProjectXHttpClient,
        account_id: i64,
    ) -> anyhow::Result<ReconciliationSnapshot> {
        let mut snapshot =
            Self::fetch_reconciliation_snapshot_from_http(http_client, account_id).await?;

        snapshot.recent_trade_orders = Self::fetch_recent_trade_orders_from_http(
            http_client,
            account_id,
            &snapshot.trades,
            &snapshot.open_orders,
        )
        .await?;

        Ok(snapshot)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_reconciliation_snapshot_with_context(
        snapshot: ReconciliationSnapshot,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        base_currency: Option<Currency>,
        emitter: &ExecutionEventEmitter,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        open_order_state: &DashMap<i64, OpenOrderState>,
        open_position_state: &DashMap<i64, PositionState>,
        ts_init: UnixNanos,
        emit_reports: bool,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<()> {
        let ReconciliationSnapshot {
            open_orders,
            recent_trade_orders,
            positions,
            trades,
        } = snapshot;

        log::info!(
            "ProjectX reconciliation snapshot: open_orders={}, recent_trade_orders={}, open_positions={}, trades_24h={}",
            open_orders.len(),
            recent_trade_orders.len(),
            positions.len(),
            trades.len()
        );

        let trade_avg_px_by_order_id = Self::weighted_trade_avg_px_by_order_id(&trades)?;
        let trade_order_ids: HashSet<i64> = trades
            .iter()
            .filter(|trade| !trade.voided && trade.size > 0)
            .map(|trade| trade.order_id.get())
            .collect();
        let staged_venue_to_client = DashMap::new();
        for entry in venue_to_client {
            staged_venue_to_client.insert(*entry.key(), *entry.value());
        }
        let mut venue_mapping_updates = Vec::new();
        let mut order_reports = Vec::new();
        let mut open_order_updates = Vec::new();
        let mut order_ids = HashSet::new();

        for mut order in open_orders {
            let order_id = order.id.get();
            order_ids.insert(order_id);
            let new_state = OpenOrderState {
                status: order.status.code(),
                fill_volume: i64::from(order.fill_volume.unwrap_or(0)),
                filled_price: order.filled_price,
            };
            let client_order_id = order
                .custom_tag
                .as_deref()
                .and_then(Self::try_parse_client_order_id);
            if let Some(client_order_id) = client_order_id {
                staged_venue_to_client.insert(order_id, client_order_id);
                venue_mapping_updates.push((order_id, client_order_id));
            }
            Self::backfill_order_filled_price(&mut order, &trade_avg_px_by_order_id);
            let account_id = Self::account_id_from_num_with_context(
                i64::from(order.account_id.get()),
                account_ids,
                account_issuer,
            );
            let report = Self::map_order_report_with_context(
                order,
                account_id,
                &staged_venue_to_client,
                ts_init,
                aliases,
            )?;
            if open_order_state
                .get(&order_id)
                .is_none_or(|prev| *prev != new_state)
            {
                if emit_reports {
                    order_reports.push(report);
                }
                open_order_updates.push((order_id, new_state));
            }
        }

        for mut order in recent_trade_orders {
            let order_id = order.id.get();
            let existing_client_order_id =
                staged_venue_to_client.get(&order_id).map(|value| *value);
            let client_order_id = order
                .custom_tag
                .as_deref()
                .and_then(Self::try_parse_client_order_id)
                .or(existing_client_order_id);
            if let Some(client_order_id) = client_order_id {
                staged_venue_to_client.insert(order_id, client_order_id);
                venue_mapping_updates.push((order_id, client_order_id));
            }

            Self::backfill_order_filled_price(&mut order, &trade_avg_px_by_order_id);
            let account_id = Self::account_id_from_num_with_context(
                i64::from(order.account_id.get()),
                account_ids,
                account_issuer,
            );
            let report = Self::map_order_report_with_context(
                order,
                account_id,
                &staged_venue_to_client,
                ts_init,
                aliases,
            )?;

            // For known orders, let the fill report drive filled quantity reconciliation.
            // Emitting both a FILLED order status and a fill report causes duplicate fill
            // application once the engine infers from the status report first.
            if emit_reports
                && !(existing_client_order_id.is_some() && trade_order_ids.contains(&order_id))
            {
                order_reports.push(report);
            }
        }

        let stale_order_ids: Vec<i64> = open_order_state
            .iter()
            .filter_map(|entry| (!order_ids.contains(entry.key())).then_some(*entry.key()))
            .collect();

        let mut staged_seen_trade_ids =
            AHashSet::with_capacity(seen_trade_ids.len() + trades.len());
        for trade_id in seen_trade_ids.iter() {
            staged_seen_trade_ids.insert(*trade_id);
        }
        let mut staged_voided_trade_ids =
            AHashSet::with_capacity(voided_trade_ids.len() + trades.len());
        for trade_id in voided_trade_ids.iter() {
            staged_voided_trade_ids.insert(*trade_id);
        }
        let mut trade_state_updates = Vec::new();
        let mut fill_reports = Vec::new();
        let mut fill_void_events = Vec::new();

        for trade in trades {
            let trade_id = trade.id.get();
            let account_id = Self::account_id_from_num_with_context(
                i64::from(trade.account_id.get()),
                account_ids,
                account_issuer,
            );
            let fill_report = Self::map_fill_report_with_context(
                trade.clone(),
                account_id,
                base_currency,
                &staged_venue_to_client,
                ts_init,
                aliases,
            )?;

            if trade.voided {
                if staged_voided_trade_ids.contains(&trade_id) {
                    continue;
                }
                let had_fill = staged_seen_trade_ids.contains(&trade_id);
                if emit_reports && had_fill {
                    let event = Self::build_trade_void_event(
                        &trade,
                        emitter.trader_id(),
                        account_ids,
                        account_issuer,
                        order_meta_by_client,
                        &staged_venue_to_client,
                        base_currency,
                    )?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Could not route ProjectX voided trade {trade_id} to a known order"
                        )
                    })?;
                    fill_void_events.push(event);
                }
                staged_seen_trade_ids.insert(trade_id);
                staged_voided_trade_ids.insert(trade_id);
                trade_state_updates.push((trade_id, true));
                continue;
            }

            if staged_voided_trade_ids.contains(&trade_id)
                || staged_seen_trade_ids.contains(&trade_id)
            {
                continue;
            }
            if emit_reports {
                fill_reports.push(fill_report);
            }
            staged_seen_trade_ids.insert(trade_id);
            trade_state_updates.push((trade_id, false));
        }

        let mut position_ids = HashSet::new();
        let mut position_reports = Vec::new();
        let mut position_state_updates = Vec::new();

        for position in positions {
            let position_id = i64::from(position.id.get());
            position_ids.insert(position_id);
            let new_state = PositionState {
                size: i64::from(position.size),
                type_: position.position_type.code(),
                average_price: position.average_price,
            };
            let account_id = Self::account_id_from_num_with_context(
                i64::from(position.account_id.get()),
                account_ids,
                account_issuer,
            );
            let report =
                Self::map_position_report_with_context(position, account_id, ts_init, aliases)?;
            if open_position_state
                .get(&position_id)
                .is_none_or(|prev| *prev != new_state)
            {
                if emit_reports {
                    position_reports.push(report);
                }
                position_state_updates.push((position_id, new_state));
            }
        }
        let stale_position_ids: Vec<i64> = open_position_state
            .iter()
            .filter_map(|entry| (!position_ids.contains(entry.key())).then_some(*entry.key()))
            .collect();

        // Commit only after every provider entity has been validated and mapped.
        for (order_id, client_order_id) in venue_mapping_updates {
            venue_to_client.insert(order_id, client_order_id);
        }
        for report in order_reports {
            emitter.send_order_status_report(report);
        }
        for (order_id, state) in open_order_updates {
            open_order_state.insert(order_id, state);
        }
        for report in fill_reports {
            emitter.send_fill_report(report);
        }
        for event in fill_void_events {
            emitter.send_order_event(OrderEventAny::FillVoided(event));
        }
        for (trade_id, voided) in trade_state_updates {
            seen_trade_ids.insert(trade_id);
            if voided {
                voided_trade_ids.insert(trade_id);
            }
        }
        Self::bound_trade_dedup_state(seen_trade_ids, voided_trade_ids);
        for report in position_reports {
            emitter.send_position_report(report);
        }
        for (position_id, state) in position_state_updates {
            open_position_state.insert(position_id, state);
        }
        for order_id in stale_order_ids {
            open_order_state.remove(&order_id);
        }
        for position_id in stale_position_ids {
            open_position_state.remove(&position_id);
        }
        Ok(())
    }

    fn seed_reconciliation_snapshot_state(
        &self,
        snapshot: ReconciliationSnapshot,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        let cache = self.core.cache();
        let aliases = Self::instrument_aliases_from_cache(&cache);
        Self::apply_reconciliation_snapshot_with_context(
            snapshot,
            &self.account_ids,
            self.core.account_id.get_issuer(),
            self.core.base_currency,
            &self.emitter,
            &self.order_meta_by_client,
            &self.venue_to_client,
            &self.seen_trade_ids,
            &self.voided_trade_ids,
            &self.open_order_state,
            &self.open_position_state,
            ts_init,
            false,
            Some(&aliases),
        )
    }

    async fn reconcile_execution_state(&self, account_id: i64) -> anyhow::Result<()> {
        let snapshot =
            Self::fetch_reconciliation_snapshot_from_http(&self.http_client, account_id).await?;
        self.seed_reconciliation_snapshot_state(
            snapshot,
            get_atomic_clock_realtime().get_time_ns(),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn reconcile_account_snapshot_with_context(
        http_client: &ProjectXHttpClient,
        account_id: i64,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        base_currency: Option<Currency>,
        emitter: &ExecutionEventEmitter,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        open_order_state: &DashMap<i64, OpenOrderState>,
        open_position_state: &DashMap<i64, PositionState>,
        ts_init: UnixNanos,
        aliases: Option<&HashMap<String, InstrumentId>>,
    ) -> anyhow::Result<()> {
        let snapshot =
            Self::fetch_runtime_reconciliation_snapshot_from_http(http_client, account_id).await?;
        Self::apply_reconciliation_snapshot_with_context(
            snapshot,
            account_ids,
            account_issuer,
            base_currency,
            emitter,
            order_meta_by_client,
            venue_to_client,
            seen_trade_ids,
            voided_trade_ids,
            open_order_state,
            open_position_state,
            ts_init,
            true,
            aliases,
        )
    }

    async fn refresh_account_state_with_context(
        http_client: &ProjectXHttpClient,
        account_ids_num: &[i64],
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        account_type: AccountType,
        base_currency: Option<Currency>,
        emitter: &ExecutionEventEmitter,
    ) -> anyhow::Result<()> {
        if account_ids_num.is_empty() {
            return Ok(());
        }

        let subscribed_ids: HashSet<i64> = account_ids_num.iter().copied().collect();
        let accounts = http_client
            .search_accounts()
            .await?
            .into_iter()
            .filter(|account| subscribed_ids.contains(&i64::from(account.id.get())))
            .collect::<Vec<_>>();

        if accounts.is_empty() {
            anyhow::bail!(
                "Subscribed ProjectX account(s) {account_ids_num:?} were not returned by /api/Account/search"
            );
        }

        Self::handle_user_accounts_event(
            accounts,
            emitter,
            account_ids,
            account_issuer,
            account_type,
            base_currency,
        )?;
        Ok(())
    }

    async fn await_account_registered(&self, timeout_secs: f64) -> anyhow::Result<()> {
        let account_id = self.core.account_id;

        if self.core.cache().account(&account_id).is_some() {
            log::info!("ProjectX account {account_id} registered");
            return Ok(());
        }

        let start = Instant::now();
        let timeout = StdDuration::from_secs_f64(timeout_secs);
        let interval = StdDuration::from_millis(10);

        loop {
            tokio::time::sleep(interval).await;

            if self.core.cache().account(&account_id).is_some() {
                log::info!("ProjectX account {account_id} registered");
                return Ok(());
            }

            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "Timeout waiting for ProjectX account {account_id} to register after {timeout_secs}s"
                );
            }
        }
    }

    async fn fetch_order_status_reports(
        &self,
        open_only: bool,
        instrument_id: Option<InstrumentId>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let account_ids = self
            .account_state_context()
            .resolve_all_account_ids_num()
            .await?;
        let start_timestamp = start.map_or_else(
            || Ok(Self::reconciliation_start_timestamp()),
            |value| Self::projectx_timestamp_from_unix_nanos(value, "order-search start"),
        )?;
        let end_timestamp = end
            .map(|value| Self::projectx_timestamp_from_unix_nanos(value, "order-search end"))
            .transpose()?;
        let mut orders = Vec::new();
        let mut needs_trade_backfill = false;

        for &expected_account_id in &account_ids {
            let account_id = Self::to_client_account_id(expected_account_id)?;
            let mut account_orders = if open_only {
                self.http_client.search_open_orders(account_id).await?
            } else {
                self.http_client
                    .search_orders(&OrderSearch::new(
                        account_id,
                        start_timestamp,
                        end_timestamp,
                    )?)
                    .await?
            };
            for order in &account_orders {
                Self::ensure_response_account(
                    expected_account_id,
                    i64::from(order.account_id.get()),
                    "order",
                    order.id.get(),
                )?;
            }
            needs_trade_backfill |= account_orders
                .iter()
                .any(|order| order.fill_volume.unwrap_or(0) > 0 && order.filled_price.is_none());
            orders.append(&mut account_orders);
        }

        if needs_trade_backfill {
            let mut trades = Vec::new();

            for &expected_account_id in &account_ids {
                let account_id = Self::to_client_account_id(expected_account_id)?;
                let mut account_trades = self
                    .http_client
                    .search_trades(&TradeSearch::new(
                        account_id,
                        start_timestamp,
                        end_timestamp,
                    )?)
                    .await?;
                for trade in &account_trades {
                    Self::ensure_response_account(
                        expected_account_id,
                        i64::from(trade.account_id.get()),
                        "trade",
                        trade.id.get(),
                    )?;
                }
                trades.append(&mut account_trades);
            }

            Self::backfill_orders_filled_prices(&mut orders, &trades)?;
        }

        let reports = orders
            .into_iter()
            .map(|order| self.map_order_report(order, ts_init))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(reports
            .into_iter()
            .filter(|report| instrument_id.is_none_or(|id| report.instrument_id == id))
            .collect())
    }

    async fn fetch_fill_reports(
        &self,
        instrument_id: Option<InstrumentId>,
        venue_order_id: Option<VenueOrderId>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<FillReport>> {
        let account_ids = self
            .account_state_context()
            .resolve_all_account_ids_num()
            .await?;
        let start_timestamp = start.map_or_else(
            || Ok(Self::reconciliation_start_timestamp()),
            |value| Self::projectx_timestamp_from_unix_nanos(value, "trade-search start"),
        )?;
        let end_timestamp = end
            .map(|value| Self::projectx_timestamp_from_unix_nanos(value, "trade-search end"))
            .transpose()?;
        let mut trades = Vec::new();

        for expected_account_id in account_ids {
            let account_id = Self::to_client_account_id(expected_account_id)?;
            let mut account_trades = self
                .http_client
                .search_trades(&TradeSearch::new(
                    account_id,
                    start_timestamp,
                    end_timestamp,
                )?)
                .await?;
            for trade in &account_trades {
                Self::ensure_response_account(
                    expected_account_id,
                    i64::from(trade.account_id.get()),
                    "trade",
                    trade.id.get(),
                )?;
            }
            trades.append(&mut account_trades);
        }

        let reports = trades
            .into_iter()
            .filter(|trade| !trade.voided)
            .map(|trade| self.map_fill_report(trade, ts_init))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(reports
            .into_iter()
            .filter(|report| {
                if instrument_id.is_some_and(|id| report.instrument_id != id) {
                    return false;
                }

                if venue_order_id.is_some_and(|id| report.venue_order_id != id) {
                    return false;
                }
                true
            })
            .collect())
    }

    async fn fetch_position_status_reports(
        &self,
        instrument_id: Option<InstrumentId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let account_ids = self
            .account_state_context()
            .resolve_all_account_ids_num()
            .await?;
        let mut positions = Vec::new();

        for expected_account_id in account_ids {
            let account_id = Self::to_client_account_id(expected_account_id)?;
            let mut account_positions = self.http_client.search_open_positions(account_id).await?;
            for position in &account_positions {
                Self::ensure_response_account(
                    expected_account_id,
                    i64::from(position.account_id.get()),
                    "position",
                    i64::from(position.id.get()),
                )?;
            }
            positions.append(&mut account_positions);
        }

        let reports = positions
            .into_iter()
            .map(|position| self.map_position_report(position, ts_init))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(reports
            .into_iter()
            .filter(|report| instrument_id.is_none_or(|id| report.instrument_id == id))
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_trade_fill_event(
        trade: &Trade,
        emitter: &ExecutionEventEmitter,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        base_currency: Option<Currency>,
    ) -> anyhow::Result<bool> {
        let order_id = trade.order_id.get();
        let Some(client_order_id) = venue_to_client.get(&order_id).map(|v| *v) else {
            return Ok(false);
        };
        let Some(meta) = order_meta_by_client.get(&client_order_id).map(|v| *v) else {
            return Ok(false);
        };

        let trade_id_num = trade.id.get();

        if trade.voided {
            if voided_trade_ids.contains(&trade_id_num) {
                return Ok(true);
            }
            let had_fill = seen_trade_ids.contains(&trade_id_num);
            if had_fill
                && !Self::emit_trade_void_event(
                    trade,
                    emitter,
                    trader_id,
                    account_ids,
                    account_issuer,
                    order_meta_by_client,
                    venue_to_client,
                    base_currency,
                )?
            {
                return Ok(false);
            }
            if !had_fill {
                Quantity::from_decimal(Decimal::from(trade.size)).map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid ProjectX voided trade {trade_id_num} size {}: {e}",
                        trade.size
                    )
                })?;
                Price::from_decimal(trade.price).map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid ProjectX voided trade {trade_id_num} price {}: {e}",
                        trade.price
                    )
                })?;
                Self::trade_commission(trade, base_currency.unwrap_or_else(Currency::USD))?;
            }
            seen_trade_ids.insert(trade_id_num);
            voided_trade_ids.insert(trade_id_num);
            Self::bound_trade_dedup_state(seen_trade_ids, voided_trade_ids);
            return Ok(true);
        }

        if voided_trade_ids.contains(&trade_id_num) || seen_trade_ids.contains(&trade_id_num) {
            return Ok(true);
        }

        let now = get_atomic_clock_realtime().get_time_ns();
        let ts_event = Self::parse_ts(&trade.creation_timestamp)?;
        let account_id = Self::account_id_from_num_with_context(
            i64::from(trade.account_id.get()),
            account_ids,
            account_issuer,
        );
        let last_qty = Quantity::from_decimal(Decimal::from(trade.size)).map_err(|e| {
            anyhow::anyhow!(
                "Invalid ProjectX trade {trade_id_num} size {}: {e}",
                trade.size
            )
        })?;
        let last_px = Price::from_decimal(trade.price).map_err(|e| {
            anyhow::anyhow!(
                "Invalid ProjectX trade {trade_id_num} price {}: {e}",
                trade.price
            )
        })?;
        let trade_id = TradeId::new(format!("PX-{}", trade.id.get()));
        let currency = base_currency.unwrap_or_else(Currency::USD);
        let commission = Self::trade_commission(trade, currency)?;
        let event = OrderFilled::new(
            trader_id,
            meta.strategy_id,
            meta.instrument_id,
            client_order_id,
            VenueOrderId::new(trade.order_id.to_string()),
            account_id,
            trade_id,
            meta.order_side,
            meta.order_type,
            last_qty,
            last_px,
            currency,
            LiquiditySide::NoLiquiditySide,
            UUID4::new(),
            ts_event,
            now,
            false,
            None,
            Some(commission),
            None,
        );
        emitter.send_order_event(OrderEventAny::Filled(event));
        seen_trade_ids.insert(trade_id_num);
        Self::bound_trade_dedup_state(seen_trade_ids, voided_trade_ids);
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_trade_void_event(
        trade: &Trade,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        base_currency: Option<Currency>,
    ) -> anyhow::Result<Option<OrderFillVoided>> {
        let order_id = trade.order_id.get();
        let Some(client_order_id) = venue_to_client.get(&order_id).map(|value| *value) else {
            return Ok(None);
        };
        let Some(meta) = order_meta_by_client
            .get(&client_order_id)
            .map(|value| *value)
        else {
            return Ok(None);
        };
        let voided_qty = Quantity::from_decimal(Decimal::from(trade.size)).map_err(|e| {
            anyhow::anyhow!(
                "Invalid ProjectX trade void {} size {}: {e}",
                trade.id.get(),
                trade.size
            )
        })?;
        let last_px = Price::from_decimal(trade.price).map_err(|e| {
            anyhow::anyhow!(
                "Invalid ProjectX trade void {} price {}: {e}",
                trade.id.get(),
                trade.price
            )
        })?;

        let now = get_atomic_clock_realtime().get_time_ns();
        let ts_event = Self::parse_ts(&trade.creation_timestamp)?;
        let account_id = Self::account_id_from_num_with_context(
            i64::from(trade.account_id.get()),
            account_ids,
            account_issuer,
        );
        let currency = base_currency.unwrap_or_else(Currency::USD);
        let event = OrderFillVoided::new(
            trader_id,
            meta.strategy_id,
            meta.instrument_id,
            client_order_id,
            VenueOrderId::new(trade.order_id.to_string()),
            account_id,
            VenueOrderId::new(format!("PX-VOID-{}", trade.id.get())).inner(),
            TradeId::new(format!("PX-{}", trade.id.get())),
            voided_qty,
            Some(Self::trade_commission(trade, currency)?),
            meta.order_side,
            meta.order_type,
            last_px,
            currency,
            LiquiditySide::NoLiquiditySide,
            None,
            Some("ProjectX trade voided".into()),
            None,
            UUID4::new(),
            ts_event,
            now,
            false,
            false,
        );
        Ok(Some(event))
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_trade_void_event(
        trade: &Trade,
        emitter: &ExecutionEventEmitter,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        base_currency: Option<Currency>,
    ) -> anyhow::Result<bool> {
        let Some(event) = Self::build_trade_void_event(
            trade,
            trader_id,
            account_ids,
            account_issuer,
            order_meta_by_client,
            venue_to_client,
            base_currency,
        )?
        else {
            return Ok(false);
        };
        emitter.send_order_event(OrderEventAny::FillVoided(event));
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn flush_pending_trades_for_order(
        order_id: i64,
        emitter: &ExecutionEventEmitter,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        pending_trades_by_order_id: &DashMap<i64, Vec<Trade>>,
        base_currency: Option<Currency>,
    ) -> bool {
        let Some((_, pending_trades)) = pending_trades_by_order_id.remove(&order_id) else {
            return false;
        };

        let mut reconciliation_required = false;

        for trade in pending_trades {
            let is_voided = trade.voided;
            match Self::emit_trade_fill_event(
                &trade,
                emitter,
                trader_id,
                account_ids,
                account_issuer,
                order_meta_by_client,
                venue_to_client,
                seen_trade_ids,
                voided_trade_ids,
                base_currency,
            ) {
                Ok(true) => reconciliation_required |= is_voided,
                Ok(false) => {
                    reconciliation_required = true;
                    Self::buffer_pending_trade(pending_trades_by_order_id, trade);
                }
                Err(e) => {
                    reconciliation_required = true;
                    log::warn!("ProjectX pending trade conversion failed: {e}");
                }
            }
        }
        reconciliation_required
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_user_orders_event(
        orders: Vec<Order>,
        emitter: &ExecutionEventEmitter,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        last_order_state: &DashMap<ClientOrderId, OpenOrderState>,
        last_order_status: &DashMap<ClientOrderId, i32>,
        open_order_state: &DashMap<i64, OpenOrderState>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        pending_trades_by_order_id: &DashMap<i64, Vec<Trade>>,
        base_currency: Option<Currency>,
    ) -> HashSet<i64> {
        let now = get_atomic_clock_realtime().get_time_ns();
        let mut accounts_needing_snapshot = HashSet::new();

        for order in orders {
            let order_id = order.id.get();
            let account_id_num = i64::from(order.account_id.get());
            if let Err(e) = Self::map_order_status(&order) {
                log::warn!("Invalid ProjectX user-order status for order {order_id}: {e}");
                accounts_needing_snapshot.insert(account_id_num);
                continue;
            }
            let prev_state = open_order_state.get(&order_id).map(|entry| *entry);
            let new_state = OpenOrderState {
                status: order.status.code(),
                fill_volume: i64::from(order.fill_volume.unwrap_or(0)),
                filled_price: order.filled_price,
            };
            let fill_progressed =
                new_state.fill_volume > prev_state.map_or(0, |prev| prev.fill_volume);

            let account_id =
                Self::account_id_from_num_with_context(account_id_num, account_ids, account_issuer);
            let client_order_id = order
                .custom_tag
                .as_deref()
                .and_then(Self::try_parse_client_order_id)
                .or_else(|| venue_to_client.get(&order_id).map(|v| *v));

            let Some(client_order_id) = client_order_id else {
                log::debug!(
                    "ProjectX order event missing client mapping: venue_order_id={order_id}"
                );
                accounts_needing_snapshot.insert(account_id_num);
                continue;
            };

            if let Some(last) = last_order_state.get(&client_order_id)
                && *last == new_state
            {
                continue;
            }
            let Some(meta) = order_meta_by_client.get(&client_order_id).map(|v| *v) else {
                accounts_needing_snapshot.insert(account_id_num);
                continue;
            };

            let defer_fill_to_submit_snapshot =
                fill_progressed && Self::should_refresh_submit_snapshot(meta.order_type);
            let report = match Self::map_user_order_report(
                &order,
                &meta,
                account_id,
                client_order_id,
                now,
            ) {
                Ok(report) => report,
                Err(e) => {
                    log::warn!("Invalid ProjectX user-order report for order {order_id}: {e}");
                    accounts_needing_snapshot.insert(account_id_num);
                    continue;
                }
            };
            let ts_event = report.ts_last;

            venue_to_client.insert(order_id, client_order_id);

            if Self::flush_pending_trades_for_order(
                order_id,
                emitter,
                trader_id,
                account_ids,
                account_issuer,
                order_meta_by_client,
                venue_to_client,
                seen_trade_ids,
                voided_trade_ids,
                pending_trades_by_order_id,
                base_currency,
            ) {
                accounts_needing_snapshot.insert(account_id_num);
            }

            let venue_order_id = VenueOrderId::new(order.id.to_string());

            if !defer_fill_to_submit_snapshot && (fill_progressed || order.status.code() == 2) {
                emitter.send_order_status_report(report);
            }

            if !defer_fill_to_submit_snapshot
                && (fill_progressed || order.status.code() == 2)
                && order.filled_price.is_none()
            {
                accounts_needing_snapshot.insert(account_id_num);
            }

            let previous_status = last_order_status
                .get(&client_order_id)
                .map(|value| *value)
                .or_else(|| prev_state.map(|state| state.status));

            match order.status {
                ProjectXOrderStatus::Open if previous_status != Some(order.status.code()) => {
                    let event = OrderAccepted::new(
                        trader_id,
                        meta.strategy_id,
                        meta.instrument_id,
                        client_order_id,
                        venue_order_id,
                        account_id,
                        UUID4::new(),
                        ts_event,
                        now,
                        false,
                    );
                    emitter.send_order_event(OrderEventAny::Accepted(event));
                }
                ProjectXOrderStatus::Cancelled if previous_status != Some(order.status.code()) => {
                    let event = OrderCanceled::new(
                        trader_id,
                        meta.strategy_id,
                        meta.instrument_id,
                        client_order_id,
                        UUID4::new(),
                        ts_event,
                        now,
                        false,
                        Some(venue_order_id),
                        Some(account_id),
                    );
                    emitter.send_order_event(OrderEventAny::Canceled(event));
                }
                ProjectXOrderStatus::Expired if previous_status != Some(order.status.code()) => {
                    let event = OrderExpired::new(
                        trader_id,
                        meta.strategy_id,
                        meta.instrument_id,
                        client_order_id,
                        UUID4::new(),
                        ts_event,
                        now,
                        false,
                        Some(venue_order_id),
                        Some(account_id),
                    );
                    emitter.send_order_event(OrderEventAny::Expired(event));
                }
                ProjectXOrderStatus::Rejected if previous_status != Some(order.status.code()) => {
                    let event = OrderRejected::new(
                        trader_id,
                        meta.strategy_id,
                        meta.instrument_id,
                        client_order_id,
                        account_id,
                        "ProjectX venue rejected order".into(),
                        UUID4::new(),
                        ts_event,
                        now,
                        false,
                        false,
                    );
                    emitter.send_order_event(OrderEventAny::Rejected(event));
                }
                ProjectXOrderStatus::Unknown(code) => {
                    log::warn!("Ignoring ProjectX order event with unknown status code {code}");
                }
                _ => {}
            }

            last_order_state.insert(client_order_id, new_state);
            last_order_status.insert(client_order_id, order.status.code());
            open_order_state.insert(order_id, new_state);
        }

        accounts_needing_snapshot
    }

    fn handle_user_positions_event(
        positions: Vec<Position>,
        open_position_state: &DashMap<i64, PositionState>,
    ) -> HashSet<i64> {
        let mut accounts_needing_snapshot = HashSet::new();

        for position in positions {
            let position_id = i64::from(position.id.get());
            let new_state = PositionState {
                size: i64::from(position.size),
                type_: position.position_type.code(),
                average_price: position.average_price,
            };

            let changed = open_position_state
                .get(&position_id)
                .is_none_or(|prev| *prev != new_state);

            if changed {
                accounts_needing_snapshot.insert(i64::from(position.account_id.get()));
            }
        }

        accounts_needing_snapshot
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_user_trades_event(
        trades: Vec<Trade>,
        emitter: &ExecutionEventEmitter,
        trader_id: TraderId,
        account_ids: &AccountIdCache,
        account_issuer: Venue,
        order_meta_by_client: &DashMap<ClientOrderId, ProjectXOrderMeta>,
        venue_to_client: &DashMap<i64, ClientOrderId>,
        seen_trade_ids: &DashSet<i64>,
        voided_trade_ids: &DashSet<i64>,
        pending_trades_by_order_id: &DashMap<i64, Vec<Trade>>,
        base_currency: Option<Currency>,
    ) -> HashSet<i64> {
        let mut accounts_needing_snapshot = HashSet::new();
        for trade in trades {
            if !trade.voided && seen_trade_ids.contains(&trade.id.get()) {
                continue;
            }

            let account_id_num = i64::from(trade.account_id.get());
            let is_voided = trade.voided;
            match Self::emit_trade_fill_event(
                &trade,
                emitter,
                trader_id,
                account_ids,
                account_issuer,
                order_meta_by_client,
                venue_to_client,
                seen_trade_ids,
                voided_trade_ids,
                base_currency,
            ) {
                Ok(true) => {
                    if is_voided {
                        accounts_needing_snapshot.insert(account_id_num);
                    }
                }
                Ok(false) => {
                    // Buffer to cover the common trade-before-order race, but reconcile
                    // immediately so an external/unmapped fill cannot remain stranded.
                    Self::buffer_pending_trade(pending_trades_by_order_id, trade);
                    accounts_needing_snapshot.insert(account_id_num);
                }
                Err(e) => {
                    log::warn!("ProjectX live trade conversion failed: {e}");
                    accounts_needing_snapshot.insert(account_id_num);
                }
            }
        }
        accounts_needing_snapshot
    }

    fn spawn_ws_event_task(&mut self, ws_user: &ProjectXWsClient) {
        let ws_user = ws_user.clone();
        let reconciliation = self.reconciliation_context();
        let emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_ids = Arc::clone(&self.account_ids);
        let subscribed_account_ids = Arc::clone(&self.subscribed_account_ids);
        let account_issuer = self.core.account_id.get_issuer();
        let account_type = self.core.account_type;
        let base_currency = self.core.base_currency;
        let order_meta_by_client = Arc::clone(&self.order_meta_by_client);
        let venue_to_client = Arc::clone(&self.venue_to_client);
        let last_order_state = Arc::clone(&self.last_order_state);
        let last_order_status = Arc::clone(&self.last_order_status);
        let seen_trade_ids = Arc::clone(&self.seen_trade_ids);
        let voided_trade_ids = Arc::clone(&self.voided_trade_ids);
        let pending_trades_by_order_id = Arc::clone(&self.pending_trades_by_order_id);
        let open_order_state = Arc::clone(&self.open_order_state);
        let open_position_state = Arc::clone(&self.open_position_state);
        let execution_stale = Arc::clone(&self.execution_stale);
        let reconciliation_in_progress = Arc::clone(&self.reconciliation_in_progress);
        let handle = get_runtime().spawn(async move {
            let Some(mut rx) = ws_user.take_event_receiver().await else {
                execution_stale.store(true, Ordering::SeqCst);
                log::warn!("ProjectX user event receiver was already claimed");
                return;
            };

            while let Some(event) = rx.recv().await {
                match event {
                    ProjectXWsEvent::Disconnected => {
                        execution_stale.store(true, Ordering::SeqCst);
                        log::warn!("ProjectX user stream disconnected; execution is fenced");
                    }
                    ProjectXWsEvent::Reconnected => {
                        reconciliation.reconcile("user stream reconnect").await;
                    }
                    ProjectXWsEvent::ReconciliationRequired => {
                        // A transport gap is emitted while the realtime stream is still
                        // disconnected. Keep execution fenced; the Reconnected event starts
                        // reconciliation only after subscriptions have been replayed.
                        execution_stale.store(true, Ordering::SeqCst);
                        log::warn!("ProjectX user stream transport gap; execution is fenced");
                    }
                    ProjectXWsEvent::UserAccount(account) => {
                        let account_id_num = i64::from(account.id.get());
                        if !subscribed_account_ids.read().contains(&account_id_num) {
                            log::warn!(
                                "Ignoring ProjectX account update for unsubscribed account {account_id_num}"
                            );
                            continue;
                        }
                        if execution_stale.load(Ordering::SeqCst)
                            || reconciliation_in_progress.load(Ordering::SeqCst)
                        {
                            continue;
                        }

                        if let Err(e) = Self::handle_user_accounts_event(
                            vec![account],
                            &emitter,
                            &account_ids,
                            account_issuer,
                            account_type,
                            base_currency,
                        ) {
                            log::warn!("Invalid ProjectX account update: {e}");
                            reconciliation.reconcile("invalid account update").await;
                        }
                    }
                    ProjectXWsEvent::UserOrder(order) => {
                        let account_id_num = i64::from(order.account_id.get());
                        if !subscribed_account_ids.read().contains(&account_id_num) {
                            log::warn!(
                                "Ignoring ProjectX order update for unsubscribed account {account_id_num}"
                            );
                            continue;
                        }
                        if execution_stale.load(Ordering::SeqCst)
                            || reconciliation_in_progress.load(Ordering::SeqCst)
                        {
                            continue;
                        }

                        let accounts_needing_snapshot = Self::handle_user_orders_event(
                            vec![order],
                            &emitter,
                            trader_id,
                            &account_ids,
                            account_issuer,
                            &order_meta_by_client,
                            &venue_to_client,
                            &last_order_state,
                            &last_order_status,
                            &open_order_state,
                            &seen_trade_ids,
                            &voided_trade_ids,
                            &pending_trades_by_order_id,
                            base_currency,
                        );

                        if !accounts_needing_snapshot.is_empty() {
                            reconciliation.reconcile("live order update").await;
                        }
                    }
                    ProjectXWsEvent::UserPosition(position) => {
                        let account_id_num = i64::from(position.account_id.get());
                        if !subscribed_account_ids.read().contains(&account_id_num) {
                            log::warn!(
                                "Ignoring ProjectX position update for unsubscribed account {account_id_num}"
                            );
                            continue;
                        }
                        if execution_stale.load(Ordering::SeqCst)
                            || reconciliation_in_progress.load(Ordering::SeqCst)
                        {
                            continue;
                        }

                        let accounts_needing_snapshot = Self::handle_user_positions_event(
                            vec![position],
                            &open_position_state,
                        );

                        if !accounts_needing_snapshot.is_empty() {
                            reconciliation.reconcile("live position update").await;
                        }
                    }
                    ProjectXWsEvent::UserTrade(trade) => {
                        let account_id_num = i64::from(trade.account_id.get());
                        if !subscribed_account_ids.read().contains(&account_id_num) {
                            log::warn!(
                                "Ignoring ProjectX trade update for unsubscribed account {account_id_num}"
                            );
                            continue;
                        }
                        if execution_stale.load(Ordering::SeqCst)
                            || reconciliation_in_progress.load(Ordering::SeqCst)
                        {
                            continue;
                        }

                        let accounts_needing_snapshot = Self::handle_user_trades_event(
                            vec![trade],
                            &emitter,
                            trader_id,
                            &account_ids,
                            account_issuer,
                            &order_meta_by_client,
                            &venue_to_client,
                            &seen_trade_ids,
                            &voided_trade_ids,
                            &pending_trades_by_order_id,
                            base_currency,
                        );
                        if !accounts_needing_snapshot.is_empty() {
                            reconciliation.reconcile("live trade update").await;
                        }
                    }
                    _ => {}
                }
            }
            execution_stale.store(true, Ordering::SeqCst);
            reconciliation_in_progress.store(false, Ordering::SeqCst);
            log::warn!("ProjectX user event stream ended; execution is fenced");
        });
        self.ws_event_task = Some(handle);
    }
}

#[async_trait(?Send)]
impl ExecutionClient for ProjectXExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
            && self
                .ws_user
                .as_ref()
                .is_some_and(ProjectXWsClient::is_connected)
            && !self.execution_stale.load(Ordering::SeqCst)
            && !self.reconciliation_in_progress.load(Ordering::SeqCst)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *PROJECTX_VENUE
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
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        if let Some(task) = self.ws_event_task.take() {
            task.abort();
        }
        self.pending_tasks.abort_all();

        if let Some(ws_user) = self.ws_user.take() {
            let handle = get_runtime().spawn(async move {
                let _ = ws_user.disconnect().await;
            });
            self.pending_tasks.push(handle);
        }
        self.http_client.stop();
        self.execution_stale.store(true, Ordering::Release);
        self.reconciliation_in_progress
            .store(false, Ordering::Release);
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if self.core.is_connected() {
            anyhow::bail!(
                "ProjectX transport is connected but execution recovery is incomplete; commands remain fenced"
            );
        }

        self.execution_stale.store(true, Ordering::SeqCst);
        self.reconciliation_in_progress
            .store(true, Ordering::SeqCst);
        if let Err(e) = self.http_client.start().await {
            self.reconciliation_in_progress
                .store(false, Ordering::SeqCst);
            self.core.set_disconnected();
            return Err(e.into());
        }

        let ws_user = self.http_client.create_ws_client(ProjectXHub::User);
        let bootstrap_result = async {
            ws_user.connect().await?;
            let account_ids = self
                .account_state_context()
                .resolve_all_account_ids_num()
                .await?;

            ws_user
                .invoke("SubscribeAccounts", Vec::new(), true)
                .await?;

            for account_id in &account_ids {
                ws_user
                    .invoke("SubscribeOrders", vec![Value::from(*account_id)], true)
                    .await?;
                ws_user
                    .invoke("SubscribePositions", vec![Value::from(*account_id)], true)
                    .await?;
                ws_user
                    .invoke("SubscribeTrades", vec![Value::from(*account_id)], true)
                    .await?;
            }

            for account_id in account_ids {
                self.reconcile_execution_state(account_id).await?;
            }
            self.account_state_context().refresh().await?;
            self.await_account_registered(30.0).await
        }
        .await;

        if let Err(e) = bootstrap_result {
            if let Err(disconnect_error) = ws_user.disconnect().await {
                log::warn!(
                    "ProjectX failed to close user stream after connect rollback: {disconnect_error}"
                );
            }
            self.http_client.stop();
            self.execution_stale.store(true, Ordering::SeqCst);
            self.reconciliation_in_progress
                .store(false, Ordering::SeqCst);
            self.subscribed_account_ids.write().clear();
            self.account_ids.write().clear();
            *self.account_id_num.lock().expect(MUTEX_POISONED) = None;
            self.core.set_disconnected();
            return Err(e);
        }

        self.ws_user = Some(ws_user.clone());
        self.reconciliation_in_progress
            .store(false, Ordering::SeqCst);
        self.execution_stale.store(false, Ordering::SeqCst);
        self.core.set_connected();
        self.spawn_ws_event_task(&ws_user);
        log::info!("ProjectX execution client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.execution_stale.store(true, Ordering::SeqCst);
        self.core.set_disconnected();
        self.pending_tasks.abort_all();

        if let Some(task) = self.ws_event_task.take() {
            task.abort();
        }

        let disconnect_result = if let Some(ws_user) = self.ws_user.take() {
            ws_user.disconnect().await.map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        self.http_client.stop();
        self.reconciliation_in_progress
            .store(false, Ordering::SeqCst);
        self.subscribed_account_ids.write().clear();
        self.account_ids.write().clear();
        *self.account_id_num.lock().expect(MUTEX_POISONED) = None;
        log::info!("ProjectX execution client disconnected");
        disconnect_result
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = {
            let cache = self.core.cache();
            cache.order_owned(&cmd.client_order_id).ok_or_else(|| {
                anyhow::anyhow!("Order not found in cache: {}", cmd.client_order_id)
            })?
        };

        if let Some(reason) = self.command_readiness_error() {
            self.emitter.emit_order_denied(&order, reason);
            return Ok(());
        }
        if order.is_closed() {
            log::warn!(
                "Cannot submit closed ProjectX order {}",
                order.client_order_id()
            );
            return Ok(());
        }

        let position_account_id = cmd.position_id.and_then(|position_id| {
            self.core
                .cache()
                .position(&position_id)
                .map(|position| position.account_id)
        });
        let prepared = self
            .resolve_target_account_id_num(
                cmd.params.as_ref(),
                position_account_id.or_else(|| order.account_id()),
            )
            .and_then(|account_id_num| {
                self.build_place_order_request(&order, account_id_num)
                    .map(|request| (account_id_num, request))
            });
        let (account_id_num, req) = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };

        self.remember_order_meta(&order);
        self.emitter.emit_order_submitted(&order);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let should_refresh_snapshot = Self::should_refresh_submit_snapshot(order.order_type());
        let order_clone = order;
        let client_order_id = cmd.client_order_id;
        let clock = get_atomic_clock_realtime();
        let venue_to_client = Arc::clone(&self.venue_to_client);
        let last_order_status = Arc::clone(&self.last_order_status);
        let account_ids = Arc::clone(&self.account_ids);
        let order_meta_by_client = Arc::clone(&self.order_meta_by_client);
        let seen_trade_ids = Arc::clone(&self.seen_trade_ids);
        let voided_trade_ids = Arc::clone(&self.voided_trade_ids);
        let pending_trades_by_order_id = Arc::clone(&self.pending_trades_by_order_id);
        let open_order_state = Arc::clone(&self.open_order_state);
        let open_position_state = Arc::clone(&self.open_position_state);
        let account_issuer = self.core.account_id.get_issuer();
        let base_currency = self.core.base_currency;
        let reconciliation = self.reconciliation_context();
        let instrument_aliases = {
            let cache = self.core.cache();
            Self::instrument_aliases_from_cache(&cache)
        };

        let handle = get_runtime().spawn(async move {
            match http_client.place_order(&req).await {
                Ok(resp) => {
                    let order_id = resp.order_id.get();
                    venue_to_client.insert(order_id, client_order_id);
                    let should_emit_accepted = match last_order_status.entry(client_order_id) {
                        Entry::Vacant(entry) => {
                            entry.insert(ProjectXOrderStatus::Open.code());
                            true
                        }
                        Entry::Occupied(mut entry)
                            if *entry.get() == ProjectXOrderStatus::Pending.code() =>
                        {
                            entry.insert(ProjectXOrderStatus::Open.code());
                            true
                        }
                        Entry::Occupied(_) => false,
                    };
                    if should_emit_accepted {
                        emitter.emit_order_accepted(
                            &order_clone,
                            VenueOrderId::new(order_id.to_string()),
                            clock.get_time_ns(),
                        );
                    }
                    let mut reconciliation_required = Self::flush_pending_trades_for_order(
                        order_id,
                        &emitter,
                        order_clone.trader_id(),
                        &account_ids,
                        account_issuer,
                        &order_meta_by_client,
                        &venue_to_client,
                        &seen_trade_ids,
                        &voided_trade_ids,
                        &pending_trades_by_order_id,
                        base_currency,
                    );

                    if should_refresh_snapshot
                        && let Err(e) = Self::refresh_submit_snapshot(
                            &http_client,
                            account_id_num,
                            client_order_id,
                            &account_ids,
                            account_issuer,
                            base_currency,
                            &emitter,
                            &order_meta_by_client,
                            &venue_to_client,
                            &seen_trade_ids,
                            &voided_trade_ids,
                            &open_order_state,
                            &open_position_state,
                            &instrument_aliases,
                        )
                        .await
                    {
                        log::warn!("{e}");
                        reconciliation_required = true;
                    }

                    if reconciliation_required {
                        reconciliation
                            .reconcile("post-submit execution recovery")
                            .await;
                    }
                }
                Err(e) if Self::is_ambiguous_mutation_error(&e) => {
                    log::warn!(
                        "ProjectX submit for {client_order_id} has an ambiguous outcome: {e}; reconciling before admitting more commands"
                    );
                    reconciliation
                        .reconcile("ambiguous order submission")
                        .await;
                }
                Err(e) => {
                    emitter.emit_order_rejected(
                        &order_clone,
                        &format!("submit-order-error: {e}"),
                        clock.get_time_ns(),
                        false,
                    );
                }
            }
        });
        self.pending_tasks.push(handle);
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let cached_order = self.core.cache().order_owned(&cmd.client_order_id);

        if let Some(reason) = self.command_readiness_error() {
            if let Some(order) = cached_order.as_ref() {
                self.emitter.emit_order_modify_rejected(
                    order,
                    cmd.venue_order_id,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            } else {
                self.emitter.emit_order_modify_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    cmd.venue_order_id,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            }
            anyhow::bail!("{reason}");
        }

        let Some(venue_order_id) = self.resolve_venue_order_id_from_cache(&cmd) else {
            let reason = "Missing ProjectX venue_order_id for modify";

            if let Some(order) = cached_order.as_ref() {
                self.emitter.emit_order_modify_rejected(
                    order,
                    None,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            } else {
                self.emitter.emit_order_modify_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    None,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            }
            anyhow::bail!("{reason}");
        };

        let prepared = (|| -> anyhow::Result<(i64, PxModifyOrder)> {
            match cached_order.as_ref().map(|order| order.order_type()) {
                Some(OrderType::Limit) if cmd.trigger_price.is_some() => {
                    anyhow::bail!("ProjectX limit orders do not have a trigger price")
                }
                Some(OrderType::StopMarket) if cmd.price.is_some() => {
                    anyhow::bail!("ProjectX stop-market orders do not have a limit price")
                }
                Some(OrderType::Market) if cmd.price.is_some() || cmd.trigger_price.is_some() => {
                    anyhow::bail!("ProjectX market orders do not have price fields")
                }
                Some(OrderType::Limit | OrderType::Market | OrderType::StopMarket) => {}
                Some(order_type) => anyhow::bail!(
                    "ProjectX cannot preserve modification semantics for {order_type:?} orders"
                ),
                None if cmd.price.is_some() || cmd.trigger_price.is_some() => anyhow::bail!(
                    "ProjectX requires the cached order type to validate a price modification"
                ),
                None => {}
            }
            let account_id = Self::to_client_account_id(self.resolve_target_account_id_num(
                cmd.params.as_ref(),
                cached_order.as_ref().and_then(|order| order.account_id()),
            )?)?;
            let order_id_num = Self::parse_venue_order_id(venue_order_id)?;
            let order_id = Self::to_client_order_id(order_id_num)?;
            let mut builder = PxModifyOrder::builder(account_id, order_id);
            if let Some(size) = cmd.quantity.map(Self::quantity_to_i64).transpose()? {
                builder = builder.size(i32::try_from(size)?);
            }
            if let Some(limit_price) = Self::price_to_decimal(cmd.price) {
                builder = builder.limit_price(limit_price);
            }
            if let Some(stop_price) = Self::price_to_decimal(cmd.trigger_price) {
                builder = builder.stop_price(stop_price);
            }
            Ok((order_id_num, builder.build().map_err(anyhow::Error::from)?))
        })();
        let (order_id_num, req) = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                let reason = format!("Invalid ProjectX modify request: {e}");
                if let Some(order) = cached_order.as_ref() {
                    self.emitter.emit_order_modify_rejected(
                        order,
                        Some(venue_order_id),
                        &reason,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                } else {
                    self.emitter.emit_order_modify_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        &reason,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                return Ok(());
            }
        };
        self.venue_to_client
            .insert(order_id_num, cmd.client_order_id);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let command = cmd;
        let clock = get_atomic_clock_realtime();
        let cached_order = cached_order;
        let reconciliation = self.reconciliation_context();

        let handle = get_runtime().spawn(async move {
            match http_client.modify_order(&req).await {
                Ok(()) => {}
                Err(e) if Self::is_ambiguous_mutation_error(&e) => {
                    log::warn!(
                        "ProjectX modify for {} has an ambiguous outcome: {e}; reconciling before admitting more commands",
                        command.client_order_id
                    );
                    reconciliation
                        .reconcile("ambiguous order modification")
                        .await;
                }
                Err(e) => {
                    let reason = format!("modify-order-error: {e}");

                    if let Some(order) = cached_order.as_ref() {
                        emitter.emit_order_modify_rejected(
                            order,
                            command.venue_order_id,
                            &reason,
                            clock.get_time_ns(),
                        );
                    } else {
                        emitter.emit_order_modify_rejected_event(
                            command.strategy_id,
                            command.instrument_id,
                            command.client_order_id,
                            command.venue_order_id,
                            &reason,
                            clock.get_time_ns(),
                        );
                    }
                }
            }
        });
        self.pending_tasks.push(handle);

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cached_order = self.core.cache().order_owned(&cmd.client_order_id);

        if let Some(reason) = self.command_readiness_error() {
            if let Some(order) = cached_order.as_ref() {
                self.emitter.emit_order_cancel_rejected(
                    order,
                    cmd.venue_order_id,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            } else {
                self.emitter.emit_order_cancel_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    cmd.venue_order_id,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            }
            anyhow::bail!("{reason}");
        }

        let Some(venue_order_id) = self.resolve_venue_cancel_order_id_from_cache(&cmd) else {
            let reason = "Missing ProjectX venue_order_id for cancel";

            if let Some(order) = cached_order.as_ref() {
                self.emitter.emit_order_cancel_rejected(
                    order,
                    None,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            } else {
                self.emitter.emit_order_cancel_rejected_event(
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    None,
                    reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            }
            anyhow::bail!("{reason}");
        };

        let prepared = (|| -> anyhow::Result<(i64, PxCancelOrder)> {
            let account_id = Self::to_client_account_id(self.resolve_target_account_id_num(
                cmd.params.as_ref(),
                cached_order.as_ref().and_then(|order| order.account_id()),
            )?)?;
            let order_id_num = Self::parse_venue_order_id(venue_order_id)?;
            Ok((
                order_id_num,
                PxCancelOrder {
                    account_id,
                    order_id: Self::to_client_order_id(order_id_num)?,
                },
            ))
        })();
        let (order_id_num, req) = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                let reason = format!("Invalid ProjectX cancel request: {e}");
                if let Some(order) = cached_order.as_ref() {
                    self.emitter.emit_order_cancel_rejected(
                        order,
                        Some(venue_order_id),
                        &reason,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                } else {
                    self.emitter.emit_order_cancel_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        &reason,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                return Ok(());
            }
        };
        self.venue_to_client
            .insert(order_id_num, cmd.client_order_id);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let command = cmd;
        let clock = get_atomic_clock_realtime();
        let cached_order = cached_order;
        let reconciliation = self.reconciliation_context();
        let last_order_status = Arc::clone(&self.last_order_status);

        let handle = get_runtime().spawn(async move {
            match http_client.cancel_order(&req).await {
                Ok(()) => {
                    if let Some(order) = cached_order.as_ref() {
                        let should_emit_canceled =
                            match last_order_status.entry(command.client_order_id) {
                                Entry::Vacant(entry) => {
                                    entry.insert(ProjectXOrderStatus::Cancelled.code());
                                    true
                                }
                                Entry::Occupied(entry)
                                    if matches!(
                                        *entry.get(),
                                        status
                                            if status == ProjectXOrderStatus::Cancelled.code()
                                                || status == ProjectXOrderStatus::Filled.code()
                                                || status == ProjectXOrderStatus::Expired.code()
                                                || status == ProjectXOrderStatus::Rejected.code()
                                    ) =>
                                {
                                    false
                                }
                                Entry::Occupied(mut entry) => {
                                    entry.insert(ProjectXOrderStatus::Cancelled.code());
                                    true
                                }
                            };
                        if should_emit_canceled {
                            emitter.emit_order_canceled(
                                order,
                                Some(venue_order_id),
                                clock.get_time_ns(),
                            );
                        }
                    }
                }
                Err(e) if Self::is_ambiguous_mutation_error(&e) => {
                    log::warn!(
                        "ProjectX cancel for {} has an ambiguous outcome: {e}; reconciling before admitting more commands",
                        command.client_order_id
                    );
                    reconciliation
                        .reconcile("ambiguous order cancellation")
                        .await;
                }
                Err(e) => {
                    let reason = format!("cancel-order-error: {e}");

                    if let Some(order) = cached_order.as_ref() {
                        emitter.emit_order_cancel_rejected(
                            order,
                            command.venue_order_id,
                            &reason,
                            clock.get_time_ns(),
                        );
                    } else {
                        emitter.emit_order_cancel_rejected_event(
                            command.strategy_id,
                            command.instrument_id,
                            command.client_order_id,
                            command.venue_order_id,
                            &reason,
                            clock.get_time_ns(),
                        );
                    }
                }
            }
        });
        self.pending_tasks.push(handle);
        Ok(())
    }

    fn query_account(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        let account_id = canonicalize_projectx_account_id(cmd.account_id)?;
        let account_id_num = self
            .account_num_from_account_id(account_id)
            .or_else(|| Self::parse_account_num_from_account_id(account_id))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "ProjectX query account {} does not resolve to the selected subscription",
                    cmd.account_id
                )
            })?;
        self.ensure_account_id_num_known(account_id_num)?;
        *self.account_id_num.lock().expect(MUTEX_POISONED) = Some(account_id_num);
        let context = self.account_state_context();
        let reconciliation = self.reconciliation_context();
        let handle = get_runtime().spawn(async move {
            if let Err(e) = context.refresh().await {
                log::warn!("ProjectX query_account refresh failed: {e:?}");
                reconciliation
                    .reconcile("query account refresh failure")
                    .await;
            }
        });
        self.pending_tasks.push(handle);
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        if let Some(reason) = self.command_readiness_error() {
            anyhow::bail!("{reason}");
        }

        let side = (cmd.order_side != OrderSide::NoOrderSide).then_some(cmd.order_side);
        let cache = self.core.cache();
        let orders = cache
            .orders_open(
                None,
                Some(&cmd.instrument_id),
                Some(&cmd.strategy_id),
                None,
                side,
            )
            .into_iter()
            .chain(cache.orders_inflight(
                None,
                Some(&cmd.instrument_id),
                Some(&cmd.strategy_id),
                None,
                side,
            ))
            .map(|order| order.cloned())
            .collect::<Vec<_>>();

        if orders.is_empty() {
            return Ok(());
        }

        for order in orders {
            if order.venue_order_id().is_none() {
                log::debug!(
                    "Skipping ProjectX cancel_all_orders for {} without venue_order_id",
                    order.client_order_id()
                );
                continue;
            }

            let cancel = CancelOrder::new(
                cmd.trader_id,
                cmd.client_id,
                cmd.strategy_id,
                order.instrument_id(),
                order.client_order_id(),
                order.venue_order_id(),
                UUID4::new(),
                get_atomic_clock_realtime().get_time_ns(),
                cmd.params.clone(),
                None,
            );
            self.cancel_order(cancel)?;
        }

        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let (mut reports, historical_reports) = tokio::try_join!(
            self.fetch_order_status_reports(true, cmd.instrument_id, None, None, cmd.ts_init),
            self.fetch_order_status_reports(false, cmd.instrument_id, None, None, cmd.ts_init),
        )?;
        let open_order_ids = reports
            .iter()
            .map(|report| report.venue_order_id)
            .collect::<HashSet<_>>();
        reports.extend(
            historical_reports
                .into_iter()
                .filter(|report| !open_order_ids.contains(&report.venue_order_id)),
        );

        let found = reports.into_iter().find(|report| {
            if cmd
                .venue_order_id
                .is_some_and(|venue_order_id| report.venue_order_id != venue_order_id)
            {
                return false;
            }

            if cmd
                .client_order_id
                .is_some_and(|client_order_id| report.client_order_id != Some(client_order_id))
            {
                return false;
            }

            if cmd
                .instrument_id
                .is_some_and(|instrument_id| report.instrument_id != instrument_id)
            {
                return false;
            }
            true
        });

        Ok(found)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.fetch_order_status_reports(
            cmd.open_only,
            cmd.instrument_id,
            cmd.start,
            cmd.end,
            cmd.ts_init,
        )
        .await
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.fetch_fill_reports(
            cmd.instrument_id,
            cmd.venue_order_id,
            cmd.start,
            cmd.end,
            cmd.ts_init,
        )
        .await
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        if cmd.start.is_some() || cmd.end.is_some() {
            log::debug!("ProjectX position reports ignore start/end filters (open snapshot only)");
        }
        self.fetch_position_status_reports(cmd.instrument_id, cmd.ts_init)
            .await
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_now = get_atomic_clock_realtime().get_time_ns();
        let start = Self::lookback_start_unix_nanos(lookback_mins.unwrap_or(24 * 60))?;

        let (open_order_reports, historical_order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.fetch_order_status_reports(true, None, None, None, ts_now),
            self.fetch_order_status_reports(false, None, Some(start), None, ts_now),
            self.fetch_fill_reports(None, None, Some(start), None, ts_now),
            self.fetch_position_status_reports(None, ts_now),
        )?;

        let mut orders_by_venue_id =
            HashMap::with_capacity(open_order_reports.len() + historical_order_reports.len());
        for report in historical_order_reports {
            orders_by_venue_id.insert(report.venue_order_id, report);
        }
        for report in open_order_reports {
            orders_by_venue_id.insert(report.venue_order_id, report);
        }

        Ok(Some(self.build_current_state_mass_status(
            ts_now,
            orders_by_venue_id.into_values().collect(),
            fill_reports,
            position_reports,
        )))
    }
}

#[cfg(test)]
impl Default for ProjectXExecutionClient {
    fn default() -> Self {
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("PROJECTX"),
            *PROJECTX_VENUE,
            OmsType::Netting,
            AccountId::from("PROJECTX-001"),
            AccountType::Margin,
            None,
            std::rc::Rc::new(std::cell::RefCell::new(Cache::default())),
        );
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PROJECTX-001"),
            crate::common::enums::ProjectXEnvironment::TopstepX,
            "test-user",
            "test-key",
        )
        .expect("test ProjectX config should be valid");
        Self::new(core, config)
            .expect("default ProjectXExecutionClient construction should succeed")
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashSet, fs, rc::Rc, sync::Arc};

    use ahash::AHashMap;
    use dashmap::{DashMap, DashSet};
    use nautilus_common::{
        cache::Cache,
        clients::ExecutionClient,
        messages::{ExecutionEvent, ExecutionReport, execution::CancelAllOrders},
    };
    use nautilus_core::{Params, UUID4, UnixNanos};
    use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
    use nautilus_model::{
        enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, PositionSideSpecified},
        events::OrderEventAny,
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue,
        },
        instruments::Instrument,
        types::Currency,
    };
    use parking_lot::RwLock as ParkingRwLock;
    use serde_json::{Value, json};

    use super::ProjectXExecutionClient;
    use crate::{
        common::consts::PROJECTX_VENUE, config::ProjectXExecClientConfig,
        factories::projectx_contract_to_instrument,
    };
    use projectx_client::{Account, Order, Position, Trade};
    use rust_decimal::{Decimal, prelude::ToPrimitive};

    fn load_fixture(name: &str) -> Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join(name);
        let bytes =
            fs::read(&path).unwrap_or_else(|e| panic!("failed reading fixture {path:?}: {e}"));
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("failed parsing fixture {path:?}: {e}"))
    }

    fn test_emitter() -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let mut emitter = ExecutionEventEmitter::new(
            nautilus_core::time::get_atomic_clock_realtime(),
            TraderId::from("TRADER-001"),
            AccountId::from("PROJECTX-001"),
            AccountType::Margin,
            None,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(tx);
        (emitter, rx)
    }

    fn count_reports(rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>) -> usize {
        let mut count = 0;

        while let Ok(event) = rx.try_recv() {
            if matches!(event, ExecutionEvent::Report(_)) {
                count += 1;
            }
        }
        count
    }

    fn collect_reports(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) -> Vec<ExecutionReport> {
        let mut reports = Vec::new();

        while let Ok(event) = rx.try_recv() {
            if let ExecutionEvent::Report(report) = event {
                reports.push(report);
            }
        }

        reports
    }

    fn count_filled_order_events(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) -> usize {
        let mut count = 0;

        while let Ok(event) = rx.try_recv() {
            if matches!(event, ExecutionEvent::Order(OrderEventAny::Filled(_))) {
                count += 1;
            }
        }
        count
    }

    fn count_account_events(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) -> usize {
        let mut count = 0;

        while let Ok(event) = rx.try_recv() {
            if matches!(event, ExecutionEvent::Account(_)) {
                count += 1;
            }
        }
        count
    }

    fn test_client_with_cache() -> (ProjectXExecutionClient, Rc<RefCell<Cache>>) {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("PROJECTX"),
            *PROJECTX_VENUE,
            OmsType::Netting,
            AccountId::from("PROJECTX-001"),
            AccountType::Margin,
            None,
            Rc::clone(&cache),
        );
        let config = ProjectXExecClientConfig::new(
            TraderId::from("TRADER-001"),
            AccountId::from("PROJECTX-001"),
            crate::common::enums::ProjectXEnvironment::TopstepX,
            "test-user",
            "test-key",
        )
        .expect("test ProjectX config should be valid");
        let client = ProjectXExecutionClient::new(core, config)
            .expect("test ProjectXExecutionClient construction should succeed");
        (client, cache)
    }

    fn base_order(status: i32, fill_volume: Option<i64>, size: i64) -> Order {
        let mut value = serde_json::json!({
            "id": 1,
            "accountId": 1,
            "contractId": "MESM6",
            "symbolId": "MES",
            "creationTimestamp": "2026-04-02T00:00:00Z",
            "updateTimestamp": "2026-04-02T00:00:01Z",
            "status": status,
            "type": 1,
            "side": 0,
            "size": size,
            "limitPrice": 5200.25,
        });
        if let Some(fill_volume) = fill_volume {
            value["fillVolume"] = serde_json::json!(fill_volume);
        }
        serde_json::from_value(value).expect("order")
    }

    fn base_trade(order_id: i64, id: i64, price: f64, size: i64) -> Trade {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "accountId": 1,
            "contractId": "MESM6",
            "creationTimestamp": "2026-04-02T00:00:01Z",
            "price": price,
            "profitAndLoss": 0.0,
            "fees": 0.0,
            "side": 0,
            "size": size,
            "voided": false,
            "orderId": order_id,
        }))
        .expect("trade")
    }

    fn base_position(type_: i32, size: i64, average_price: f64) -> Position {
        serde_json::from_value(serde_json::json!({
            "id": 1,
            "accountId": 1,
            "contractId": "MESM6",
            "creationTimestamp": "2026-04-02T00:00:01Z",
            "type": type_,
            "size": size,
            "averagePrice": average_price,
        }))
        .expect("position")
    }

    #[rstest::rstest]
    fn map_order_status_prefers_fill_volume() {
        let partial = base_order(1, Some(1), 2);
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&partial).unwrap(),
            OrderStatus::PartiallyFilled
        );

        let filled = base_order(1, Some(2), 2);
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&filled).unwrap(),
            OrderStatus::Filled
        );
    }

    #[rstest::rstest]
    fn map_order_status_uses_projectx_status_codes() {
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&base_order(6, None, 2)).unwrap(),
            OrderStatus::Submitted
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&base_order(3, None, 2)).unwrap(),
            OrderStatus::Canceled
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&base_order(5, None, 2)).unwrap(),
            OrderStatus::Rejected
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&base_order(4, None, 2)).unwrap(),
            OrderStatus::Expired
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_status(&base_order(7, None, 2)).unwrap(),
            OrderStatus::PendingCancel
        );
    }

    #[rstest::rstest]
    fn backfill_order_filled_price_uses_weighted_trade_average() {
        let mut order = base_order(1, Some(3), 3);
        let weighted = ProjectXExecutionClient::weighted_trade_avg_px_by_order_id(&[
            base_trade(order.id.get(), 10, 5200.0, 1),
            base_trade(order.id.get(), 11, 5202.0, 2),
        ])
        .expect("valid weighted average");

        ProjectXExecutionClient::backfill_order_filled_price(&mut order, &weighted);

        assert_eq!(
            order.filled_price.map(|v| v.to_f64().unwrap_or_default()),
            Some(5201.333333333333)
        );
    }

    #[rstest::rstest]
    fn backfill_order_filled_price_ignores_voided_trades() {
        let mut order = base_order(1, Some(2), 2);
        let mut voided = base_trade(order.id.get(), 10, 9999.0, 1);
        voided.voided = true;
        let weighted = ProjectXExecutionClient::weighted_trade_avg_px_by_order_id(&[
            voided,
            base_trade(order.id.get(), 11, 5201.0, 2),
        ])
        .expect("valid weighted average");

        ProjectXExecutionClient::backfill_order_filled_price(&mut order, &weighted);

        assert_eq!(
            order.filled_price.map(|v| v.to_f64().unwrap_or_default()),
            Some(5201.0)
        );
    }

    #[rstest::rstest]
    fn backfill_orders_filled_prices_updates_each_matching_order() {
        let mut order_a = base_order(1, Some(1), 1);
        order_a.id = projectx_client::OrderId::new(10).unwrap();
        let mut order_b = base_order(1, Some(2), 2);
        order_b.id = projectx_client::OrderId::new(20).unwrap();
        let trades = vec![
            base_trade(order_a.id.get(), 100, 5200.0, 1),
            base_trade(order_b.id.get(), 200, 5201.0, 1),
            base_trade(order_b.id.get(), 201, 5203.0, 1),
        ];
        let mut orders = vec![order_a, order_b];

        ProjectXExecutionClient::backfill_orders_filled_prices(&mut orders, &trades)
            .expect("valid fill price backfill");

        assert_eq!(
            orders[0]
                .filled_price
                .map(|v| v.to_f64().unwrap_or_default()),
            Some(5200.0)
        );
        assert_eq!(
            orders[1]
                .filled_price
                .map(|v| v.to_f64().unwrap_or_default()),
            Some(5202.0)
        );
    }

    #[rstest::rstest]
    fn map_side_type_and_position_side() {
        assert_eq!(
            ProjectXExecutionClient::map_order_side(projectx_client::Side::Bid).unwrap(),
            OrderSide::Buy
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_side(projectx_client::Side::Ask).unwrap(),
            OrderSide::Sell
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_type(projectx_client::OrderType::Limit).unwrap(),
            OrderType::Limit
        );
        assert_eq!(
            ProjectXExecutionClient::map_order_type(projectx_client::OrderType::TrailingStop)
                .unwrap(),
            OrderType::TrailingStopMarket
        );

        let mut position = base_position(1, 2, 5200.25);
        assert_eq!(
            ProjectXExecutionClient::map_position_side(&position).unwrap(),
            PositionSideSpecified::Long
        );
        position.position_type = projectx_client::PositionType::Short;
        assert_eq!(
            ProjectXExecutionClient::map_position_side(&position).unwrap(),
            PositionSideSpecified::Short
        );
        position.size = 0;
        assert_eq!(
            ProjectXExecutionClient::map_position_side(&position).unwrap(),
            PositionSideSpecified::Flat
        );
    }

    #[rstest::rstest]
    fn reconciliation_snapshot_emits_monotonic_diffs_and_prunes_state() {
        let (emitter, mut rx) = test_emitter();
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let open_order_state = DashMap::new();
        let open_position_state = DashMap::new();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));

        let order = base_order(1, Some(1), 2);
        let position = base_position(1, 1, 5200.25);
        let trade = base_trade(order.id.get(), 20, 5200.5, 1);

        let snapshot = super::ReconciliationSnapshot {
            open_orders: vec![order.clone()],
            recent_trade_orders: vec![],
            positions: vec![position.clone()],
            trades: vec![trade.clone()],
        };
        let ts = UnixNanos::from(1_000);
        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            snapshot,
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            ts,
            true,
            None,
        )
        .expect("valid reconciliation snapshot");
        assert_eq!(count_reports(&mut rx), 3);

        let same_snapshot = super::ReconciliationSnapshot {
            open_orders: vec![order.clone()],
            recent_trade_orders: vec![],
            positions: vec![position.clone()],
            trades: vec![trade.clone()],
        };
        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            same_snapshot,
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            ts,
            true,
            None,
        )
        .expect("valid reconciliation snapshot");
        assert_eq!(count_reports(&mut rx), 0);

        let mut changed_order = order.clone();
        changed_order.status = projectx_client::OrderStatus::Filled;
        changed_order.fill_volume = Some(2);
        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![changed_order],
                recent_trade_orders: vec![],
                positions: vec![position.clone()],
                trades: vec![trade.clone()],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            ts,
            true,
            None,
        )
        .expect("valid reconciliation snapshot");
        assert_eq!(count_reports(&mut rx), 1);

        // Prune tracked open-order/open-position cache entries.
        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![],
                recent_trade_orders: vec![],
                positions: vec![],
                trades: vec![],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            ts,
            true,
            None,
        )
        .expect("valid reconciliation snapshot");
        assert_eq!(open_order_state.len(), 0);
        assert_eq!(open_position_state.len(), 0);

        // Reintroduced order/position should emit again after prune.
        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![order],
                recent_trade_orders: vec![],
                positions: vec![position],
                trades: vec![trade], // same trade id remains deduped by seen_trade_ids
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            ts,
            true,
            None,
        )
        .expect("valid reconciliation snapshot");
        assert_eq!(count_reports(&mut rx), 2);
    }

    #[rstest::rstest]
    fn reconciliation_snapshot_can_seed_state_without_emitting_reports() {
        let (emitter, mut rx) = test_emitter();
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let open_order_state = DashMap::new();
        let open_position_state = DashMap::new();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));

        let order = base_order(1, Some(1), 2);
        let position = base_position(1, 1, 5200.25);
        let trade = base_trade(order.id.get(), 20, 5200.5, 1);

        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![order.clone()],
                recent_trade_orders: vec![],
                positions: vec![position],
                trades: vec![trade.clone()],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            UnixNanos::from(1_000),
            false,
            None,
        )
        .expect("valid reconciliation snapshot");

        assert_eq!(count_reports(&mut rx), 0);
        assert_eq!(open_order_state.len(), 1);
        assert_eq!(open_position_state.len(), 1);
        assert!(seen_trade_ids.contains(&trade.id.get()));
        assert_eq!(
            venue_to_client.get(&order.id.get()).map(|entry| *entry),
            order
                .custom_tag
                .as_deref()
                .and_then(ProjectXExecutionClient::try_parse_client_order_id),
        );
    }

    #[rstest::rstest]
    fn reconciliation_snapshot_does_not_commit_partial_invalid_state() {
        let (emitter, mut rx) = test_emitter();
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let open_order_state = DashMap::new();
        let open_position_state = DashMap::new();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        let order = base_order(1, Some(1), 2);
        let trade = base_trade(order.id.get(), 20, 5200.5, 1);
        let invalid_position = base_position(99, 1, 5200.25);

        let result = ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![order],
                recent_trade_orders: vec![],
                positions: vec![invalid_position],
                trades: vec![trade],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            UnixNanos::from(1_000),
            true,
            None,
        );

        assert!(result.is_err());
        assert_eq!(count_reports(&mut rx), 0);
        assert!(open_order_state.is_empty());
        assert!(open_position_state.is_empty());
        assert!(seen_trade_ids.is_empty());
        assert!(venue_to_client.is_empty());
    }

    #[rstest::rstest]
    fn reconciliation_snapshot_emits_recent_closed_order_before_fill() {
        let (emitter, mut rx) = test_emitter();
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let open_order_state = DashMap::new();
        let open_position_state = DashMap::new();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));

        let mut closed_order = base_order(2, Some(1), 1);
        closed_order.id = projectx_client::OrderId::new(42).unwrap();
        closed_order.side = projectx_client::Side::Ask;

        let mut trade = base_trade(closed_order.id.get(), 20, 5200.5, 1);
        trade.side = projectx_client::Side::Ask;

        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![],
                recent_trade_orders: vec![closed_order],
                positions: vec![],
                trades: vec![trade],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            UnixNanos::from(1_000),
            true,
            None,
        )
        .expect("valid reconciliation snapshot");

        let reports = collect_reports(&mut rx);
        assert_eq!(reports.len(), 2);
        assert!(
            matches!(&reports[0], ExecutionReport::Order(order) if order.venue_order_id.to_string() == "42")
        );
        assert!(
            matches!(&reports[1], ExecutionReport::Fill(fill) if fill.venue_order_id.to_string() == "42")
        );
        assert!(open_order_state.is_empty());
    }

    #[rstest::rstest]
    fn reconciliation_snapshot_skips_known_closed_order_report_when_fill_exists() {
        let (emitter, mut rx) = test_emitter();
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let open_order_state = DashMap::new();
        let open_position_state = DashMap::new();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        let client_order_id = ClientOrderId::from("PXEMA-TEST-RECON-001");
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        venue_to_client.insert(42, client_order_id);

        let mut closed_order = base_order(2, Some(1), 1);
        closed_order.id = projectx_client::OrderId::new(42).unwrap();
        closed_order.side = projectx_client::Side::Ask;
        closed_order.custom_tag = Some(client_order_id.to_string());

        let mut trade = base_trade(closed_order.id.get(), 20, 5200.5, 1);
        trade.side = projectx_client::Side::Ask;

        ProjectXExecutionClient::apply_reconciliation_snapshot_with_context(
            super::ReconciliationSnapshot {
                open_orders: vec![],
                recent_trade_orders: vec![closed_order],
                positions: vec![],
                trades: vec![trade],
            },
            &account_ids,
            Venue::from("PROJECTX"),
            None,
            &emitter,
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &open_order_state,
            &open_position_state,
            UnixNanos::from(1_000),
            true,
            None,
        )
        .expect("valid reconciliation snapshot");

        let reports = collect_reports(&mut rx);
        assert_eq!(reports.len(), 1);
        assert!(
            matches!(&reports[0], ExecutionReport::Fill(fill) if fill.venue_order_id.to_string() == "42")
        );
    }

    #[rstest::rstest]
    fn pending_trade_is_flushed_once_venue_mapping_is_known() {
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let pending_trades_by_order_id: DashMap<i64, Vec<Trade>> = DashMap::new();
        let client_order_id = ClientOrderId::from("PXEMA-TEST-001");
        let venue_order_id = 42;

        order_meta_by_client.insert(
            client_order_id,
            super::ProjectXOrderMeta {
                strategy_id: StrategyId::from("STRAT-001"),
                instrument_id: InstrumentId::from("MNQM26.PROJECTX"),
                order_side: OrderSide::Sell,
                order_type: OrderType::Market,
            },
        );

        let trade: Trade = serde_json::from_value(serde_json::json!({
            "id": 7,
            "accountId": 1,
            "contractId": "CON.F.US.MNQ.M26".to_string(),
            "creationTimestamp": "2026-04-02T00:00:01Z".to_string(),
            "price": 5200.25,
            "profitAndLoss": 0.0,
            "fees": 0.0,
            "side": 1,
            "size": 1,
            "voided": false,
            "orderId": venue_order_id,
        }))
        .expect("Trade");

        ProjectXExecutionClient::handle_user_trades_event(
            vec![trade.clone()],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );

        assert_eq!(count_filled_order_events(&mut rx), 0);
        assert!(!seen_trade_ids.contains(&trade.id.get()));
        assert_eq!(
            pending_trades_by_order_id
                .get(&venue_order_id)
                .map(|entry| entry.len()),
            Some(1)
        );

        venue_to_client.insert(venue_order_id, client_order_id);
        ProjectXExecutionClient::flush_pending_trades_for_order(
            venue_order_id,
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );

        assert_eq!(count_filled_order_events(&mut rx), 1);
        assert!(seen_trade_ids.contains(&trade.id.get()));
        assert!(!pending_trades_by_order_id.contains_key(&venue_order_id));

        ProjectXExecutionClient::handle_user_trades_event(
            vec![trade],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );

        assert_eq!(count_filled_order_events(&mut rx), 0);
    }

    #[rstest::rstest]
    fn parse_gateway_user_trade_accepts_null_profit_and_loss() {
        let payload = json!([{
            "id": 7,
            "accountId": 1,
            "contractId": "CON.F.US.MNQ.M26",
            "creationTimestamp": "2026-04-02T00:00:01Z",
            "price": 5200.25,
            "profitAndLoss": null,
            "fees": 0.0,
            "side": 1,
            "size": 1,
            "voided": false,
            "orderId": 42
        }]);

        let trades: Vec<Trade> = serde_json::from_value(payload).expect("trades");

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].profit_and_loss, None);
    }

    #[rstest::rstest]
    fn trade_commission_preserves_signed_rebates() {
        let mut trade = base_trade(42, 7, 5200.25, 1);
        trade.fees = Decimal::new(-125, 2);
        trade.commissions = Some(Decimal::new(25, 2));

        let commission = ProjectXExecutionClient::trade_commission(&trade, Currency::USD())
            .expect("signed commission should convert");

        assert_eq!(commission.as_decimal(), Decimal::NEGATIVE_ONE);
    }

    #[rstest::rstest]
    fn malformed_provider_contract_id_is_rejected_without_panicking() {
        let result = ProjectXExecutionClient::instrument_id_from_contract_id("", None);

        assert!(result.is_err());
    }

    #[rstest::rstest]
    fn projectx_execution_fixture_samples_deserialize_for_replay_tests() {
        let fixture = load_fixture("execution_snapshot.json");

        let accounts: Vec<Account> =
            serde_json::from_value(fixture["accounts"].clone()).expect("accounts");
        let positions: Vec<Position> =
            serde_json::from_value(fixture["positions"].clone()).expect("positions");
        let orders: Vec<Order> = serde_json::from_value(fixture["orders"].clone()).expect("orders");
        let trades: Vec<Trade> = serde_json::from_value(fixture["trades"].clone()).expect("trades");

        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].name, "SIM-ACCOUNT");
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].contract_id.to_string(), "CON.F.US.MNQ.M26");
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].custom_tag.as_deref(), Some("STRAT-ENTRY"));
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].order_id.get(), 7001);
    }

    #[rstest::rstest]
    fn projectx_user_stream_fixture_replay_emits_expected_outputs() {
        let fixture = load_fixture("user_stream_events.json");
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids
            .write()
            .insert(42, AccountId::from("PROJECTX-42"));
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let last_order_state = DashMap::new();
        let last_order_status = DashMap::new();
        let open_order_state: DashMap<i64, super::OpenOrderState> = DashMap::new();
        let open_position_state: DashMap<i64, super::PositionState> = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let pending_trades_by_order_id: DashMap<i64, Vec<Trade>> = DashMap::new();
        let client_order_id = ClientOrderId::from("PXEMA-TEST-FIXTURE-001");

        order_meta_by_client.insert(
            client_order_id,
            super::ProjectXOrderMeta {
                strategy_id: StrategyId::from("STRAT-001"),
                instrument_id: InstrumentId::from("MNQM26.PROJECTX"),
                order_side: OrderSide::Buy,
                order_type: OrderType::Limit,
            },
        );

        let accounts: Vec<Account> =
            serde_json::from_value(fixture["accounts"].clone()).expect("accounts");
        let positions: Vec<Position> =
            serde_json::from_value(fixture["positions"].clone()).expect("positions");
        let orders_initial: Vec<Order> =
            serde_json::from_value(fixture["orders_initial"].clone()).expect("orders");
        let orders_progress: Vec<Order> =
            serde_json::from_value(fixture["orders_progress"].clone()).expect("orders");
        let trades: Vec<Trade> = serde_json::from_value(fixture["trades"].clone()).expect("trades");

        ProjectXExecutionClient::handle_user_accounts_event(
            accounts,
            &emitter,
            &account_ids,
            Venue::from("PROJECTX"),
            AccountType::Margin,
            Some(Currency::USD()),
        )
        .expect("valid account update");
        assert_eq!(count_account_events(&mut rx), 1);

        let changed_accounts =
            ProjectXExecutionClient::handle_user_positions_event(positions, &open_position_state);
        assert_eq!(changed_accounts, HashSet::from([42]));

        let initial_refresh_accounts = ProjectXExecutionClient::handle_user_orders_event(
            orders_initial,
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );
        assert!(initial_refresh_accounts.is_empty());
        assert_eq!(count_reports(&mut rx), 0);
        assert_eq!(
            venue_to_client.get(&7101).map(|entry| *entry),
            Some(client_order_id)
        );

        let progress_refresh_accounts = ProjectXExecutionClient::handle_user_orders_event(
            orders_progress,
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );
        assert!(progress_refresh_accounts.is_empty());
        assert_eq!(count_reports(&mut rx), 1);

        ProjectXExecutionClient::handle_user_trades_event(
            trades,
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );
        assert_eq!(count_filled_order_events(&mut rx), 1);
        assert!(seen_trade_ids.contains(&9101));
    }

    #[rstest::rstest]
    #[rstest::rstest]
    fn gateway_user_market_order_fill_progress_uses_post_submit_snapshot_only() {
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let last_order_state = DashMap::new();
        let last_order_status = DashMap::new();
        let open_order_state: DashMap<i64, super::OpenOrderState> = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let pending_trades_by_order_id: DashMap<i64, Vec<Trade>> = DashMap::new();
        let client_order_id = ClientOrderId::from("PXEMA-TEST-002");

        order_meta_by_client.insert(
            client_order_id,
            super::ProjectXOrderMeta {
                strategy_id: StrategyId::from("STRAT-001"),
                instrument_id: InstrumentId::from("MNQM26.PROJECTX"),
                order_side: OrderSide::Buy,
                order_type: OrderType::Market,
            },
        );

        let accounts = ProjectXExecutionClient::handle_user_orders_event(
            vec![
                serde_json::from_value(serde_json::json!({
                    "id": 84,
                    "accountId": 1,
                    "contractId": "CON.F.US.MNQ.M26",
                    "symbolId": "F.US.MNQ",
                    "creationTimestamp": "2026-04-02T00:00:00Z",
                    "updateTimestamp": "2026-04-02T00:00:01Z",
                    "status": 2,
                    "type": 2,
                    "side": 0,
                    "size": 1,
                    "fillVolume": 1,
                    "customTag": client_order_id.to_string(),
                }))
                .expect("Order"),
            ],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );

        assert!(accounts.is_empty());
        assert_eq!(count_reports(&mut rx), 0);
        assert_eq!(count_filled_order_events(&mut rx), 0);
    }

    #[rstest::rstest]
    fn gateway_user_order_fill_progress_without_mapping_triggers_snapshot_refresh() {
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let last_order_state = DashMap::new();
        let last_order_status = DashMap::new();
        let open_order_state: DashMap<i64, super::OpenOrderState> = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let pending_trades_by_order_id: DashMap<i64, Vec<Trade>> = DashMap::new();

        let accounts = ProjectXExecutionClient::handle_user_orders_event(
            vec![
                serde_json::from_value(serde_json::json!({
                    "id": 84,
                    "accountId": 1,
                    "contractId": "CON.F.US.MNQ.M26",
                    "symbolId": "F.US.MNQ",
                    "creationTimestamp": "2026-04-02T00:00:00Z",
                    "updateTimestamp": "2026-04-02T00:00:01Z",
                    "status": 2,
                    "type": 2,
                    "side": 0,
                    "size": 1,
                    "fillVolume": 1,
                }))
                .expect("Order"),
            ],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );

        assert_eq!(accounts, HashSet::from([1]));
        assert_eq!(count_reports(&mut rx), 0);
        assert_eq!(count_filled_order_events(&mut rx), 0);
    }

    #[rstest::rstest]
    fn gateway_user_position_change_triggers_snapshot_refresh() {
        let open_position_state = DashMap::new();

        let accounts = ProjectXExecutionClient::handle_user_positions_event(
            vec![
                serde_json::from_value(serde_json::json!({
                    "id": 10,
                    "accountId": 1,
                    "contractId": "CON.F.US.MNQ.M26",
                    "creationTimestamp": "2026-04-02T00:00:01Z",
                    "type": 1,
                    "size": 1,
                    "averagePrice": 5200.25,
                }))
                .expect("Position"),
            ],
            &open_position_state,
        );

        assert_eq!(accounts, HashSet::from([1]));
        assert!(open_position_state.is_empty());
        open_position_state.insert(
            10,
            super::PositionState {
                size: 1,
                type_: projectx_client::PositionType::Long.code(),
                average_price: Decimal::new(520_025, 2),
            },
        );

        let accounts = ProjectXExecutionClient::handle_user_positions_event(
            vec![
                serde_json::from_value(serde_json::json!({
                    "id": 10,
                    "accountId": 1,
                    "contractId": "CON.F.US.MNQ.M26",
                    "creationTimestamp": "2026-04-02T00:00:01Z",
                    "type": 1,
                    "size": 1,
                    "averagePrice": 5200.25,
                }))
                .expect("Position"),
            ],
            &open_position_state,
        );

        assert!(accounts.is_empty());
    }

    #[rstest::rstest]
    fn gateway_user_account_emits_account_state() {
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));

        ProjectXExecutionClient::handle_user_accounts_event(
            vec![
                serde_json::from_value(serde_json::json!({
                    "id": 1,
                    "name": "Main Trading Account",
                    "balance": 10_000.50,
                    "canTrade": true,
                    "isVisible": true,
                    "simulated": false,
                }))
                .expect("Account"),
            ],
            &emitter,
            &account_ids,
            Venue::from("PROJECTX"),
            AccountType::Margin,
            Some(Currency::USD()),
        )
        .expect("valid account update");

        assert_eq!(count_account_events(&mut rx), 1);
    }

    #[rstest::rstest]
    fn gateway_user_limit_order_same_status_fill_progress_emits_order_report() {
        let (emitter, mut rx) = test_emitter();
        let account_ids = Arc::new(ParkingRwLock::new(AHashMap::new()));
        account_ids.write().insert(1, AccountId::from("PROJECTX-1"));
        let order_meta_by_client = DashMap::new();
        let venue_to_client = DashMap::new();
        let last_order_state = DashMap::new();
        let last_order_status = DashMap::new();
        let open_order_state: DashMap<i64, super::OpenOrderState> = DashMap::new();
        let seen_trade_ids = DashSet::new();
        let voided_trade_ids = DashSet::new();
        let pending_trades_by_order_id: DashMap<i64, Vec<Trade>> = DashMap::new();
        let client_order_id = ClientOrderId::from("PXEMA-TEST-003");

        order_meta_by_client.insert(
            client_order_id,
            super::ProjectXOrderMeta {
                strategy_id: StrategyId::from("STRAT-001"),
                instrument_id: InstrumentId::from("MNQM26.PROJECTX"),
                order_side: OrderSide::Buy,
                order_type: OrderType::Limit,
            },
        );

        let base_order: Order = serde_json::from_value(serde_json::json!({
            "id": 91,
            "accountId": 1,
            "contractId": "CON.F.US.MNQ.M26",
            "symbolId": "F.US.MNQ",
            "creationTimestamp": "2026-04-02T00:00:00Z",
            "updateTimestamp": "2026-04-02T00:00:01Z",
            "status": 1,
            "type": 2,
            "side": 0,
            "size": 2,
            "fillVolume": 0,
            "customTag": client_order_id.to_string(),
        }))
        .expect("order");

        let accounts = ProjectXExecutionClient::handle_user_orders_event(
            vec![base_order.clone()],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );
        assert!(accounts.is_empty());
        assert_eq!(count_reports(&mut rx), 0);

        let mut partial = base_order;
        partial.fill_volume = Some(1);
        partial.filled_price =
            Some(serde_json::from_value(serde_json::json!(5200.25)).expect("price"));

        let accounts = ProjectXExecutionClient::handle_user_orders_event(
            vec![partial],
            &emitter,
            TraderId::from("TRADER-001"),
            &account_ids,
            Venue::from("PROJECTX"),
            &order_meta_by_client,
            &venue_to_client,
            &last_order_state,
            &last_order_status,
            &open_order_state,
            &seen_trade_ids,
            &voided_trade_ids,
            &pending_trades_by_order_id,
            None,
        );
        assert!(accounts.is_empty());
        assert_eq!(count_reports(&mut rx), 1);
    }

    #[rstest::rstest]
    #[rstest::rstest]
    fn map_position_report_uses_netting_shape_and_avg_open_price() {
        let report = ProjectXExecutionClient::map_position_report_with_context(
            base_position(1, 1, 5200.25),
            AccountId::from("PROJECTX-PRAC-V2-64413-98419885"),
            UnixNanos::from(1_000),
            None,
        )
        .expect("valid position report");

        assert_eq!(report.venue_position_id, None);
        assert_eq!(
            report.avg_px_open.as_ref().map(ToString::to_string),
            Some("5200.25".to_string())
        );
    }

    #[rstest::rstest]
    #[rstest::rstest]
    fn submit_snapshot_refresh_is_enabled_for_market_orders() {
        assert!(ProjectXExecutionClient::should_refresh_submit_snapshot(
            OrderType::Market
        ));
        assert!(ProjectXExecutionClient::should_refresh_submit_snapshot(
            OrderType::MarketToLimit
        ));
        assert!(!ProjectXExecutionClient::should_refresh_submit_snapshot(
            OrderType::Limit
        ));
    }

    #[rstest::rstest]
    fn current_state_mass_status_omits_historical_fill_reports() {
        let client = ProjectXExecutionClient::default();
        let ts_now = UnixNanos::from(42);
        let order_report = client
            .map_order_report(base_order(1, Some(1), 2), ts_now)
            .expect("valid order report");
        let position_report = client
            .map_position_report(base_position(1, 1, 5200.25), ts_now)
            .expect("valid position report");

        let mass_status = client.build_current_state_mass_status(
            ts_now,
            vec![order_report],
            Vec::new(),
            vec![position_report],
        );

        assert_eq!(mass_status.order_reports().len(), 1);
        assert!(mass_status.fill_reports().is_empty());
        assert_eq!(mass_status.position_reports().len(), 1);
    }

    #[rstest::rstest]
    fn select_subscribed_account_prefers_configured_account_name() {
        let client = ProjectXExecutionClient::default();
        let accounts = vec![
            serde_json::from_value(serde_json::json!({
                "id": 101,
                "name": "ALT-ACCOUNT".to_string(),
                "balance": 0.0,
                "canTrade": true,
                "isVisible": true,
            }))
            .expect("Account"),
            serde_json::from_value(serde_json::json!({
                "id": 202,
                "name": "PROJECTX-001".to_string(),
                "balance": 0.0,
                "canTrade": true,
                "isVisible": true,
            }))
            .expect("Account"),
        ];

        let context = client.account_state_context();
        let ids = context
            .cache_account_ids(&accounts)
            .expect("valid account IDs");
        let selected = context
            .select_subscribed_account_id_num(&ids)
            .expect("configured account should be selected");

        assert_eq!(selected, 202);
    }

    #[rstest::rstest]
    #[rstest::rstest]
    fn contract_id_from_instrument_prefers_cached_projectx_contract_metadata() {
        let (client, cache) = test_client_with_cache();
        let instrument = projectx_contract_to_instrument(
            &serde_json::from_value(serde_json::json!({
                "id": "CON.F.US.MNQ.M26",
                "name": "MNQM6",
                "description": "Micro Nasdaq",
                "tickSize": 0.25,
                "tickValue": 0.5,
                "activeContract": true,
                "symbolId": "F.US.MNQ",
            }))
            .expect("Contract"),
        )
        .expect("contract should convert into an instrument");
        let instrument_id = instrument.id();
        cache
            .borrow_mut()
            .add_instrument(instrument)
            .expect("instrument should cache cleanly");

        let contract_id = client.contract_id_from_instrument_id(instrument_id);
        assert_eq!(contract_id, "CON.F.US.MNQ.M26");
    }

    #[rstest::rstest]
    fn contract_id_from_instrument_falls_back_to_symbol_translation() {
        let client = ProjectXExecutionClient::default();

        let contract_id =
            client.contract_id_from_instrument_id(InstrumentId::from("MESM26.PROJECTX"));
        assert_eq!(contract_id, "CON.F.US.MES.M26");
    }

    #[rstest::rstest]
    fn instrument_id_from_contract_uses_cached_vendor_alias_when_available() {
        let (_client, cache) = test_client_with_cache();
        cache
            .borrow_mut()
            .add_instrument(
                projectx_contract_to_instrument(
                    &serde_json::from_value(serde_json::json!({
                        "id": "CON.F.US.MNQ.M26",
                        "name": "MNQM6",
                        "description": "Micro Nasdaq",
                        "tickSize": 0.25,
                        "tickValue": 0.5,
                        "activeContract": true,
                        "symbolId": "F.US.MNQ",
                    }))
                    .expect("Contract"),
                )
                .expect("contract should convert into an instrument"),
            )
            .expect("instrument should cache cleanly");

        let aliases = {
            let cache = cache.borrow();
            ProjectXExecutionClient::instrument_aliases_from_cache(&cache)
        };
        let instrument_id =
            ProjectXExecutionClient::instrument_id_from_contract_id("MNQM6", Some(&aliases))
                .expect("valid cached alias");

        assert_eq!(instrument_id, InstrumentId::from("MNQM26.PROJECTX"));
    }

    #[rstest::rstest]
    fn cancel_all_orders_rejects_when_user_stream_is_missing() {
        let client = ProjectXExecutionClient::default();
        client.core.set_connected();
        client
            .execution_stale
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let cmd = CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("PROJECTX")),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("MESM26.PROJECTX"),
            OrderSide::Buy,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        let err = client
            .cancel_all_orders(cmd)
            .expect_err("missing user stream should fence cancel_all_orders");
        assert!(err.to_string().contains("user stream"));
    }

    #[rstest::rstest]
    fn cancel_all_orders_returns_readiness_error_when_not_ready() {
        let client = ProjectXExecutionClient::default();
        let cmd = CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("PROJECTX")),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("MESM26.PROJECTX"),
            OrderSide::Buy,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        let err = client
            .cancel_all_orders(cmd)
            .expect_err("disconnected client should reject cancel_all_orders");
        assert!(
            err.to_string().contains("not connected")
                || err.to_string().contains("execution state is stale"),
            "unexpected readiness error: {err}"
        );
    }

    #[rstest::rstest]
    fn resolve_target_account_prefers_explicit_numeric_param() {
        let client = ProjectXExecutionClient::default();
        client.subscribed_account_ids.write().insert(1);
        client.subscribed_account_ids.write().insert(2);
        *client
            .account_id_num
            .lock()
            .expect(nautilus_core::MUTEX_POISONED) = Some(1);

        let mut params = Params::new();
        params.insert("account_id_num".to_string(), json!(2));

        let resolved = client
            .resolve_target_account_id_num(Some(&params), None)
            .expect("explicit account id num should resolve");
        assert_eq!(resolved, 2);
    }

    #[rstest::rstest]
    fn resolve_target_account_supports_string_forms() {
        let client = ProjectXExecutionClient::default();
        client.subscribed_account_ids.write().insert(1);
        client.subscribed_account_ids.write().insert(2);
        *client
            .account_id_num
            .lock()
            .expect(nautilus_core::MUTEX_POISONED) = Some(1);

        let mut numeric_string = Params::new();
        numeric_string.insert("account_id".to_string(), json!("2"));
        let resolved_numeric = client
            .resolve_target_account_id_num(Some(&numeric_string), None)
            .expect("numeric account string should resolve");
        assert_eq!(resolved_numeric, 2);

        let mut full_account_id = Params::new();
        full_account_id.insert("account_id".to_string(), json!("PROJECTX-2"));
        let resolved_full = client
            .resolve_target_account_id_num(Some(&full_account_id), None)
            .expect("full account id string should resolve");
        assert_eq!(resolved_full, 2);
    }

    #[rstest::rstest]
    fn resolve_target_account_supports_raw_topstep_account_labels() {
        let client = ProjectXExecutionClient::default();
        client
            .account_ids
            .write()
            .insert(2, AccountId::from("PROJECTX-PRAC-V2-64413-98419885"));
        client.subscribed_account_ids.write().insert(2);
        *client
            .account_id_num
            .lock()
            .expect(nautilus_core::MUTEX_POISONED) = Some(2);

        let mut raw_account_id = Params::new();
        raw_account_id.insert("account_id".to_string(), json!("PRAC-V2-64413-98419885"));
        let resolved = client
            .resolve_target_account_id_num(Some(&raw_account_id), None)
            .expect("raw Topstep account label should resolve");
        assert_eq!(resolved, 2);
    }
}
