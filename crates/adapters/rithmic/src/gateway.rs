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

//! Central gateway for managing Rithmic plant connections.
//!
//! The `RithmicGateway` provides a single point of connection management for all
//! Rithmic plants (ticker, order, pnl, history). It handles:
//! - Plant lifecycle (connect, disconnect, reconnect)
//! - Background message processing tasks
//! - Event channels for downstream consumers
//! - Shared instrument state
//!
//! # Example
//!
//! ```rust,ignore
//! use rithmic_nt::{RithmicGateway, GatewayConfig};
//!
//! let config = GatewayConfig::from_env()?;
//! let mut gateway = RithmicGateway::new(config);
//! gateway.connect().await?;
//! ```

use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use ahash::AHashMap;
use arc_swap::ArcSwap;
use nautilus_common::live::get_runtime;
use nautilus_model::{
    data::{BookOrder, InstrumentStatus, OrderBookDepth10, depth::DEPTH10_LEN},
    enums::{MarketStatusAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use rithmic_rs::{
    RithmicAccount, RithmicConfig,
    api::RithmicResponse,
    error::RithmicError as RsRithmicError,
    plants::{
        history_plant::{RithmicHistoryPlant, RithmicHistoryPlantHandle},
        order_plant::{RithmicOrderPlant, RithmicOrderPlantHandle},
        pnl_plant::{RithmicPnlPlant, RithmicPnlPlantHandle},
        ticker_plant::{RithmicTickerPlant, RithmicTickerPlantHandle},
    },
    rti::{
        messages::RithmicMessage,
        request_time_bar_replay::BarType as TimeBarType,
        request_time_bar_update::{BarType as LiveTimeBarType, Request as LiveTimeBarRequest},
    },
    ws::ConnectStrategy,
};
use tokio::{
    sync::{RwLock, broadcast},
    task::JoinHandle,
};
use ustr::Ustr;

use crate::{
    common::{
        enums::ConnectionState,
        parse::{rithmic_depth_order_id, tick_size_to_precision},
    },
    config::{RithmicEnv, optional_env_var, parse_rithmic_env, required_env_var},
    data::{
        MarketDataEvent, RithmicBarType,
        custom::{
            RithmicCustomData, RithmicEndOfDayPrices, RithmicIndicatorPrices, RithmicOpenInterest,
            RithmicOrderPriceLimits, RithmicQuoteStatistics, RithmicSymbolMarginRate,
            RithmicTradeStatistics,
        },
    },
    error::{Result, RithmicError},
    execution::{ExecutionEvent, ExecutionHandler, RithmicExecutionClient},
    instruments::front_month::resolve_front_month_contract_with_handle,
    providers::{AccountEvent, PositionEvent},
};

/// Maximum reconnection attempts before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 100;

const DEFAULT_APP_VERSION: &str = "1.0";

// The supported catalog is limited to USD-margined contracts on CME, CBOT,
// NYMEX, and COMEX. Rithmic account PnL messages carry no currency field.
const SUPPORTED_ACCOUNT_CURRENCY: &str = "USD";

/// Initial backoff duration for reconnection.
const INITIAL_BACKOFF_MS: u64 = 1000;

/// Maximum backoff duration for reconnection.
const MAX_BACKOFF_MS: u64 = 30000;

/// Maximum time to wait for an individual plant logout before aborting it.
const DISCONNECT_TIMEOUT_SECS: u64 = 5;

/// Broadcast buffer for downstream market data, execution, and PnL consumers.
const DOWNSTREAM_EVENT_BUFFER_CAPACITY: usize = 10_000;

/// Maximum number of live deltas held while a depth snapshot is in flight.
const ORDER_BOOK_BOOTSTRAP_MAX_DELTAS: usize = 10_000;

/// Maximum time an order-book snapshot may keep live deltas buffered.
const ORDER_BOOK_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum automatic resume attempts for a single historical bar request.
const MAX_HISTORY_RESUME_ATTEMPTS: usize = 128;

fn normalize_server_name(server: &str) -> String {
    server
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn resolve_server_endpoint(server: &str) -> Result<&'static str> {
    match normalize_server_name(server).as_str() {
        "chicago" => Ok("wss://rprotocol.rithmic.com:443"),
        "sydney" => Ok("wss://rprotocol-au.rithmic.com:443"),
        "saopaulo" => Ok("wss://rprotocol-br.rithmic.com:443"),
        "colo75" => Ok("wss://protocol-colo75.rithmic.com:443"),
        "frankfurt" => Ok("wss://rprotocol-de.rithmic.com:443"),
        "hongkong" => Ok("wss://rprotocol-hk.rithmic.com:443"),
        "ireland" => Ok("wss://rprotocol-ie.rithmic.com:443"),
        "mumbai" => Ok("wss://rprotocol-in.rithmic.com:443"),
        "seoul" => Ok("wss://rprotocol-kr.rithmic.com:443"),
        "capetown" => Ok("wss://rprotocol-za.rithmic.com:443"),
        "tokyo" => Ok("wss://rprotocol-jp.rithmic.com:443"),
        "singapore" => Ok("wss://rprotocol-sg.rithmic.com:443"),
        "test" => Ok("wss://rituz00100.rithmic.com:443"),
        _ => Err(RithmicError::Config(format!(
            "Unknown Rithmic server {server:?}. Expected one of: Chicago, Sydney, Sao Paulo, Colo75, Frankfurt, Hong Kong, Ireland, Mumbai, Seoul, Cape Town, Tokyo, Singapore, Test"
        ))),
    }
}

fn default_server_name(env: RithmicEnv) -> &'static str {
    match env {
        RithmicEnv::Demo | RithmicEnv::Live => "Chicago",
        RithmicEnv::Test => "Test",
    }
}

fn time_bar_interval_seconds(bar_type: TimeBarType, bar_period: i32) -> Option<i32> {
    let base_seconds = match bar_type {
        TimeBarType::SecondBar => 1,
        TimeBarType::MinuteBar => 60,
        TimeBarType::DailyBar => 86_400,
        TimeBarType::WeeklyBar => 604_800,
    };

    if bar_period <= 0 {
        return None;
    }

    bar_period.checked_mul(base_seconds)
}

fn time_bar_replay_request_key(message: &RithmicMessage) -> Option<&str> {
    match message {
        RithmicMessage::ResponseTimeBarReplay(bar) => bar.request_key.as_deref(),
        _ => None,
    }
}

fn time_bar_replay_marker(message: &RithmicMessage) -> Option<i32> {
    match message {
        RithmicMessage::ResponseTimeBarReplay(bar) => bar.marker.filter(|value| *value > 0),
        _ => None,
    }
}

fn last_time_bar_marker<'a, I>(messages: I) -> Option<i32>
where
    I: IntoIterator<Item = &'a RithmicMessage>,
    I::IntoIter: DoubleEndedIterator,
{
    messages.into_iter().rev().find_map(time_bar_replay_marker)
}

fn should_resume_time_bar_history<'a, I>(
    messages: I,
    bar_type: TimeBarType,
    bar_period: i32,
    end_time_sec: i32,
) -> bool
where
    I: IntoIterator<Item = &'a RithmicMessage>,
    I::IntoIter: DoubleEndedIterator,
{
    let interval_sec = match time_bar_interval_seconds(bar_type, bar_period) {
        Some(value) => value,
        None => return false,
    };
    let last_marker = match last_time_bar_marker(messages) {
        Some(value) => value,
        None => return false,
    };

    last_marker.saturating_add(interval_sec) < end_time_sec
}

/// Configuration for the Rithmic gateway.
///
/// This unified configuration contains all credentials needed to connect
/// to any Rithmic plant. Use `from_env()` to load from environment variables.
#[must_use]
#[derive(Clone)]
pub struct GatewayConfig {
    /// Rithmic environment (Demo, Live, Test).
    pub environment: RithmicEnv,
    /// Rithmic username.
    pub username: String,
    /// Rithmic password.
    pub password: String,
    /// System name for Rithmic connection.
    pub system_name: String,
    /// Application name sent during login.
    pub app_name: String,
    /// Application version sent during login.
    pub app_version: String,
    /// FCM ID (Futures Commission Merchant).
    pub fcm_id: String,
    /// IB ID (Introducing Broker).
    pub ib_id: String,
    /// Trading account ID.
    pub account_id: String,
    /// Optional named primary Rithmic server.
    pub server: Option<String>,
    /// Optional named alternate Rithmic server.
    pub alt_server: Option<String>,
    /// Optional primary WebSocket URL override.
    pub url_override: Option<String>,
    /// Optional alternate WebSocket URL override.
    pub beta_url_override: Option<String>,
    /// Whether to connect the ticker plant.
    pub enable_ticker: bool,
    /// Whether to connect the order plant.
    pub enable_order: bool,
    /// Whether to connect the PnL plant.
    pub enable_pnl: bool,
    /// Whether to connect the history plant (lazy by default).
    pub enable_history: bool,
}

impl Debug for GatewayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(GatewayConfig))
            .field("environment", &self.environment)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("system_name", &self.system_name)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("fcm_id", &self.fcm_id)
            .field("ib_id", &self.ib_id)
            .field("account_id", &self.account_id)
            .field("server", &self.server)
            .field("alt_server", &self.alt_server)
            .field(
                "url_override",
                &self.url_override.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "beta_url_override",
                &self.beta_url_override.as_ref().map(|_| "[REDACTED]"),
            )
            .field("enable_ticker", &self.enable_ticker)
            .field("enable_order", &self.enable_order)
            .field("enable_pnl", &self.enable_pnl)
            .field("enable_history", &self.enable_history)
            .finish()
    }
}

impl GatewayConfig {
    /// Creates a validated gateway configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when a required login or application identity is empty.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        environment: RithmicEnv,
        username: impl Into<String>,
        password: impl Into<String>,
        system_name: impl Into<String>,
        app_name: impl Into<String>,
        fcm_id: impl Into<String>,
        ib_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Result<Self> {
        let config = Self {
            environment,
            username: username.into(),
            password: password.into(),
            system_name: system_name.into(),
            app_name: app_name.into(),
            app_version: DEFAULT_APP_VERSION.to_string(),
            fcm_id: fcm_id.into(),
            ib_id: ib_id.into(),
            account_id: account_id.into(),
            server: None,
            alt_server: None,
            url_override: None,
            beta_url_override: None,
            enable_ticker: true,
            enable_order: true,
            enable_pnl: true,
            enable_history: false, // Lazy by default
        };
        config.validate_identity()?;
        Ok(config)
    }

    fn validate_identity(&self) -> Result<()> {
        for (name, value) in [
            ("username", self.username.as_str()),
            ("password", self.password.as_str()),
            ("system_name", self.system_name.as_str()),
            ("app_name", self.app_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(RithmicError::Config(format!(
                    "Rithmic {name} cannot be empty"
                )));
            }
        }
        Ok(())
    }

    /// Creates configuration from environment variables.
    ///
    /// Required environment variables:
    /// - `RITHMIC_USERNAME`
    /// - `RITHMIC_PASSWORD`
    /// - `RITHMIC_SYSTEM_NAME`
    /// - `RITHMIC_ACCOUNT_ID`
    /// - `RITHMIC_APP_NAME`
    /// - `RITHMIC_ENV` (optional, defaults to "demo")
    ///
    /// Optional environment variables:
    /// - `RITHMIC_APP_VERSION`
    /// - `RITHMIC_FCM_ID`
    /// - `RITHMIC_IB_ID`
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_profile(None)
    }

    /// Creates configuration from environment variables, optionally scoped by profile.
    pub fn from_env_with_profile(profile: Option<&str>) -> Result<Self> {
        let environment = optional_env_var("ENV", profile)?
            .map_or(Ok(RithmicEnv::Demo), |s| parse_rithmic_env(&s))?;
        let (url_key, alt_url_key) = match environment {
            RithmicEnv::Demo => ("DEMO_URL", "DEMO_ALT_URL"),
            RithmicEnv::Live => ("LIVE_URL", "LIVE_ALT_URL"),
            RithmicEnv::Test => ("TEST_URL", "TEST_ALT_URL"),
        };

        let config = Self {
            environment,
            username: required_env_var("USERNAME", profile)?,
            password: required_env_var("PASSWORD", profile)?,
            system_name: required_env_var("SYSTEM_NAME", profile)?,
            app_name: required_env_var("APP_NAME", profile)?,
            app_version: optional_env_var("APP_VERSION", profile)?
                .unwrap_or_else(|| DEFAULT_APP_VERSION.to_string()),
            fcm_id: optional_env_var("FCM_ID", profile)?.unwrap_or_default(),
            ib_id: optional_env_var("IB_ID", profile)?.unwrap_or_default(),
            account_id: required_env_var("ACCOUNT_ID", profile)?,
            server: optional_env_var("SERVER", profile)?,
            alt_server: optional_env_var("ALT_SERVER", profile)?,
            url_override: optional_env_var(url_key, profile)?,
            beta_url_override: optional_env_var(alt_url_key, profile)?,
            enable_ticker: true,
            enable_order: true,
            enable_pnl: true,
            enable_history: optional_env_var("ENABLE_HISTORY", profile)?
                .is_some_and(|v| !matches!(v.to_lowercase().as_str(), "false" | "0" | "no")),
        };
        config.validate_identity()?;
        Ok(config)
    }

    /// Enables or disables the ticker plant.
    pub fn with_ticker(mut self, enable: bool) -> Self {
        self.enable_ticker = enable;
        self
    }

    /// Enables or disables the order plant.
    pub fn with_order(mut self, enable: bool) -> Self {
        self.enable_order = enable;
        self
    }

    /// Enables or disables the PnL plant.
    pub fn with_pnl(mut self, enable: bool) -> Self {
        self.enable_pnl = enable;
        self
    }

    /// Enables or disables the history plant.
    pub fn with_history(mut self, enable: bool) -> Self {
        self.enable_history = enable;
        self
    }

    /// Sets the application name.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Sets the application version.
    pub fn with_app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = app_version.into();
        self
    }

    /// Sets the named primary Rithmic server.
    pub fn with_server(mut self, server: impl Into<String>) -> Self {
        self.server = Some(server.into());
        self
    }

    /// Sets the named alternate Rithmic server.
    pub fn with_alt_server(mut self, alt_server: impl Into<String>) -> Self {
        self.alt_server = Some(alt_server.into());
        self
    }

    /// Sets the primary WebSocket URL override.
    pub fn with_url_override(mut self, url: impl Into<String>) -> Self {
        self.url_override = Some(url.into());
        self
    }

    /// Sets the alternate WebSocket URL override.
    pub fn with_beta_url_override(mut self, beta_url: impl Into<String>) -> Self {
        self.beta_url_override = Some(beta_url.into());
        self
    }

    /// Converts to rithmic-rs RithmicConfig.
    pub(crate) fn to_rithmic_config(&self) -> Result<RithmicConfig> {
        self.validate_identity()?;
        let env = self.environment;

        let named_url = Some(
            resolve_server_endpoint(self.server.as_deref().unwrap_or(default_server_name(env)))?
                .to_string(),
        );
        let url = self
            .url_override
            .clone()
            .or(named_url)
            .ok_or_else(|| RithmicError::Config("Rithmic URL is not configured".to_string()))?;

        let named_beta_url = self
            .alt_server
            .as_deref()
            .map(resolve_server_endpoint)
            .transpose()?
            .map(str::to_string);
        let beta_url = self
            .beta_url_override
            .clone()
            .or(named_beta_url)
            .unwrap_or_default();

        let mut builder = RithmicConfig::builder(env)
            .user(self.username.clone())
            .password(self.password.clone())
            .app_name(self.app_name.clone())
            .app_version(self.app_version.clone())
            .url(url)
            .beta_url(beta_url);

        builder = builder.system_name(self.system_name.clone());

        builder
            .build()
            .map_err(|e| RithmicError::Config(e.to_string()))
    }

    /// Converts to an account identity for order and PnL operations.
    pub(crate) fn to_rithmic_account(&self) -> RithmicAccount {
        RithmicAccount::new(
            self.fcm_id.clone(),
            self.ib_id.clone(),
            self.account_id.clone(),
        )
    }
}

/// Instrument information cached from reference data.
#[derive(Debug, Clone)]
pub struct InstrumentInfo {
    /// Symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Tick size.
    pub tick_size: Option<f64>,
    /// Point value (dollar value per point).
    pub point_value: Option<f64>,
    /// Product code.
    pub product_code: Option<String>,
    /// Description/name.
    pub description: Option<String>,
    /// Currency.
    pub currency: Option<String>,
    /// Whether tradeable.
    pub is_tradeable: bool,
}

/// P&L event emitted by the gateway.
#[derive(Debug, Clone)]
pub enum PnlEvent {
    /// Account-level P&L update.
    Account(AccountEvent),
    /// Position-level P&L update.
    Position(PositionEvent),
}

#[derive(Debug)]
struct PendingOrderBookBootstrap {
    live_deltas: Vec<crate::data::BookDelta>,
    started_at: Instant,
}

impl Default for PendingOrderBookBootstrap {
    fn default() -> Self {
        Self {
            live_deltas: Vec::new(),
            started_at: Instant::now(),
        }
    }
}

/// Central gateway for Rithmic connections.
///
/// Manages the lifecycle of all Rithmic plants and provides event channels
/// for downstream consumers.
///
/// # Plant Handles
///
/// After calling `connect()`, the ticker, order, and PnL plant handles are moved
/// to background processor tasks. Only the history plant handle remains accessible
/// via `history_handle()` since it uses a request/response pattern.
pub struct RithmicGateway {
    config: GatewayConfig,

    // Plants (owned, used to get handles for disconnect)
    ticker_plant: Option<RithmicTickerPlant>,
    order_plant: Option<RithmicOrderPlant>,
    pnl_plant: Option<RithmicPnlPlant>,
    history_plant: Option<RithmicHistoryPlant>,

    // Query handles - kept for request/response operations
    // (separate from handles moved to processor tasks)
    ticker_query_handle: Option<RithmicTickerPlantHandle>,
    history_handle: Option<RithmicHistoryPlantHandle>,

    // Shared state
    instruments: Arc<RwLock<AHashMap<String, InstrumentInfo>>>,
    order_book_bootstraps: Arc<RwLock<AHashMap<String, PendingOrderBookBootstrap>>>,
    connection_state: Arc<ArcSwap<ConnectionState>>,
    order_updates_available: bool,
    order_processor_accounts: AHashMap<String, ()>,
    pnl_processor_accounts: AHashMap<String, ()>,

    // Event channels for downstream consumers
    market_data_tx: broadcast::Sender<MarketDataEvent>,
    execution_tx: broadcast::Sender<ExecutionEvent>,
    pnl_tx: broadcast::Sender<PnlEvent>,

    // Background task handles
    task_handles: Vec<JoinHandle<()>>,
}

impl RithmicGateway {
    /// Creates a new gateway without connecting.
    ///
    /// Call `connect()` to establish connections to Rithmic plants.
    pub fn new(config: GatewayConfig) -> Self {
        let (market_data_tx, _) = broadcast::channel(DOWNSTREAM_EVENT_BUFFER_CAPACITY);
        let (execution_tx, _) = broadcast::channel(DOWNSTREAM_EVENT_BUFFER_CAPACITY);
        let (pnl_tx, _) = broadcast::channel(DOWNSTREAM_EVENT_BUFFER_CAPACITY);

        Self {
            config,
            ticker_plant: None,
            order_plant: None,
            pnl_plant: None,
            history_plant: None,
            ticker_query_handle: None,
            history_handle: None,
            instruments: Arc::new(RwLock::new(AHashMap::new())),
            order_book_bootstraps: Arc::new(RwLock::new(AHashMap::new())),
            connection_state: Arc::new(ArcSwap::from_pointee(ConnectionState::Disconnected)),
            order_updates_available: false,
            order_processor_accounts: AHashMap::new(),
            pnl_processor_accounts: AHashMap::new(),
            market_data_tx,
            execution_tx,
            pnl_tx,
            task_handles: Vec::new(),
        }
    }

    /// Returns the gateway configuration.
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    /// Returns the current connection state.
    pub fn connection_state(&self) -> ConnectionState {
        **self.connection_state.load()
    }

    /// Returns true if the order plant accepted the order-updates subscription.
    pub fn order_updates_available(&self) -> bool {
        self.order_updates_available
    }

    /// Returns true if the gateway is connected.
    pub fn is_connected(&self) -> bool {
        self.connection_state() == ConnectionState::Connected
    }

    /// Returns a reference to the shared instruments map.
    pub fn instruments(&self) -> &Arc<RwLock<AHashMap<String, InstrumentInfo>>> {
        &self.instruments
    }

    /// Returns the ticker plant handle for queries if connected.
    ///
    /// This handle is separate from the one used by the background processor
    /// and can be used for request/response operations like getting reference data.
    pub fn ticker_handle(&self) -> Option<&RithmicTickerPlantHandle> {
        self.ticker_query_handle.as_ref()
    }

    /// Returns a mutable reference to the ticker plant handle for queries.
    pub fn ticker_handle_mut(&mut self) -> Option<&mut RithmicTickerPlantHandle> {
        self.ticker_query_handle.as_mut()
    }

    /// Returns a new order plant handle for sending commands.
    ///
    /// Each call creates a fresh handle with its own subscription receiver.
    /// This handle is separate from the one used by the background processor
    /// and can be used to place, modify, and cancel orders.
    ///
    /// Returns `None` if the order plant is not connected.
    pub fn order_handle(&self, account: &RithmicAccount) -> Option<RithmicOrderPlantHandle> {
        self.order_plant.as_ref().map(|p| p.get_handle(account))
    }

    /// Returns a new PnL plant handle for request/response operations if connected.
    pub fn pnl_handle(&self, account: &RithmicAccount) -> Option<RithmicPnlPlantHandle> {
        self.pnl_plant.as_ref().map(|p| p.get_handle(account))
    }

    /// Returns all trading accounts accessible to the current order session.
    pub async fn list_accounts(&self) -> Result<Vec<String>> {
        let account = self.config.to_rithmic_account();
        let handle = self
            .order_handle(&account)
            .ok_or_else(|| RithmicError::Connection("Order plant not connected".to_string()))?;

        let responses = handle
            .get_account_list()
            .await
            .map_err(|e| RithmicError::Connection(format!("Account list request failed: {e}")))?;

        Ok(responses
            .into_iter()
            .filter_map(|response| match response.message {
                RithmicMessage::ResponseAccountList(resp) => resp.account_id,
                _ => None,
            })
            .collect())
    }

    /// Gets the current front month contract symbol for a product.
    ///
    /// This is useful for subscribing to the active contract instead of a specific
    /// expired symbol.
    pub async fn get_front_month_symbol(&self, product: &str, exchange: &str) -> Result<String> {
        let handle = self
            .ticker_handle()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        let (symbol, _) =
            resolve_front_month_contract_with_handle(handle, product, exchange).await?;
        Ok(symbol)
    }

    /// Requests a PnL snapshot for the configured account.
    ///
    /// The snapshot payload is emitted asynchronously through the gateway's PnL event channel.
    pub async fn request_pnl_snapshot(&mut self) -> Result<()> {
        let account = self.config.to_rithmic_account();
        self.ensure_pnl_processor(&account).await?;
        let handle = self
            .pnl_handle(&account)
            .ok_or_else(|| RithmicError::Connection("PnL plant not connected".to_string()))?;

        handle
            .pnl_position_snapshots()
            .await
            .map_err(|e| RithmicError::Connection(format!("PnL snapshot request failed: {e}")))?;

        Ok(())
    }

    /// Ensures the shared order-plant processor is running for the specified account.
    pub async fn ensure_order_processor(&mut self, account: &RithmicAccount) -> Result<()> {
        if self
            .order_processor_accounts
            .contains_key(&account.account_id)
        {
            return Ok(());
        }

        let plant = self
            .order_plant
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Order plant not connected".to_string()))?;

        let handle = plant.get_handle(account);
        let response = handle
            .subscribe_order_updates()
            .await
            .map_err(|e| RithmicError::Connection(format!("Order subscribe failed: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Connection(format!(
                "Order subscribe error for account {}: {e}",
                account.account_id
            )));
        }

        let event_tx = self.execution_tx.clone();
        let connection_state = Arc::clone(&self.connection_state);
        let account_id = account.account_id.clone();

        let task = get_runtime().spawn(async move {
            order_processor(handle, event_tx, connection_state).await;
        });

        self.order_updates_available = true;
        self.order_processor_accounts.insert(account_id, ());
        self.task_handles.push(task);
        Ok(())
    }

    /// Ensures the shared PnL-plant processor is running for the specified account.
    pub async fn ensure_pnl_processor(&mut self, account: &RithmicAccount) -> Result<()> {
        if self
            .pnl_processor_accounts
            .contains_key(&account.account_id)
        {
            return Ok(());
        }

        let plant = self
            .pnl_plant
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("PnL plant not connected".to_string()))?;

        let handle = plant.get_handle(account);
        let response = handle
            .subscribe_pnl_updates()
            .await
            .map_err(|e| RithmicError::Connection(format!("PnL subscribe failed: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Connection(format!(
                "PnL subscribe error for account {}: {e}",
                account.account_id
            )));
        }

        let event_tx = self.pnl_tx.clone();
        let execution_tx = self.execution_tx.clone();
        let connection_state = Arc::clone(&self.connection_state);
        let account_id = account.account_id.clone();

        let task = get_runtime().spawn(async move {
            pnl_processor(handle, event_tx, execution_tx, connection_state).await;
        });

        self.pnl_processor_accounts.insert(account_id, ());
        self.task_handles.push(task);
        Ok(())
    }

    /// Returns the history plant handle if connected.
    ///
    /// This handle can be used for request/response operations like historical data queries.
    pub fn history_handle(&self) -> Option<&RithmicHistoryPlantHandle> {
        self.history_handle.as_ref()
    }

    /// Returns a mutable reference to the history plant handle if connected.
    pub fn history_handle_mut(&mut self) -> Option<&mut RithmicHistoryPlantHandle> {
        self.history_handle.as_mut()
    }

    /// Creates a new market data event subscription.
    pub fn subscribe_market_data_events(&self) -> broadcast::Receiver<MarketDataEvent> {
        self.market_data_tx.subscribe()
    }

    /// Creates a new execution event subscription.
    pub fn subscribe_execution_events(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.execution_tx.subscribe()
    }

    /// Convenience helper: pipe execution events from the gateway to a
    /// `RithmicExecutionClient`, keeping local order state in sync.
    pub fn pipe_execution_events(
        &self,
        client: Arc<RithmicExecutionClient>,
    ) -> Result<JoinHandle<()>> {
        let rx = self.subscribe_execution_events();

        Ok(client.spawn_event_pump(rx))
    }

    /// Creates a new PnL event subscription.
    pub fn subscribe_pnl_events(&self) -> broadcast::Receiver<PnlEvent> {
        self.pnl_tx.subscribe()
    }

    /// Ensures instrument info is cached before subscribing.
    ///
    /// If the instrument is not in the cache, fetches reference data from Rithmic
    /// and populates the cache with tick size information for precision calculations.
    async fn ensure_instrument_info(&self, symbol: &str, exchange: &str) -> Result<()> {
        let key = format!("{exchange}:{symbol}");

        // Check if already cached (read lock)
        {
            let instruments = self.instruments.read().await;

            if instruments.get(&key).is_some() {
                return Ok(());
            }
        }

        let handle = self
            .ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        let response = handle
            .get_reference_data(symbol, exchange)
            .await
            .map_err(|e| RithmicError::Api(format!("Reference data request failed: {e}")))?;

        if let Some(e) = &response.error {
            return Err(RithmicError::Instrument(format!(
                "Reference data error for {symbol}: {e}"
            )));
        }

        if let RithmicMessage::ResponseReferenceData(ref_data) = &response.message {
            let tick_size = ref_data.min_qprice_change;
            let point_value = ref_data.single_point_value;

            let is_tradeable = ref_data
                .is_tradable
                .as_ref()
                .is_none_or(|s| s.eq_ignore_ascii_case("true") || s == "1");

            let instrument_info = InstrumentInfo {
                symbol: symbol.to_string(),
                exchange: exchange.to_string(),
                tick_size,
                point_value,
                product_code: ref_data.product_code.clone(),
                description: ref_data.symbol_name.clone(),
                currency: ref_data.currency.clone(),
                is_tradeable,
            };

            // Write to cache (write lock)
            let mut instruments = self.instruments.write().await;
            instruments.insert(key, instrument_info);
            tracing::debug!("Cached instrument info for {symbol} on {exchange}");
        }

        Ok(())
    }

    /// Subscribes to market data (quotes and trades) for an instrument.
    ///
    /// This sends a subscription request to the ticker plant. After subscription,
    /// `BestBidOffer` and `LastTrade` messages will be received and transformed
    /// into `MarketDataEvent::Quote` and `MarketDataEvent::Trade` events.
    ///
    /// # Arguments
    /// * `symbol` - The instrument symbol (e.g., "ESZ4")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// `Ok(())` on successful subscription, or an error if not connected or subscription fails.
    pub async fn subscribe_market_data(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;

        let handle = self.ticker_query_handle()?;

        let response = handle
            .subscribe(symbol, exchange)
            .await
            .map_err(|e| RithmicError::Connection(format!("Subscription failed: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Connection(format!(
                "Subscription rejected: {e} (source={})",
                response.source
            )));
        }

        tracing::debug!("Subscribed to market data for {symbol} on {exchange}");
        Ok(())
    }

    fn ticker_query_handle(&self) -> Result<&RithmicTickerPlantHandle> {
        self.ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))
    }

    fn handle_ticker_subscription_response(
        response: std::result::Result<RithmicResponse, RsRithmicError>,
        failure_context: &str,
        rejection_context: &str,
    ) -> Result<()> {
        let response =
            response.map_err(|e| RithmicError::Connection(format!("{failure_context}: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Connection(format!(
                "{rejection_context}: {e} (source={})",
                response.source
            )));
        }

        Ok(())
    }

    /// Subscribes to venue instrument-status (`MarketMode`) updates for an instrument.
    pub async fn subscribe_instrument_status(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_instrument_status(symbol, exchange).await,
            "Instrument status subscription failed",
            "Instrument status subscription rejected",
        )
    }

    /// Unsubscribes from venue instrument-status (`MarketMode`) updates for an instrument.
    pub async fn unsubscribe_instrument_status(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_instrument_status(symbol, exchange).await,
            "Instrument status unsubscribe failed",
            "Instrument status unsubscribe rejected",
        )
    }

    /// Subscribes to top-of-book depth (`OrderBook`) updates for an instrument.
    pub async fn subscribe_order_book_depth(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_order_book_summary(symbol, exchange).await,
            "Book depth subscription failed",
            "Book depth subscription rejected",
        )
    }

    /// Unsubscribes from top-of-book depth (`OrderBook`) updates for an instrument.
    pub async fn unsubscribe_order_book_depth(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle
                .unsubscribe_order_book_summary(symbol, exchange)
                .await,
            "Book depth unsubscribe failed",
            "Book depth unsubscribe rejected",
        )
    }

    /// Subscribes to trade statistics updates for an instrument.
    pub async fn subscribe_trade_statistics(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_session_prices(symbol, exchange).await,
            "Trade statistics subscription failed",
            "Trade statistics subscription rejected",
        )
    }

    /// Unsubscribes from trade statistics updates for an instrument.
    pub async fn unsubscribe_trade_statistics(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_session_prices(symbol, exchange).await,
            "Trade statistics unsubscribe failed",
            "Trade statistics unsubscribe rejected",
        )
    }

    /// Subscribes to quote statistics updates for an instrument.
    pub async fn subscribe_quote_statistics(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_quote_statistics(symbol, exchange).await,
            "Quote statistics subscription failed",
            "Quote statistics subscription rejected",
        )
    }

    /// Unsubscribes from quote statistics updates for an instrument.
    pub async fn unsubscribe_quote_statistics(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_quote_statistics(symbol, exchange).await,
            "Quote statistics unsubscribe failed",
            "Quote statistics unsubscribe rejected",
        )
    }

    /// Subscribes to indicator price updates for an instrument.
    pub async fn subscribe_indicator_prices(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_indicator_prices(symbol, exchange).await,
            "Indicator prices subscription failed",
            "Indicator prices subscription rejected",
        )
    }

    /// Unsubscribes from indicator price updates for an instrument.
    pub async fn unsubscribe_indicator_prices(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_indicator_prices(symbol, exchange).await,
            "Indicator prices unsubscribe failed",
            "Indicator prices unsubscribe rejected",
        )
    }

    /// Subscribes to open-interest updates for an instrument.
    pub async fn subscribe_open_interest(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_open_interest(symbol, exchange).await,
            "Open interest subscription failed",
            "Open interest subscription rejected",
        )
    }

    /// Unsubscribes from open-interest updates for an instrument.
    pub async fn unsubscribe_open_interest(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_open_interest(symbol, exchange).await,
            "Open interest unsubscribe failed",
            "Open interest unsubscribe rejected",
        )
    }

    /// Subscribes to end-of-day price updates for an instrument.
    pub async fn subscribe_end_of_day_prices(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_end_of_day_prices(symbol, exchange).await,
            "End-of-day prices subscription failed",
            "End-of-day prices subscription rejected",
        )
    }

    /// Unsubscribes from end-of-day price updates for an instrument.
    pub async fn unsubscribe_end_of_day_prices(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.unsubscribe_end_of_day_prices(symbol, exchange).await,
            "End-of-day prices unsubscribe failed",
            "End-of-day prices unsubscribe rejected",
        )
    }

    /// Subscribes to order price limit updates for an instrument.
    pub async fn subscribe_order_price_limits(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_order_price_limits(symbol, exchange).await,
            "Order price limits subscription failed",
            "Order price limits subscription rejected",
        )
    }

    /// Unsubscribes from order price limit updates for an instrument.
    pub async fn unsubscribe_order_price_limits(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle
                .unsubscribe_order_price_limits(symbol, exchange)
                .await,
            "Order price limits unsubscribe failed",
            "Order price limits unsubscribe rejected",
        )
    }

    /// Subscribes to symbol margin rate updates for an instrument.
    pub async fn subscribe_symbol_margin_rate(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle.subscribe_symbol_margin_rate(symbol, exchange).await,
            "Symbol margin rate subscription failed",
            "Symbol margin rate subscription rejected",
        )
    }

    /// Unsubscribes from symbol margin rate updates for an instrument.
    pub async fn unsubscribe_symbol_margin_rate(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;
        let handle = self.ticker_query_handle()?;

        Self::handle_ticker_subscription_response(
            handle
                .unsubscribe_symbol_margin_rate(symbol, exchange)
                .await,
            "Symbol margin rate unsubscribe failed",
            "Symbol margin rate unsubscribe rejected",
        )
    }

    /// Subscribes to order book depth for an instrument.
    ///
    /// # Arguments
    /// * `symbol` - The instrument symbol (e.g., "ESZ4")
    /// * `exchange` - The exchange code (e.g., "CME")
    pub async fn subscribe_order_book(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;

        let handle = self
            .ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        handle
            .subscribe_order_book(symbol, exchange)
            .await
            .map_err(|e| {
                RithmicError::Connection(format!("Order book subscription failed: {e}"))
            })?;

        tracing::debug!("Subscribed to order book for {symbol} on {exchange}");
        Ok(())
    }

    /// Subscribes to order-book deltas and replays a snapshot before publishing live updates.
    pub async fn subscribe_order_book_bootstrapped(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<()> {
        self.ensure_instrument_info(symbol, exchange).await?;

        let key = order_book_subscription_key(symbol, exchange);
        self.begin_order_book_bootstrap(&key).await;

        let subscribed = self.subscribe_order_book(symbol, exchange).await;
        if let Err(e) = subscribed {
            self.clear_order_book_bootstrap(&key).await;
            return Err(e);
        }

        let result = tokio::time::timeout(
            ORDER_BOOK_BOOTSTRAP_TIMEOUT,
            self.bootstrap_order_book_snapshot(symbol, exchange),
        )
        .await
        .map_err(|_| {
            RithmicError::Connection(format!(
                "Order book snapshot timed out for {symbol} on {exchange}"
            ))
        })
        .and_then(|result| result);

        if result.is_err() {
            self.clear_order_book_bootstrap(&key).await;
            if let Err(e) = self.unsubscribe_order_book(symbol, exchange).await {
                tracing::warn!(
                    "Failed to roll back order-book subscription for {symbol} on {exchange}: {e}"
                );
            }
        }

        result
    }

    /// Requests a depth-by-order snapshot for an instrument.
    pub async fn request_order_book_snapshot(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>> {
        self.ensure_instrument_info(symbol, exchange).await?;

        let handle = self
            .ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        let responses = handle
            .request_depth_by_order_snapshot(symbol, exchange)
            .await
            .map_err(|e| {
                RithmicError::Connection(format!("Order book snapshot request failed: {e}"))
            })?;
        if let Some(e) = responses
            .iter()
            .find_map(|response| response.error.as_ref())
        {
            return Err(RithmicError::Api(format!(
                "Order book snapshot rejected for {exchange}:{symbol}: {e}"
            )));
        }
        if !responses.iter().any(|response| {
            matches!(
                response.message,
                RithmicMessage::ResponseDepthByOrderSnapshot(_)
            )
        }) {
            return Err(RithmicError::Api(format!(
                "Order book snapshot returned no snapshot rows for {exchange}:{symbol}"
            )));
        }
        Ok(responses)
    }

    /// Bootstraps the local order book by replaying a fresh snapshot into the market-data channel.
    pub async fn bootstrap_order_book_snapshot(&self, symbol: &str, exchange: &str) -> Result<()> {
        let responses = self.request_order_book_snapshot(symbol, exchange).await?;
        let snapshot_sequence = depth_snapshot_sequence(&responses);
        let instruments = self.instruments.read().await;

        let events = depth_snapshot_to_events(&responses, &instruments);
        if events.is_empty() {
            return Err(RithmicError::Api(format!(
                "Order book snapshot contained no valid rows for {exchange}:{symbol}"
            )));
        }
        for event in events {
            let _ = self.market_data_tx.send(event);
        }

        self.flush_buffered_order_book_deltas(
            &order_book_subscription_key(symbol, exchange),
            snapshot_sequence,
        )
        .await;
        Ok(())
    }

    /// Unsubscribes from market data (quotes and trades) for an instrument.
    ///
    /// # Arguments
    /// * `symbol` - The instrument symbol (e.g., "ESZ4")
    /// * `exchange` - The exchange code (e.g., "CME")
    pub async fn unsubscribe_market_data(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self
            .ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        handle
            .unsubscribe(symbol, exchange)
            .await
            .map_err(|e| RithmicError::Connection(format!("Unsubscribe failed: {e}")))?;

        tracing::debug!("Unsubscribed from market data for {symbol} on {exchange}");
        Ok(())
    }

    /// Unsubscribes from order book depth for an instrument.
    ///
    /// # Arguments
    /// * `symbol` - The instrument symbol (e.g., "ESZ4")
    /// * `exchange` - The exchange code (e.g., "CME")
    pub async fn unsubscribe_order_book(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self
            .ticker_query_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("Ticker plant not connected".to_string()))?;

        handle
            .unsubscribe_order_book(symbol, exchange)
            .await
            .map_err(|e| RithmicError::Connection(format!("Order book unsubscribe failed: {e}")))?;

        tracing::debug!("Unsubscribed from order book for {symbol} on {exchange}");
        Ok(())
    }

    // Historical data methods.

    /// Requests historical time bars from the history plant.
    ///
    /// The history plant must be enabled in the gateway configuration
    /// (`enable_history: true`) for this method to work.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The instrument symbol (e.g., "ESH5")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `bar_type` - The type of bar (SecondBar, MinuteBar, DailyBar, WeeklyBar)
    /// * `bar_period` - The period (e.g., 1 for 1-minute, 5 for 5-minute)
    /// * `start_time_sec` - Start time as Unix timestamp in seconds
    /// * `end_time_sec` - End time as Unix timestamp in seconds
    ///
    /// # Returns
    ///
    /// A vector of `RithmicResponse` containing the bar data.
    pub async fn request_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>> {
        let handle = self
            .history_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("History plant not connected".to_string()))?;

        let mut responses = handle
            .load_time_bars(
                symbol.to_string(),
                exchange.to_string(),
                bar_type,
                bar_period,
                start_time_sec,
                end_time_sec,
            )
            .await
            .map_err(|e| RithmicError::Api(format!("Bar request failed: {e}")))?;

        let mut resume_attempts = 0usize;
        while should_resume_time_bar_history(
            responses.iter().map(|response| &response.message),
            bar_type,
            bar_period,
            end_time_sec,
        ) {
            if resume_attempts >= MAX_HISTORY_RESUME_ATTEMPTS {
                tracing::warn!(
                    symbol,
                    exchange,
                    bar_period,
                    attempts = resume_attempts,
                    "Stopping historical bar resume loop after hitting retry limit"
                );
                break;
            }

            let previous_last_marker =
                last_time_bar_marker(responses.iter().map(|response| &response.message));
            let Some(request_key) = responses
                .iter()
                .rev()
                .find_map(|response| time_bar_replay_request_key(&response.message))
                .map(str::to_owned)
            else {
                tracing::warn!(
                    symbol,
                    exchange,
                    bar_period,
                    "Historical bar request appears truncated but no request_key was returned"
                );
                break;
            };

            let resumed = handle
                .resume_bars(request_key.clone())
                .await
                .map_err(|e| RithmicError::Api(format!("Bar resume failed: {e}")))?;

            if resumed.is_empty() {
                tracing::warn!(
                    symbol,
                    exchange,
                    bar_period,
                    request_key,
                    "Historical bar resume returned no additional data"
                );
                break;
            }

            let resumed_last_marker =
                last_time_bar_marker(resumed.iter().map(|response| &response.message));
            responses.extend(resumed);
            resume_attempts += 1;

            if let (Some(previous_last_marker), Some(resumed_last_marker)) =
                (previous_last_marker, resumed_last_marker)
                && resumed_last_marker <= previous_last_marker
            {
                tracing::warn!(
                    symbol,
                    exchange,
                    bar_period,
                    request_key,
                    previous_last_marker,
                    resumed_last_marker,
                    "Historical bar resume made no timestamp progress"
                );
                break;
            }
        }

        tracing::debug!(
            responses = responses.len(),
            resume_attempts,
            symbol,
            exchange,
            "Received historical bar responses"
        );
        Ok(responses)
    }

    /// Subscribes to live time-bar updates on the history plant.
    pub async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
    ) -> Result<()> {
        let handle = self
            .history_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("History plant not connected".to_string()))?;

        let response = handle
            .subscribe_time_bar_updates(
                symbol,
                exchange,
                replay_bar_type_to_live(bar_type),
                bar_period,
                LiveTimeBarRequest::Subscribe,
            )
            .await
            .map_err(|e| RithmicError::Api(format!("Live bar subscription failed: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Api(format!(
                "Live bar subscription failed: {e}"
            )));
        }

        tracing::debug!(
            "Subscribed to live {:?} bars(period={}) for {} on {}",
            bar_type,
            bar_period,
            symbol,
            exchange
        );
        Ok(())
    }

    /// Unsubscribes from live time-bar updates on the history plant.
    pub async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_period: i32,
    ) -> Result<()> {
        let handle = self
            .history_handle
            .as_ref()
            .ok_or_else(|| RithmicError::Connection("History plant not connected".to_string()))?;

        let response = handle
            .subscribe_time_bar_updates(
                symbol,
                exchange,
                replay_bar_type_to_live(bar_type),
                bar_period,
                LiveTimeBarRequest::Unsubscribe,
            )
            .await
            .map_err(|e| RithmicError::Api(format!("Live bar unsubscribe failed: {e}")))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Api(format!(
                "Live bar unsubscribe failed: {e}"
            )));
        }

        tracing::debug!(
            "Unsubscribed from live {:?} bars(period={}) for {} on {}",
            bar_type,
            bar_period,
            symbol,
            exchange
        );
        Ok(())
    }

    /// Returns true if the history plant is connected.
    ///
    /// Use this to check before calling `request_bars`.
    pub fn has_history_plant(&self) -> bool {
        self.history_handle.is_some()
    }

    #[must_use]
    pub(crate) fn has_ticker_plant(&self) -> bool {
        self.ticker_plant.is_some()
    }

    #[must_use]
    pub(crate) fn has_order_plant(&self) -> bool {
        self.order_plant.is_some()
    }

    #[must_use]
    pub(crate) fn has_pnl_plant(&self) -> bool {
        self.pnl_plant.is_some()
    }

    fn merge_requested_plants(&mut self, requested: &GatewayConfig) {
        self.config.enable_ticker |= requested.enable_ticker;
        self.config.enable_order |= requested.enable_order;
        self.config.enable_pnl |= requested.enable_pnl;
        self.config.enable_history |= requested.enable_history;
    }

    /// Ensures the gateway is connected with the requested plants enabled.
    pub async fn connect_requested(&mut self, requested: &GatewayConfig) -> Result<()> {
        let previous_enabled = (
            self.config.enable_ticker,
            self.config.enable_order,
            self.config.enable_pnl,
            self.config.enable_history,
        );
        self.merge_requested_plants(requested);

        if !self.is_connected() {
            let result = self.connect().await;
            if result.is_err() {
                (
                    self.config.enable_ticker,
                    self.config.enable_order,
                    self.config.enable_pnl,
                    self.config.enable_history,
                ) = previous_enabled;
            }
            return result;
        }

        let previous_plants = self.connected_plants();
        let result = async {
            let rithmic_config = self.config.to_rithmic_config()?;

            let ticker_handle = if self.config.enable_ticker && !self.has_ticker_plant() {
                Some(self.connect_ticker_plant(&rithmic_config).await?)
            } else {
                None
            };

            if self.config.enable_order && !self.has_order_plant() {
                self.connect_order_plant(&rithmic_config).await?;
            }

            if self.config.enable_pnl && !self.has_pnl_plant() {
                self.connect_pnl_plant(&rithmic_config).await?;
            }

            if self.config.enable_history && !self.has_history_plant() {
                self.connect_history_plant(&rithmic_config).await?;
            }

            Ok(ticker_handle)
        }
        .await;

        match result {
            Ok(ticker_handle) => {
                self.spawn_processors(ticker_handle, !previous_plants.3);
                Ok(())
            }
            Err(e) => {
                (
                    self.config.enable_ticker,
                    self.config.enable_order,
                    self.config.enable_pnl,
                    self.config.enable_history,
                ) = previous_enabled;
                self.rollback_new_plants(previous_plants).await;
                self.set_connection_state(ConnectionState::Connected);
                Err(e)
            }
        }
    }

    /// Connects to all enabled Rithmic plants.
    ///
    /// This method:
    /// 1. Connects to each enabled plant (ticker, order, pnl, history)
    /// 2. Authenticates with each plant
    /// 3. Spawns background processor tasks for message handling
    /// 4. Sets connection state to Connected on success
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        self.set_connection_state(ConnectionState::Connecting);
        tracing::info!("Connecting to Rithmic gateway...");

        let previous_plants = self.connected_plants();
        let result = async {
            let rithmic_config = self.config.to_rithmic_config()?;

            let ticker_handle = if self.config.enable_ticker && !self.has_ticker_plant() {
                Some(self.connect_ticker_plant(&rithmic_config).await?)
            } else {
                None
            };

            if self.config.enable_order && !self.has_order_plant() {
                self.connect_order_plant(&rithmic_config).await?;
            }

            if self.config.enable_pnl && !self.has_pnl_plant() {
                self.connect_pnl_plant(&rithmic_config).await?;
            }

            if self.config.enable_history && !self.has_history_plant() {
                tracing::info!("Connecting to history plant...");
                self.connect_history_plant(&rithmic_config).await?;
            } else if !self.config.enable_history {
                tracing::info!("Skipping history plant connection (enable_history=false)");
            }

            Ok(ticker_handle)
        }
        .await;

        let ticker_handle = match result {
            Ok(handle) => handle,
            Err(e) => {
                self.rollback_new_plants(previous_plants).await;
                self.set_connection_state(ConnectionState::Disconnected);
                return Err(e);
            }
        };

        self.spawn_processors(
            ticker_handle,
            self.config.enable_history && !previous_plants.3,
        );

        self.set_connection_state(ConnectionState::Connected);

        // Emit authenticated event per NautilusTrader convention
        let _ = self.market_data_tx.send(MarketDataEvent::Authenticated);
        let _ = self.execution_tx.send(ExecutionEvent::Authenticated);

        tracing::info!("Rithmic gateway connected successfully");

        Ok(())
    }

    /// Connects to the ticker plant and returns a handle for the processor.
    ///
    /// Also stores a separate query handle for request/response operations.
    async fn connect_ticker_plant(
        &mut self,
        config: &RithmicConfig,
    ) -> Result<RithmicTickerPlantHandle> {
        tracing::info!("Connecting to ticker plant...");

        let plant = RithmicTickerPlant::connect(config, ConnectStrategy::Simple)
            .await
            .map_err(|e| {
                RithmicError::Connection(format!("Ticker plant connection failed: {e}"))
            })?;

        // Get two handles: one for processor, one for queries
        let processor_handle = plant.get_handle();
        let query_handle = plant.get_handle();

        // Login to ticker plant (using processor handle, login state is shared)
        let response = processor_handle
            .login()
            .await
            .map_err(|e| RithmicError::Authentication(format!("Ticker plant login failed: {e}")))?;

        if let Some(e) = &response.error {
            return Err(RithmicError::Authentication(format!(
                "Ticker plant login error: {e} (source={})",
                response.source
            )));
        }

        tracing::debug!("Ticker plant connected and authenticated");

        self.ticker_plant = Some(plant);
        self.ticker_query_handle = Some(query_handle);
        Ok(processor_handle)
    }

    /// Connects to the order plant and returns a handle for the processor.
    async fn connect_order_plant(&mut self, config: &RithmicConfig) -> Result<()> {
        tracing::info!("Connecting to order plant...");
        tracing::debug!(
            system_name = %config.system_name,
            user = %config.user,
            "Order plant login context"
        );

        let plant = RithmicOrderPlant::connect(config, ConnectStrategy::Simple)
            .await
            .map_err(|e| RithmicError::Connection(format!("Order plant connection failed: {e}")))?;

        let login_account = self.config.to_rithmic_account();
        let login_handle = plant.get_handle(&login_account);

        // Login to order plant
        let response = login_handle
            .login()
            .await
            .map_err(|e| RithmicError::Authentication(format!("Order plant login failed: {e}")))?;

        if let Some(e) = &response.error {
            return Err(RithmicError::Authentication(format!(
                "Order plant login error: {e}"
            )));
        }

        // Account-scoped order subscriptions are established by execution clients.
        self.order_updates_available = false;

        tracing::debug!("Order plant connected and authenticated");

        // Store plant so we can create handles via order_handle()
        self.order_plant = Some(plant);
        Ok(())
    }

    /// Connects to the PnL plant and returns a handle for the processor.
    async fn connect_pnl_plant(&mut self, config: &RithmicConfig) -> Result<()> {
        tracing::info!("Connecting to PnL plant...");

        let plant = RithmicPnlPlant::connect(config, ConnectStrategy::Simple)
            .await
            .map_err(|e| RithmicError::Connection(format!("PnL plant connection failed: {e}")))?;

        let login_account = self.config.to_rithmic_account();
        let handle = plant.get_handle(&login_account);

        // Login to PnL plant
        let response = handle
            .login()
            .await
            .map_err(|e| RithmicError::Authentication(format!("PnL plant login failed: {e}")))?;

        if let Some(e) = &response.error {
            return Err(RithmicError::Authentication(format!(
                "PnL plant login error: {e}"
            )));
        }

        tracing::debug!("PnL plant connected and authenticated");

        self.pnl_plant = Some(plant);
        Ok(())
    }

    /// Connects to the history plant.
    async fn connect_history_plant(&mut self, config: &RithmicConfig) -> Result<()> {
        tracing::info!("Connecting to history plant...");

        let plant = RithmicHistoryPlant::connect(config, ConnectStrategy::Simple)
            .await
            .map_err(|e| {
                RithmicError::Connection(format!("History plant connection failed: {e}"))
            })?;

        let handle = plant.get_handle();

        // Login to history plant
        let response = handle.login().await.map_err(|e| {
            RithmicError::Authentication(format!("History plant login failed: {e}"))
        })?;

        if let Some(e) = &response.error {
            return Err(RithmicError::Authentication(format!(
                "History plant login error: {e}"
            )));
        }

        tracing::debug!("History plant connected and authenticated");

        self.history_plant = Some(plant);
        self.history_handle = Some(handle);

        Ok(())
    }

    /// Spawns background processor tasks for each connected plant.
    fn spawn_processors(
        &mut self,
        ticker_handle: Option<RithmicTickerPlantHandle>,
        spawn_history: bool,
    ) {
        // Spawn ticker processor

        if let Some(handle) = ticker_handle {
            let instruments = Arc::clone(&self.instruments);
            let order_book_bootstraps = Arc::clone(&self.order_book_bootstraps);
            let event_tx = self.market_data_tx.clone();
            let connection_state = Arc::clone(&self.connection_state);

            let task = get_runtime().spawn(async move {
                ticker_processor(
                    handle,
                    instruments,
                    order_book_bootstraps,
                    event_tx,
                    connection_state,
                )
                .await;
            });
            self.task_handles.push(task);
        }

        if spawn_history && let Some(handle) = self.history_handle.clone() {
            let event_tx = self.market_data_tx.clone();
            let connection_state = Arc::clone(&self.connection_state);
            let instruments = Arc::clone(&self.instruments);

            let task = get_runtime().spawn(async move {
                history_processor(handle, instruments, event_tx, connection_state).await;
            });
            self.task_handles.push(task);
        }
    }

    fn connected_plants(&self) -> (bool, bool, bool, bool) {
        (
            self.ticker_plant.is_some(),
            self.order_plant.is_some(),
            self.pnl_plant.is_some(),
            self.history_plant.is_some(),
        )
    }

    fn has_connection_resources(&self) -> bool {
        let (ticker, order, pnl, history) = self.connected_plants();
        ticker
            || order
            || pnl
            || history
            || self.ticker_query_handle.is_some()
            || self.history_handle.is_some()
            || !self.task_handles.is_empty()
    }

    /// Rolls back only plants added by the current connection transaction,
    /// preserving any plants which were already serving another shared lease.
    async fn rollback_new_plants(&mut self, previous: (bool, bool, bool, bool)) {
        let account = self.config.to_rithmic_account();

        if !previous.0
            && let Some(plant) = self.ticker_plant.take()
        {
            let handle = plant.get_handle();
            if !matches!(
                tokio::time::timeout(
                    Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                    handle.disconnect(),
                )
                .await,
                Ok(Ok(_))
            ) {
                handle.abort();
            }
            self.ticker_query_handle = None;
        }

        if !previous.1
            && let Some(plant) = self.order_plant.take()
        {
            let handle = plant.get_handle(&account);
            if !matches!(
                tokio::time::timeout(
                    Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                    handle.disconnect(),
                )
                .await,
                Ok(Ok(_))
            ) {
                handle.abort();
            }
            self.order_updates_available = false;
        }

        if !previous.2
            && let Some(plant) = self.pnl_plant.take()
        {
            let handle = plant.get_handle(&account);
            if !matches!(
                tokio::time::timeout(
                    Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                    handle.disconnect(),
                )
                .await,
                Ok(Ok(_))
            ) {
                handle.abort();
            }
        }

        if !previous.3
            && let Some(plant) = self.history_plant.take()
        {
            let handle = plant.get_handle();
            if !matches!(
                tokio::time::timeout(
                    Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                    handle.disconnect(),
                )
                .await,
                Ok(Ok(_))
            ) {
                handle.abort();
            }
            self.history_handle = None;
        }

        self.order_book_bootstraps.write().await.clear();
    }

    /// Disconnects from all Rithmic plants.
    ///
    /// This method:
    /// 1. Signals all processor tasks to stop
    /// 2. Disconnects from each plant
    /// 3. Cleans up resources
    pub async fn disconnect(&mut self) -> Result<()> {
        if !self.has_connection_resources() {
            self.set_connection_state(ConnectionState::Disconnected);
            return Ok(());
        }

        tracing::info!("Disconnecting from Rithmic gateway...");

        // Abort and drain all processor tasks (they own the streaming handles).
        let handles = self.task_handles.drain(..).collect::<Vec<_>>();
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }

        self.order_book_bootstraps.write().await.clear();
        self.order_processor_accounts.clear();
        self.pnl_processor_accounts.clear();

        // Get fresh handles from plants and disconnect.
        // The processor tasks owned the streaming handles, but we can get new handles
        // from the plants to call disconnect and close the connections cleanly.
        let account = self.config.to_rithmic_account();

        if let Some(plant) = self.ticker_plant.take() {
            let handle = plant.get_handle();

            match tokio::time::timeout(
                Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                handle.disconnect(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Error disconnecting ticker plant: {e}; aborting plant");
                    handle.abort();
                }
                Err(_) => {
                    tracing::warn!("Timed out disconnecting ticker plant; aborting plant");
                    handle.abort();
                }
            }
        }

        if let Some(plant) = self.order_plant.take() {
            let handle = plant.get_handle(&account);

            match tokio::time::timeout(
                Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                handle.disconnect(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Error disconnecting order plant: {e}; aborting plant");
                    handle.abort();
                }
                Err(_) => {
                    tracing::warn!("Timed out disconnecting order plant; aborting plant");
                    handle.abort();
                }
            }
        }

        if let Some(plant) = self.pnl_plant.take() {
            let handle = plant.get_handle(&account);

            match tokio::time::timeout(
                Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                handle.disconnect(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Error disconnecting PnL plant: {e}; aborting plant");
                    handle.abort();
                }
                Err(_) => {
                    tracing::warn!("Timed out disconnecting PnL plant; aborting plant");
                    handle.abort();
                }
            }
        }

        if let Some(plant) = self.history_plant.take() {
            let handle = plant.get_handle();

            match tokio::time::timeout(
                Duration::from_secs(DISCONNECT_TIMEOUT_SECS),
                handle.disconnect(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Error disconnecting history plant: {e}; aborting plant");
                    handle.abort();
                }
                Err(_) => {
                    tracing::warn!("Timed out disconnecting history plant; aborting plant");
                    handle.abort();
                }
            }
        }

        // Also clear the stored query handles
        self.ticker_query_handle = None;
        self.history_handle = None;

        self.set_connection_state(ConnectionState::Disconnected);
        tracing::info!("Rithmic gateway disconnected");

        Ok(())
    }

    /// Attempts to reconnect to Rithmic with exponential backoff.
    pub async fn reconnect(&mut self) -> Result<()> {
        self.set_connection_state(ConnectionState::Reconnecting);
        tracing::info!("Attempting to reconnect to Rithmic...");

        let mut attempts = 0;
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        while attempts < MAX_RECONNECT_ATTEMPTS {
            attempts += 1;

            // Clean up existing connections

            if let Err(e) = self.disconnect().await {
                tracing::warn!("Error during disconnect for reconnect: {e}");
            }

            // Wait before retry
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

            // Attempt reconnection

            match self.connect().await {
                Ok(()) => {
                    // Emit reconnected event per NautilusTrader convention
                    let _ = self.market_data_tx.send(MarketDataEvent::Reconnected);
                    let _ = self.execution_tx.send(ExecutionEvent::Reconnected);

                    tracing::info!("Reconnection successful after {attempts} attempts");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("Reconnection attempt {attempts} failed: {e}");
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                }
            }
        }

        self.set_connection_state(ConnectionState::Error);
        Err(RithmicError::Connection(format!(
            "Reconnection failed after {MAX_RECONNECT_ATTEMPTS} attempts"
        )))
    }

    /// Reconnects only when the gateway is not already back in a connected state.
    pub async fn reconnect_if_needed(&mut self) -> Result<()> {
        match self.connection_state() {
            ConnectionState::Connected | ConnectionState::Connecting => Ok(()),
            ConnectionState::Reconnecting
            | ConnectionState::Disconnected
            | ConnectionState::Error => self.reconnect().await,
        }
    }

    /// Sets the connection state and emits state change events.
    fn set_connection_state(&self, state: ConnectionState) {
        self.connection_state.store(Arc::new(state));

        // Emit state change to all channels
        let _ = self
            .market_data_tx
            .send(MarketDataEvent::ConnectionState(state));
        let _ = self
            .execution_tx
            .send(ExecutionEvent::ConnectionState(state));
    }

    async fn begin_order_book_bootstrap(&self, key: &str) {
        self.order_book_bootstraps
            .write()
            .await
            .insert(key.to_string(), PendingOrderBookBootstrap::default());
    }

    async fn clear_order_book_bootstrap(&self, key: &str) {
        self.order_book_bootstraps.write().await.remove(key);
    }

    async fn flush_buffered_order_book_deltas(&self, key: &str, snapshot_sequence: Option<u64>) {
        loop {
            let pending = {
                let mut bootstraps = self.order_book_bootstraps.write().await;
                let should_remove = bootstraps
                    .get(key)
                    .is_some_and(|state| state.live_deltas.is_empty());

                if should_remove {
                    bootstraps.remove(key);
                    Vec::new()
                } else if let Some(state) = bootstraps.get_mut(key) {
                    std::mem::take(&mut state.live_deltas)
                } else {
                    Vec::new()
                }
            };

            if pending.is_empty() {
                break;
            }

            for delta in pending {
                if snapshot_sequence.is_none_or(|sequence| delta.sequence > sequence) {
                    let _ = self.market_data_tx.send(MarketDataEvent::BookDelta(delta));
                }
            }
        }
    }
}

impl Debug for RithmicGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicGateway))
            .field("environment", &self.config.environment)
            .field("connection_state", &self.connection_state())
            .field("ticker_enabled", &self.config.enable_ticker)
            .field("order_enabled", &self.config.enable_order)
            .field("pnl_enabled", &self.config.enable_pnl)
            .field("history_enabled", &self.config.enable_history)
            .field(
                "instruments_count",
                &self.instruments.try_read().map_or(0, |m| m.len()),
            )
            .finish()
    }
}

impl Drop for RithmicGateway {
    fn drop(&mut self) {
        // Abort all tasks on drop

        for handle in self.task_handles.drain(..) {
            handle.abort();
        }
    }
}

// Processor tasks.

/// Background task that processes ticker plant messages.
///
/// This task receives messages from the ticker plant's subscription receiver
/// and transforms them into `MarketDataEvent` types for downstream consumers.
async fn ticker_processor(
    mut handle: RithmicTickerPlantHandle,
    instruments: Arc<RwLock<AHashMap<String, InstrumentInfo>>>,
    order_book_bootstraps: Arc<RwLock<AHashMap<String, PendingOrderBookBootstrap>>>,
    event_tx: broadcast::Sender<MarketDataEvent>,
    connection_state: Arc<ArcSwap<ConnectionState>>,
) {
    tracing::debug!("Ticker processor started");
    // Cache only lives for the lifetime of the active ticker task, so it is
    // dropped on disconnect/reconnect and cannot leak stale quotes across sessions.
    let mut quote_state: AHashMap<String, crate::data::QuoteTick> = AHashMap::new();

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(response) => {
                // Check for connection issues

                if is_connection_issue(&response) {
                    tracing::warn!("Ticker plant connection issue detected");
                    quote_state.clear();
                    notify_market_data_reconnecting(&event_tx, &connection_state);
                    break;
                }

                // Check for errors

                if let Some(e) = &response.error {
                    tracing::warn!("Ticker plant error: {e}");
                    let _ = event_tx.send(MarketDataEvent::Error(e.to_string()));
                    continue;
                }

                // Transform market data messages
                let events = {
                    let instruments_guard = instruments.read().await;
                    transform_market_data(&response, &instruments_guard, &mut quote_state)
                };
                let events = if let Some(key) = live_order_book_key(&response.message) {
                    match buffer_live_order_book_events(&order_book_bootstraps, &key, events).await
                    {
                        OrderBookBufferResult::Publish(events) => events,
                        OrderBookBufferResult::Buffered => continue,
                        OrderBookBufferResult::RecoveryRequired => {
                            tracing::warn!(
                                "Order-book bootstrap exceeded its bounds for {key}; reconnecting"
                            );
                            let _ = event_tx.send(MarketDataEvent::Error(format!(
                                "Order-book bootstrap bounds exceeded for {key}"
                            )));
                            notify_market_data_reconnecting(&event_tx, &connection_state);
                            break;
                        }
                    }
                } else {
                    events
                };

                for event in events {
                    let _ = event_tx.send(event);
                }
            }
            Err(e) => {
                // Channel closed or lagged
                tracing::error!("Ticker processor receive error: {e}");
                quote_state.clear();
                notify_market_data_reconnecting(&event_tx, &connection_state);
                break;
            }
        }
    }

    tracing::debug!("Ticker processor stopped");
}

/// Background task that processes history-plant subscription updates.
async fn history_processor(
    mut handle: RithmicHistoryPlantHandle,
    instruments: Arc<RwLock<AHashMap<String, InstrumentInfo>>>,
    event_tx: broadcast::Sender<MarketDataEvent>,
    connection_state: Arc<ArcSwap<ConnectionState>>,
) {
    tracing::debug!("History processor started");

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(response) => {
                if is_connection_issue(&response) {
                    tracing::warn!("History plant connection issue detected");
                    notify_market_data_reconnecting(&event_tx, &connection_state);
                    break;
                }

                if let Some(e) = &response.error {
                    tracing::warn!("History plant error: {e}");
                    let _ = event_tx.send(MarketDataEvent::Error(e.to_string()));
                    continue;
                }

                let instruments_guard = instruments.read().await;

                if let Some(event) = transform_history_market_data(&response, &instruments_guard) {
                    let _ = event_tx.send(event);
                }
            }
            Err(e) => {
                tracing::error!("History processor receive error: {e}");
                notify_market_data_reconnecting(&event_tx, &connection_state);
                break;
            }
        }
    }

    tracing::debug!("History processor stopped");
}

/// Background task that processes order plant messages.
async fn order_processor(
    mut handle: RithmicOrderPlantHandle,
    event_tx: broadcast::Sender<ExecutionEvent>,
    connection_state: Arc<ArcSwap<ConnectionState>>,
) {
    tracing::debug!("Order processor started");

    let handler = ExecutionHandler::new();

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(response) => {
                tracing::debug!(
                    request_id = %response.request_id,
                    source = %response.source,
                    is_update = response.is_update,
                    has_more = response.has_more,
                    multi_response = response.multi_response,
                    message_kind = ?std::mem::discriminant(&response.message),
                    "Order processor received response"
                );

                // Check for connection issues

                if is_connection_issue(&response) {
                    tracing::warn!("Order plant connection issue detected");
                    notify_execution_reconnecting(&event_tx, &connection_state);
                    break;
                }

                // Check for errors

                if let Some(e) = &response.error {
                    tracing::warn!("Order plant error: {e}");
                    let _ = event_tx.send(ExecutionEvent::Error(e.to_string()));
                    continue;
                }

                // Transform execution messages

                if let Some(event) = handler.handle_response(&response) {
                    tracing::debug!(?event, "Order processor emitting mapped execution event");
                    let _ = event_tx.send(event);
                } else {
                    tracing::debug!("Order processor ignored response after execution mapping");
                }
            }
            Err(e) => {
                tracing::error!("Order processor receive error: {e}");
                notify_execution_reconnecting(&event_tx, &connection_state);
                break;
            }
        }
    }

    tracing::debug!("Order processor stopped");
}

/// Background task that processes PnL plant messages.
async fn pnl_processor(
    mut handle: RithmicPnlPlantHandle,
    event_tx: broadcast::Sender<PnlEvent>,
    execution_tx: broadcast::Sender<ExecutionEvent>,
    connection_state: Arc<ArcSwap<ConnectionState>>,
) {
    tracing::debug!("PnL processor started");

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(response) => {
                // Check for connection issues

                if is_connection_issue(&response) {
                    tracing::warn!("PnL plant connection issue detected");
                    notify_execution_reconnecting(&execution_tx, &connection_state);
                    break;
                }

                // Check for errors

                if let Some(e) = &response.error {
                    tracing::warn!("PnL plant error: {e}");
                    continue;
                }

                // Transform PnL messages
                // Note: Actual transformation will be implemented in Phase 5

                if let Some(event) = transform_pnl(&response) {
                    let _ = event_tx.send(event);
                }
            }
            Err(e) => {
                tracing::error!("PnL processor receive error: {e}");
                notify_execution_reconnecting(&execution_tx, &connection_state);
                break;
            }
        }
    }

    tracing::debug!("PnL processor stopped");
}

// Helper functions.

/// Checks if a response indicates a connection issue.
fn is_connection_issue(response: &RithmicResponse) -> bool {
    matches!(
        response.message,
        RithmicMessage::ConnectionError
            | RithmicMessage::HeartbeatTimeout
            | RithmicMessage::ForcedLogout(_)
    )
}

fn notify_market_data_reconnecting(
    event_tx: &broadcast::Sender<MarketDataEvent>,
    connection_state: &Arc<ArcSwap<ConnectionState>>,
) {
    connection_state.store(Arc::new(ConnectionState::Reconnecting));
    let _ = event_tx.send(MarketDataEvent::ConnectionState(
        ConnectionState::Reconnecting,
    ));
}

fn notify_execution_reconnecting(
    event_tx: &broadcast::Sender<ExecutionEvent>,
    connection_state: &Arc<ArcSwap<ConnectionState>>,
) {
    connection_state.store(Arc::new(ConnectionState::Reconnecting));
    let _ = event_tx.send(ExecutionEvent::ConnectionState(
        ConnectionState::Reconnecting,
    ));
}

#[inline]
fn order_book_subscription_key(symbol: &str, exchange: &str) -> String {
    format!("{exchange}:{symbol}")
}

#[inline]
fn live_order_book_key(message: &RithmicMessage) -> Option<String> {
    match message {
        RithmicMessage::DepthByOrder(depth) => {
            let symbol = depth.symbol.as_deref()?;
            let exchange = depth.exchange.as_deref()?;
            Some(order_book_subscription_key(symbol, exchange))
        }
        _ => None,
    }
}

async fn buffer_live_order_book_events(
    order_book_bootstraps: &Arc<RwLock<AHashMap<String, PendingOrderBookBootstrap>>>,
    key: &str,
    events: Vec<MarketDataEvent>,
) -> OrderBookBufferResult {
    let mut bootstraps = order_book_bootstraps.write().await;
    let Some(state) = bootstraps.get_mut(key) else {
        return OrderBookBufferResult::Publish(events);
    };

    let incoming = events
        .iter()
        .filter(|event| matches!(event, MarketDataEvent::BookDelta(_)))
        .count();
    if state.started_at.elapsed() >= ORDER_BOOK_BOOTSTRAP_TIMEOUT
        || state.live_deltas.len().saturating_add(incoming) > ORDER_BOOK_BOOTSTRAP_MAX_DELTAS
    {
        bootstraps.remove(key);
        return OrderBookBufferResult::RecoveryRequired;
    }

    for event in events {
        if let MarketDataEvent::BookDelta(delta) = event {
            state.live_deltas.push(delta);
        }
    }

    OrderBookBufferResult::Buffered
}

enum OrderBookBufferResult {
    Publish(Vec<MarketDataEvent>),
    Buffered,
    RecoveryRequired,
}

/// Converts Rithmic's ssboe (seconds since beginning of epoch) and usecs to Unix nanoseconds.
///
/// Rithmic timestamps use:
/// - `ssboe`: Seconds since Unix epoch (1970-01-01)
/// - `usecs`: Microseconds component
#[inline]
fn rithmic_timestamp_to_nanos(ssboe: Option<i32>, usecs: Option<i32>) -> Option<u64> {
    let secs = u64::try_from(ssboe?).ok()?;
    let micros = u64::try_from(usecs.unwrap_or_default()).ok()?;
    if micros >= 1_000_000 {
        return None;
    }

    secs.checked_mul(1_000_000_000)?
        .checked_add(micros.checked_mul(1_000)?)
}

/// Returns current time in Unix nanoseconds.
#[inline]
fn now_nanos() -> u64 {
    crate::common::converters::now_unix_nanos().as_u64()
}

#[inline]
fn replay_bar_type_to_live(bar_type: TimeBarType) -> LiveTimeBarType {
    match bar_type {
        TimeBarType::SecondBar => LiveTimeBarType::SecondBar,
        TimeBarType::MinuteBar => LiveTimeBarType::MinuteBar,
        TimeBarType::DailyBar => LiveTimeBarType::DailyBar,
        TimeBarType::WeeklyBar => LiveTimeBarType::WeeklyBar,
    }
}

#[inline]
fn live_bar_type_to_replay(value: i32) -> Option<TimeBarType> {
    match LiveTimeBarType::try_from(value).ok()? {
        LiveTimeBarType::SecondBar => Some(TimeBarType::SecondBar),
        LiveTimeBarType::MinuteBar => Some(TimeBarType::MinuteBar),
        LiveTimeBarType::DailyBar => Some(TimeBarType::DailyBar),
        LiveTimeBarType::WeeklyBar => Some(TimeBarType::WeeklyBar),
    }
}

#[inline]
fn time_bar_timestamp_to_nanos(marker: Option<i32>) -> Option<u64> {
    u64::try_from(marker.filter(|value| *value > 0)?)
        .ok()?
        .checked_mul(1_000_000_000)
}

#[inline]
fn tick_bar_marker(data_bar_ssboe: &[i32]) -> Option<i64> {
    data_bar_ssboe.last().copied().map(i64::from)
}

#[inline]
fn tick_bar_timestamp_to_nanos(data_bar_ssboe: &[i32], data_bar_usecs: &[i32]) -> Option<u64> {
    rithmic_timestamp_to_nanos(
        data_bar_ssboe.last().copied(),
        data_bar_usecs.last().copied(),
    )
}

#[inline]
fn live_tick_bar_type_to_bar_type(value: i32) -> Option<RithmicBarType> {
    match rithmic_rs::rti::tick_bar::BarType::try_from(value).ok()? {
        rithmic_rs::rti::tick_bar::BarType::TickBar => Some(RithmicBarType::TickBar),
        rithmic_rs::rti::tick_bar::BarType::RangeBar => None,
        rithmic_rs::rti::tick_bar::BarType::VolumeBar => None,
    }
}

#[inline]
fn bbo_side_updated(bits: Option<u32>, bit: u32, price: Option<f64>, size: Option<i32>) -> bool {
    match bits {
        Some(bits) => bits & bit != 0,
        None => price.is_some_and(|value| value > 0.0) || size.is_some_and(|value| value > 0),
    }
}

#[inline]
fn quote_is_complete(quote: &crate::data::QuoteTick) -> bool {
    quote.bid_price > 0.0 && quote.ask_price > 0.0 && quote.bid_size > 0.0 && quote.ask_size > 0.0
}

#[inline]
fn get_precisions(instruments: &AHashMap<String, InstrumentInfo>, key: &str) -> Option<(u8, u8)> {
    let tick_size = instruments.get(key)?.tick_size?;
    if !tick_size.is_finite() || tick_size <= 0.0 {
        return None;
    }

    let price_precision = tick_size_to_precision(tick_size).ok()?;
    Price::new_checked(tick_size, price_precision).ok()?;
    // Rithmic depth and trade sizes are integer contract counts.
    Some((price_precision, 0))
}

fn checked_market_price(value: f64, precision: u8) -> Option<f64> {
    Price::new_checked(value, precision).ok()?;
    Some(value)
}

fn checked_market_quantity(value: u64, precision: u8) -> Option<f64> {
    Quantity::from_mantissa_exponent_checked(value, 0, precision)
        .ok()
        .map(|quantity| quantity.as_f64())
}

/// Transforms a ticker plant response to a market data event.
fn transform_market_data(
    response: &RithmicResponse,
    instruments: &AHashMap<String, InstrumentInfo>,
    quote_state: &mut AHashMap<String, crate::data::QuoteTick>,
) -> Vec<MarketDataEvent> {
    transform_market_data_message(&response.message, instruments, quote_state)
}

fn transform_history_market_data(
    response: &RithmicResponse,
    instruments: &AHashMap<String, InstrumentInfo>,
) -> Option<MarketDataEvent> {
    transform_history_market_data_message(&response.message, instruments)
}

fn transform_history_market_data_message(
    message: &RithmicMessage,
    instruments: &AHashMap<String, InstrumentInfo>,
) -> Option<MarketDataEvent> {
    match message {
        RithmicMessage::TimeBar(bar) => {
            let symbol = bar.symbol.as_ref()?;
            let exchange = bar.exchange.as_ref()?;
            let key = format!("{exchange}:{symbol}");
            let bar_type = RithmicBarType::from(live_bar_type_to_replay(bar.r#type?)?);
            let bar_period = bar.period.as_ref()?.parse::<i32>().ok()?;
            let ts_event = time_bar_timestamp_to_nanos(bar.marker)?;
            let ts_init = now_nanos();
            let (price_precision, size_precision) = get_precisions(instruments, &key)?;
            let open_price = checked_market_price(bar.open_price?, price_precision)?;
            let high_price = checked_market_price(bar.high_price?, price_precision)?;
            let low_price = checked_market_price(bar.low_price?, price_precision)?;
            let close_price = checked_market_price(bar.close_price?, price_precision)?;
            let volume = checked_market_quantity(bar.volume?, size_precision)?;

            Some(MarketDataEvent::Bar(crate::data::TimeBar {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                bar_type,
                bar_period,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                price_precision,
                size_precision,
                marker: bar.marker.map(i64::from),
                ts_event,
                ts_init,
            }))
        }
        RithmicMessage::TickBar(bar) => {
            let symbol = bar.symbol.as_ref()?;
            let exchange = bar.exchange.as_ref()?;
            let key = format!("{exchange}:{symbol}");
            let bar_type = live_tick_bar_type_to_bar_type(bar.r#type?)?;
            let bar_period = bar.type_specifier.as_deref()?.parse::<i32>().ok()?;
            let ts_event = tick_bar_timestamp_to_nanos(&bar.data_bar_ssboe, &bar.data_bar_usecs)?;
            let ts_init = now_nanos();
            let (price_precision, size_precision) = get_precisions(instruments, &key)?;
            let open_price = checked_market_price(bar.open_price?, price_precision)?;
            let high_price = checked_market_price(bar.high_price?, price_precision)?;
            let low_price = checked_market_price(bar.low_price?, price_precision)?;
            let close_price = checked_market_price(bar.close_price?, price_precision)?;
            let volume = checked_market_quantity(bar.volume?, size_precision)?;

            Some(MarketDataEvent::Bar(crate::data::TimeBar {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                bar_type,
                bar_period,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                price_precision,
                size_precision,
                marker: tick_bar_marker(&bar.data_bar_ssboe),
                ts_event,
                ts_init,
            }))
        }
        RithmicMessage::ForcedLogout(_) => {
            tracing::warn!("Forced logout from history plant");
            Some(MarketDataEvent::Error("Forced logout".to_string()))
        }
        _ => None,
    }
}

fn transform_market_data_message(
    message: &RithmicMessage,
    instruments: &AHashMap<String, InstrumentInfo>,
    quote_state: &mut AHashMap<String, crate::data::QuoteTick>,
) -> Vec<MarketDataEvent> {
    use crate::data::{BookDelta, QuoteTick, TradeTick};

    match message {
        RithmicMessage::BestBidOffer(bbo) => {
            use rithmic_rs::rti::best_bid_offer::PresenceBits;

            let (symbol, exchange) = match (bbo.symbol.as_ref(), bbo.exchange.as_ref()) {
                (Some(s), Some(e)) => (s, e),
                _ => return vec![],
            };

            let bid_updated = bbo_side_updated(
                bbo.presence_bits,
                PresenceBits::Bid as u32,
                bbo.bid_price,
                bbo.bid_size,
            );
            let ask_updated = bbo_side_updated(
                bbo.presence_bits,
                PresenceBits::Ask as u32,
                bbo.ask_price,
                bbo.ask_size,
            );
            let bid_cleared = bbo
                .clear_bits
                .is_some_and(|bits| bits & PresenceBits::Bid as u32 != 0);
            let ask_cleared = bbo
                .clear_bits
                .is_some_and(|bits| bits & PresenceBits::Ask as u32 != 0);

            if !bid_updated && !ask_updated && !bid_cleared && !ask_cleared {
                return vec![];
            }

            let Some(ts_event) = rithmic_timestamp_to_nanos(bbo.ssboe, bbo.usecs) else {
                return vec![];
            };
            let ts_init = now_nanos();
            let key = format!("{exchange}:{symbol}");
            let prior = quote_state.get(&key);

            let Some((price_precision, size_precision)) = get_precisions(instruments, &key) else {
                tracing::debug!("Dropping BBO before instrument precision is known: {key}");
                return vec![];
            };

            // A zero component is an internal sentinel for a side not received
            // yet. Incomplete state is cached but never published.
            let quote = QuoteTick {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                bid_price: if bid_cleared {
                    0.0
                } else if bid_updated {
                    bbo.bid_price
                        .or_else(|| prior.as_ref().map(|quote| quote.bid_price))
                        .unwrap_or(0.0)
                } else {
                    prior.as_ref().map_or(0.0, |quote| quote.bid_price)
                },
                ask_price: if ask_cleared {
                    0.0
                } else if ask_updated {
                    bbo.ask_price
                        .or_else(|| prior.as_ref().map(|quote| quote.ask_price))
                        .unwrap_or(0.0)
                } else {
                    prior.as_ref().map_or(0.0, |quote| quote.ask_price)
                },
                bid_size: if bid_cleared {
                    0.0
                } else if bid_updated {
                    bbo.bid_size
                        .map(f64::from)
                        .or_else(|| prior.as_ref().map(|quote| quote.bid_size))
                        .unwrap_or(0.0)
                } else {
                    prior.as_ref().map_or(0.0, |quote| quote.bid_size)
                },
                ask_size: if ask_cleared {
                    0.0
                } else if ask_updated {
                    bbo.ask_size
                        .map(f64::from)
                        .or_else(|| prior.as_ref().map(|quote| quote.ask_size))
                        .unwrap_or(0.0)
                } else {
                    prior.as_ref().map_or(0.0, |quote| quote.ask_size)
                },
                price_precision,
                size_precision,
                ts_event,
                ts_init,
            };

            if (quote.bid_price != 0.0
                && checked_market_price(quote.bid_price, price_precision).is_none())
                || (quote.ask_price != 0.0
                    && checked_market_price(quote.ask_price, price_precision).is_none())
                || (quote.bid_size != 0.0
                    && Quantity::new_checked(quote.bid_size, size_precision).is_err())
                || (quote.ask_size != 0.0
                    && Quantity::new_checked(quote.ask_size, size_precision).is_err())
            {
                tracing::warn!("Dropping BBO with malformed numeric payload for {key}");
                return vec![];
            }

            quote_state.insert(key, quote.clone());

            if quote_is_complete(&quote) {
                vec![MarketDataEvent::Quote(quote)]
            } else {
                vec![]
            }
        }
        RithmicMessage::LastTrade(trade) => {
            let (symbol, exchange) = match (trade.symbol.as_ref(), trade.exchange.as_ref()) {
                (Some(s), Some(e)) => (s, e),
                _ => return vec![],
            };
            let price = match trade.trade_price {
                Some(p) => p,
                None => return vec![],
            };
            let Some(size) = trade.trade_size else {
                return vec![];
            };

            // Skip trades with no price or non-positive size.
            if size <= 0 {
                return vec![];
            }

            // Determine aggressor side from transaction type
            // 1 = Buy (aggressor bought), 2 = Sell (aggressor sold)
            let aggressor_side = match trade.aggressor {
                Some(1) => "BUY",
                Some(2) => "SELL",
                _ => "UNKNOWN",
            };

            let Some(ts_event) = rithmic_timestamp_to_nanos(trade.ssboe, trade.usecs) else {
                return vec![];
            };
            let ts_init = now_nanos();
            let key = format!("{exchange}:{symbol}");
            let Some((price_precision, size_precision)) = get_precisions(instruments, &key) else {
                tracing::debug!("Dropping trade before instrument precision is known: {key}");
                return vec![];
            };
            let Some(price) = checked_market_price(price, price_precision) else {
                return vec![];
            };
            let Some(size) =
                checked_market_quantity(u64::from(size.cast_unsigned()), size_precision)
            else {
                return vec![];
            };

            vec![MarketDataEvent::Trade(TradeTick {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                price,
                size,
                aggressor_side: aggressor_side.to_string(),
                trade_id: trade
                    .exchange_order_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        trade
                            .aggressor_exchange_order_id
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                    })
                    .unwrap_or_default()
                    .to_string(),
                price_precision,
                size_precision,
                ts_event,
                ts_init,
            })]
        }
        RithmicMessage::OrderBook(book) => order_book_to_depth10(book, instruments)
            .into_iter()
            .map(|depth| MarketDataEvent::Depth10(Arc::new(depth)))
            .collect(),
        RithmicMessage::DepthByOrder(depth) => {
            use rithmic_rs::rti::depth_by_order::{TransactionType, UpdateType};

            let (symbol, exchange) = match (depth.symbol.as_ref(), depth.exchange.as_ref()) {
                (Some(s), Some(e)) => (s, e),
                _ => return vec![],
            };
            let key = format!("{exchange}:{symbol}");
            let Some((price_precision, size_precision)) = get_precisions(instruments, &key) else {
                tracing::debug!(
                    "Dropping depth update before instrument precision is known: {key}"
                );
                return vec![];
            };
            let Some(sequence) = depth.sequence_number else {
                return vec![];
            };
            let Some(ts_event) = rithmic_timestamp_to_nanos(depth.ssboe, depth.usecs) else {
                return vec![];
            };
            let ts_init = now_nanos();

            let n = depth.update_type.len();
            let mut events = Vec::with_capacity(n);

            for i in 0..n {
                let update_type = match UpdateType::try_from(depth.update_type[i]) {
                    Ok(value) => value,
                    _ => continue,
                };
                let action = match update_type {
                    UpdateType::New => "ADD",
                    UpdateType::Change => "UPDATE",
                    UpdateType::Delete => "REMOVE",
                };
                let side = match depth
                    .transaction_type
                    .get(i)
                    .copied()
                    .and_then(|v| TransactionType::try_from(v).ok())
                {
                    Some(TransactionType::Buy) => "BUY",
                    Some(TransactionType::Sell) => "SELL",
                    _ => continue,
                };
                // Rithmic encodes depth fields as independent repeated vectors.
                // A delete is keyed by exchange order ID/priority and can omit
                // numeric fields; Nautilus accepts zero price/size for that action.
                let price = match depth.depth_price.get(i).copied() {
                    Some(value) => match checked_market_price(value, price_precision) {
                        Some(value) => value,
                        None => continue,
                    },
                    None if update_type == UpdateType::Delete => 0.0,
                    None => continue,
                };
                let size = match depth.depth_size.get(i).copied() {
                    Some(value) => {
                        let Ok(value) = u64::try_from(value) else {
                            continue;
                        };
                        let Some(value) = checked_market_quantity(value, size_precision) else {
                            continue;
                        };
                        value
                    }
                    None if update_type == UpdateType::Delete => 0.0,
                    None => continue,
                };
                let exchange_order_id = depth
                    .exchange_order_id
                    .get(i)
                    .map(String::as_str)
                    .filter(|value| !value.is_empty());
                let priority = match depth.depth_order_priority.get(i).copied() {
                    Some(value) => value,
                    None if exchange_order_id.is_some() => 0,
                    None => continue,
                };
                let order_id = rithmic_depth_order_id(exchange_order_id, priority);

                events.push(MarketDataEvent::BookDelta(BookDelta {
                    symbol: symbol.clone(),
                    exchange: exchange.clone(),
                    action: action.to_string(),
                    side: side.to_string(),
                    price,
                    size,
                    order_id,
                    sequence,
                    flags: 0,
                    price_precision,
                    size_precision,
                    ts_event,
                    ts_init,
                }));
            }
            if let Some(MarketDataEvent::BookDelta(last)) = events.last_mut() {
                last.flags |= RecordFlag::F_LAST as u8;
            }
            events
        }
        RithmicMessage::MarketMode(mode) => market_mode_to_status(mode)
            .into_iter()
            .map(MarketDataEvent::InstrumentStatus)
            .collect(),
        RithmicMessage::TradeStatistics(stats) => trade_statistics_to_custom(stats)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::QuoteStatistics(stats) => quote_statistics_to_custom(stats)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::IndicatorPrices(prices) => indicator_prices_to_custom(prices)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::OpenInterest(open_interest) => open_interest_to_custom(open_interest)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::EndOfDayPrices(prices) => end_of_day_prices_to_custom(prices)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::OrderPriceLimits(limits) => order_price_limits_to_custom(limits)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::SymbolMarginRate(rate) => symbol_margin_rate_to_custom(rate)
            .into_iter()
            .map(MarketDataEvent::Custom)
            .collect(),
        RithmicMessage::ForcedLogout(_) => {
            tracing::warn!("Forced logout from ticker plant");
            vec![MarketDataEvent::Error("Forced logout".to_string())]
        }
        _ => vec![],
    }
}

fn rithmic_instrument_id(symbol: &str, exchange: &str) -> Option<InstrumentId> {
    match crate::common::converters::rithmic_instrument_id(symbol, exchange) {
        Ok(instrument_id) => Some(instrument_id),
        Err(e) => {
            tracing::warn!(
                "Dropping invalid Rithmic instrument identity {symbol:?}/{exchange:?}: {e}"
            );
            None
        }
    }
}

fn empty_depth_orders(side: OrderSide, precision: u8) -> [BookOrder; DEPTH10_LEN] {
    std::array::from_fn(|_| BookOrder::new(side, Price::zero(precision), Quantity::zero(0), 0))
}

fn order_book_to_depth10(
    book: &rithmic_rs::rti::OrderBook,
    instruments: &AHashMap<String, InstrumentInfo>,
) -> Option<OrderBookDepth10> {
    let (symbol, exchange) = match (book.symbol.as_ref(), book.exchange.as_ref()) {
        (Some(symbol), Some(exchange)) => (symbol, exchange),
        _ => return None,
    };

    let key = format!("{exchange}:{symbol}");
    let (price_precision, _) = get_precisions(instruments, &key)?;
    let instrument_id = rithmic_instrument_id(symbol, exchange)?;

    let mut bids = empty_depth_orders(OrderSide::Buy, price_precision);
    let mut asks = empty_depth_orders(OrderSide::Sell, price_precision);
    let mut bid_counts = [0_u32; DEPTH10_LEN];
    let mut ask_counts = [0_u32; DEPTH10_LEN];

    for (i, price) in book.bid_price.iter().take(DEPTH10_LEN).enumerate() {
        let Some(size) = book.bid_size.get(i).copied() else {
            tracing::warn!("Dropping bid depth with misaligned price/size vectors");
            return None;
        };

        if size <= 0 {
            continue;
        }
        let Some(count) = book.bid_orders.get(i).copied() else {
            tracing::warn!("Dropping bid depth with missing order count");
            return None;
        };
        let Ok(count) = u32::try_from(count) else {
            tracing::warn!("Dropping bid depth with negative order count");
            return None;
        };
        let price = Price::new_checked(*price, price_precision)
            .map_err(|e| tracing::warn!("Dropping malformed bid depth price: {e}"))
            .ok()?;
        let size = Quantity::from_mantissa_exponent_checked(u64::from(size.cast_unsigned()), 0, 0)
            .map_err(|e| tracing::warn!("Dropping malformed bid depth size: {e}"))
            .ok()?;
        bids[i] = BookOrder::new(OrderSide::Buy, price, size, 0);
        bid_counts[i] = count;
    }

    for (i, price) in book.ask_price.iter().take(DEPTH10_LEN).enumerate() {
        let Some(size) = book.ask_size.get(i).copied() else {
            tracing::warn!("Dropping ask depth with misaligned price/size vectors");
            return None;
        };

        if size <= 0 {
            continue;
        }
        let Some(count) = book.ask_orders.get(i).copied() else {
            tracing::warn!("Dropping ask depth with missing order count");
            return None;
        };
        let Ok(count) = u32::try_from(count) else {
            tracing::warn!("Dropping ask depth with negative order count");
            return None;
        };
        let price = Price::new_checked(*price, price_precision)
            .map_err(|e| tracing::warn!("Dropping malformed ask depth price: {e}"))
            .ok()?;
        let size = Quantity::from_mantissa_exponent_checked(u64::from(size.cast_unsigned()), 0, 0)
            .map_err(|e| tracing::warn!("Dropping malformed ask depth size: {e}"))
            .ok()?;
        asks[i] = BookOrder::new(OrderSide::Sell, price, size, 0);
        ask_counts[i] = count;
    }

    let mut flags = RecordFlag::F_MBP as u8;

    if matches!(
        book.update_type
            .and_then(|value| rithmic_rs::rti::order_book::UpdateType::try_from(value).ok()),
        Some(
            rithmic_rs::rti::order_book::UpdateType::SnapshotImage
                | rithmic_rs::rti::order_book::UpdateType::Begin
                | rithmic_rs::rti::order_book::UpdateType::Solo
        )
    ) {
        flags |= RecordFlag::F_SNAPSHOT as u8;
    }

    if matches!(
        book.update_type
            .and_then(|value| rithmic_rs::rti::order_book::UpdateType::try_from(value).ok()),
        Some(
            rithmic_rs::rti::order_book::UpdateType::End
                | rithmic_rs::rti::order_book::UpdateType::Solo
                | rithmic_rs::rti::order_book::UpdateType::SnapshotImage
        )
    ) {
        flags |= RecordFlag::F_LAST as u8;
    }

    Some(OrderBookDepth10::new(
        instrument_id,
        bids,
        asks,
        bid_counts,
        ask_counts,
        flags,
        0,
        rithmic_timestamp_to_nanos(book.ssboe, book.usecs)?.into(),
        now_nanos().into(),
    ))
}

fn market_mode_to_status(mode: &rithmic_rs::rti::MarketMode) -> Option<InstrumentStatus> {
    let symbol = mode.symbol.as_deref()?;
    let exchange = mode.exchange.as_deref()?;
    let market_mode = mode.market_mode.as_deref().unwrap_or_default();
    let action = match market_mode.to_ascii_uppercase().as_str() {
        "OPEN" | "OPENING" | "TRADING" | "NORMAL" => MarketStatusAction::Trading,
        "PREOPEN" | "PRE_OPEN" | "PRE-OPEN" => MarketStatusAction::PreOpen,
        "CLOSED" | "CLOSE" => MarketStatusAction::Close,
        "PAUSE" | "PAUSED" => MarketStatusAction::Pause,
        "HALTED" | "HALT" => MarketStatusAction::Halt,
        _ => MarketStatusAction::None,
    };

    let (is_trading, is_quoting) = if action == MarketStatusAction::None {
        (None, None)
    } else {
        (
            Some(matches!(action, MarketStatusAction::Trading)),
            Some(matches!(
                action,
                MarketStatusAction::Trading | MarketStatusAction::PreOpen
            )),
        )
    };

    Some(InstrumentStatus::new(
        rithmic_instrument_id(symbol, exchange)?,
        action,
        rithmic_timestamp_to_nanos(mode.ssboe, mode.usecs)?.into(),
        now_nanos().into(),
        mode.halt_reason.as_deref().map(Ustr::from),
        mode.trade_date.as_deref().map(Ustr::from),
        is_trading,
        is_quoting,
        None,
    ))
}

#[derive(Debug)]
struct InvalidCustomNumeric;

fn checked_optional_custom_numeric(
    value: Option<f64>,
) -> std::result::Result<Option<f64>, InvalidCustomNumeric> {
    match value {
        Some(value) => Price::new_checked(value, 9)
            .map(|_| Some(value))
            .map_err(|_| InvalidCustomNumeric),
        None => Ok(None),
    }
}

fn trade_statistics_to_custom(
    stats: &rithmic_rs::rti::TradeStatistics,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::TradeStatistics(RithmicTradeStatistics {
        instrument_id: rithmic_instrument_id(stats.symbol.as_deref()?, stats.exchange.as_deref()?)?,
        is_snapshot: stats.is_snapshot?,
        open_price: checked_optional_custom_numeric(stats.open_price).ok()?,
        high_price: checked_optional_custom_numeric(stats.high_price).ok()?,
        low_price: checked_optional_custom_numeric(stats.low_price).ok()?,
        ts_event: rithmic_timestamp_to_nanos(stats.ssboe, stats.usecs)?.into(),
        ts_init: now_nanos().into(),
    }))
}

fn quote_statistics_to_custom(
    stats: &rithmic_rs::rti::QuoteStatistics,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::QuoteStatistics(RithmicQuoteStatistics {
        instrument_id: rithmic_instrument_id(stats.symbol.as_deref()?, stats.exchange.as_deref()?)?,
        is_snapshot: stats.is_snapshot?,
        highest_bid_price: checked_optional_custom_numeric(stats.highest_bid_price).ok()?,
        lowest_ask_price: checked_optional_custom_numeric(stats.lowest_ask_price).ok()?,
        ts_event: rithmic_timestamp_to_nanos(stats.ssboe, stats.usecs)?.into(),
        ts_init: now_nanos().into(),
    }))
}

fn indicator_prices_to_custom(
    prices: &rithmic_rs::rti::IndicatorPrices,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::IndicatorPrices(RithmicIndicatorPrices {
        instrument_id: rithmic_instrument_id(
            prices.symbol.as_deref()?,
            prices.exchange.as_deref()?,
        )?,
        is_snapshot: prices.is_snapshot?,
        opening_indicator: checked_optional_custom_numeric(prices.opening_indicator).ok()?,
        closing_indicator: checked_optional_custom_numeric(prices.closing_indicator).ok()?,
        ts_event: rithmic_timestamp_to_nanos(prices.ssboe, prices.usecs)?.into(),
        ts_init: now_nanos().into(),
    }))
}

fn open_interest_to_custom(
    open_interest: &rithmic_rs::rti::OpenInterest,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::OpenInterest(RithmicOpenInterest {
        instrument_id: rithmic_instrument_id(
            open_interest.symbol.as_deref()?,
            open_interest.exchange.as_deref()?,
        )?,
        is_snapshot: open_interest.is_snapshot?,
        should_clear: open_interest.should_clear?,
        open_interest: open_interest.open_interest,
        ts_event: rithmic_timestamp_to_nanos(open_interest.ssboe, open_interest.usecs)?.into(),
        ts_init: now_nanos().into(),
    }))
}

fn end_of_day_prices_to_custom(
    prices: &rithmic_rs::rti::EndOfDayPrices,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::EndOfDayPrices(RithmicEndOfDayPrices {
        instrument_id: rithmic_instrument_id(
            prices.symbol.as_deref()?,
            prices.exchange.as_deref()?,
        )?,
        is_snapshot: prices.is_snapshot?,
        close_price: checked_optional_custom_numeric(prices.close_price).ok()?,
        close_date: prices.close_date.clone(),
        adjusted_close_price: checked_optional_custom_numeric(prices.adjusted_close_price).ok()?,
        settlement_price: checked_optional_custom_numeric(prices.settlement_price).ok()?,
        settlement_date: prices.settlement_date.clone(),
        settlement_price_type: prices.settlement_price_type.clone(),
        projected_settlement_price: checked_optional_custom_numeric(
            prices.projected_settlement_price,
        )
        .ok()?,
        ts_event: rithmic_timestamp_to_nanos(prices.ssboe, prices.usecs)?.into(),
        ts_init: now_nanos().into(),
    }))
}

fn order_price_limits_to_custom(
    limits: &rithmic_rs::rti::OrderPriceLimits,
) -> Option<RithmicCustomData> {
    Some(RithmicCustomData::OrderPriceLimits(
        RithmicOrderPriceLimits {
            instrument_id: rithmic_instrument_id(
                limits.symbol.as_deref()?,
                limits.exchange.as_deref()?,
            )?,
            is_snapshot: limits.is_snapshot?,
            high_price_limit: checked_optional_custom_numeric(limits.high_price_limit).ok()?,
            low_price_limit: checked_optional_custom_numeric(limits.low_price_limit).ok()?,
            ts_event: rithmic_timestamp_to_nanos(limits.ssboe, limits.usecs)?.into(),
            ts_init: now_nanos().into(),
        },
    ))
}

fn symbol_margin_rate_to_custom(
    rate: &rithmic_rs::rti::SymbolMarginRate,
) -> Option<RithmicCustomData> {
    // SymbolMarginRate carries no venue timestamp; use one receipt timestamp
    // consistently for both event and initialization time.
    let received_at = now_nanos();
    Some(RithmicCustomData::SymbolMarginRate(
        RithmicSymbolMarginRate {
            instrument_id: rithmic_instrument_id(
                rate.symbol.as_deref()?,
                rate.exchange.as_deref()?,
            )?,
            is_snapshot: rate.is_snapshot?,
            margin_rate: checked_optional_custom_numeric(rate.margin_rate).ok()?,
            ts_event: received_at.into(),
            ts_init: received_at.into(),
        },
    ))
}

fn depth_snapshot_to_events(
    responses: &[RithmicResponse],
    instruments: &AHashMap<String, InstrumentInfo>,
) -> Vec<MarketDataEvent> {
    let snapshot_rows: Vec<&rithmic_rs::rti::ResponseDepthByOrderSnapshot> = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseDepthByOrderSnapshot(snapshot) => Some(snapshot),
            _ => None,
        })
        .collect();

    depth_snapshot_rows_to_events(&snapshot_rows, instruments)
}

fn depth_snapshot_sequence(responses: &[RithmicResponse]) -> Option<u64> {
    responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseDepthByOrderSnapshot(snapshot) => snapshot.sequence_number,
            _ => None,
        })
        .max()
}

fn depth_snapshot_rows_to_events(
    snapshot_rows: &[&rithmic_rs::rti::ResponseDepthByOrderSnapshot],
    instruments: &AHashMap<String, InstrumentInfo>,
) -> Vec<MarketDataEvent> {
    use rithmic_rs::rti::response_depth_by_order_snapshot::TransactionType;

    let mut events = Vec::with_capacity(snapshot_rows.len());

    if snapshot_rows.is_empty() {
        return events;
    }

    let Some(first) = snapshot_rows.first() else {
        return events;
    };
    let (symbol, exchange) = match (first.symbol.as_ref(), first.exchange.as_ref()) {
        (Some(symbol), Some(exchange)) => (symbol.clone(), exchange.clone()),
        _ => return events,
    };
    let Some(sequence) = first.sequence_number else {
        return events;
    };
    let ts_event = now_nanos();
    let ts_init = now_nanos();
    let key = format!("{exchange}:{symbol}");
    let Some((price_precision, size_precision)) = get_precisions(instruments, &key) else {
        tracing::debug!("Dropping depth snapshot before instrument precision is known: {key}");
        return events;
    };

    events.push(MarketDataEvent::BookDelta(crate::data::BookDelta {
        symbol: symbol.clone(),
        exchange: exchange.clone(),
        action: "CLEAR".to_string(),
        side: "NONE".to_string(),
        price: 0.0,
        size: 0.0,
        order_id: 0,
        sequence,
        flags: RecordFlag::F_SNAPSHOT as u8,
        price_precision,
        size_precision,
        ts_event,
        ts_init,
    }));

    for snapshot in snapshot_rows {
        if snapshot.symbol.as_deref() != Some(symbol.as_str())
            || snapshot.exchange.as_deref() != Some(exchange.as_str())
        {
            tracing::warn!("Ignoring mismatched instrument row in Rithmic depth snapshot");
            continue;
        }
        let side = match snapshot
            .depth_side
            .and_then(|value| TransactionType::try_from(value).ok())
        {
            Some(TransactionType::Buy) => "BUY",
            Some(TransactionType::Sell) => "SELL",
            None => continue,
        };
        let Some(price) = snapshot
            .depth_price
            .and_then(|value| checked_market_price(value, price_precision))
        else {
            continue;
        };
        let Some(sequence) = snapshot.sequence_number else {
            continue;
        };

        for (i, depth_order_priority) in snapshot.depth_order_priority.iter().enumerate() {
            let Some(size) = snapshot.depth_size.get(i).copied() else {
                continue;
            };
            let Ok(size) = u64::try_from(size) else {
                continue;
            };
            let Some(size) = checked_market_quantity(size, size_precision) else {
                continue;
            };
            let order_id = rithmic_depth_order_id(
                snapshot.exchange_order_id.get(i).map(String::as_str),
                *depth_order_priority,
            );
            events.push(MarketDataEvent::BookDelta(crate::data::BookDelta {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
                action: "ADD".to_string(),
                side: side.to_string(),
                price,
                size,
                order_id,
                sequence,
                flags: RecordFlag::F_SNAPSHOT as u8,
                price_precision,
                size_precision,
                ts_event,
                ts_init,
            }));
        }
    }

    if let Some(MarketDataEvent::BookDelta(delta)) = events.last_mut() {
        delta.flags |= RecordFlag::F_LAST as u8;
    }

    events
}

/// Transforms a PnL plant response to a PnL event.
///
/// Handles `AccountPnLPositionUpdate` for account-level balance/margin updates
/// and `InstrumentPnLPositionUpdate` for per-instrument position updates.
fn transform_pnl(response: &RithmicResponse) -> Option<PnlEvent> {
    transform_pnl_message(&response.message)
}

fn transform_pnl_message(message: &RithmicMessage) -> Option<PnlEvent> {
    use crate::providers::{AccountBalance, Position};

    match message {
        RithmicMessage::AccountPnLPositionUpdate(update) => {
            let account_id = update.account_id.clone()?;

            // Parse string fields to f64 (Rithmic sends numeric values as strings)
            let total = parse_optional_f64(&update.account_balance)?;
            let available = parse_optional_f64(&update.cash_on_hand)?;
            let locked = parse_optional_f64(&update.margin_balance)?;
            let unrealized_pnl = parse_optional_f64(&update.open_position_pnl)?;
            let realized_pnl = parse_optional_f64(&update.closed_position_pnl)?;
            let is_snapshot = update.is_snapshot?;
            let ts_event = rithmic_timestamp_to_nanos(update.ssboe, update.usecs)?;

            tracing::debug!(
                "AccountPnLPositionUpdate: account={}, balance={}, available={}, margin={}",
                account_id,
                total,
                available,
                locked
            );

            Some(PnlEvent::Account(AccountEvent::BalanceUpdate(
                AccountBalance {
                    is_snapshot,
                    account_id,
                    currency: SUPPORTED_ACCOUNT_CURRENCY.to_string(),
                    total,
                    available,
                    locked,
                    unrealized_pnl,
                    realized_pnl,
                    ts_event,
                },
            )))
        }
        RithmicMessage::InstrumentPnLPositionUpdate(update) => {
            let account_id = update.account_id.clone()?;
            let symbol = update.symbol.clone()?;
            let exchange = update.exchange.clone()?;

            // Net position is buy_qty - sell_qty
            let quantity = f64::from(update.buy_qty?) - f64::from(update.sell_qty?);
            let avg_price = match update.avg_open_fill_price {
                Some(value) if value.is_finite() => value,
                None if quantity == 0.0 => 0.0,
                _ => return None,
            };

            // InstrumentPnLPositionUpdate has f64 for day_open_pnl/day_closed_pnl
            // but string for open_position_pnl/closed_position_pnl
            let unrealized_pnl = update
                .day_open_pnl
                .filter(|value| value.is_finite())
                .or_else(|| parse_optional_f64(&update.open_position_pnl))?;
            let realized_pnl = update
                .day_closed_pnl
                .filter(|value| value.is_finite())
                .or_else(|| parse_optional_f64(&update.closed_position_pnl))?;

            let is_snapshot = update.is_snapshot?;

            let ts_event = rithmic_timestamp_to_nanos(update.ssboe, update.usecs)?;

            tracing::debug!(
                "InstrumentPnLPositionUpdate: {}:{} qty={}, avg_price={}, pnl={}",
                exchange,
                symbol,
                quantity,
                avg_price,
                unrealized_pnl
            );

            Some(PnlEvent::Position(PositionEvent::Updated(Position {
                is_snapshot,
                account_id,
                symbol,
                exchange,
                quantity,
                avg_price,
                unrealized_pnl,
                realized_pnl,
                ts_event,
            })))
        }
        RithmicMessage::ForcedLogout(_) => {
            tracing::warn!("Forced logout from PnL plant");
            Some(PnlEvent::Account(AccountEvent::Error(
                "Forced logout".to_string(),
            )))
        }
        _ => None,
    }
}

/// Parses an optional string field to f64.
#[inline]
fn parse_optional_f64(s: &Option<String>) -> Option<f64> {
    s.as_ref()
        .and_then(|value| value.parse().ok())
        .filter(|value: &f64| value.is_finite())
}

#[cfg(test)]
#[allow(
    unsafe_code,
    reason = "test environment mutation is serialized by ENV_LOCK"
)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde::{Deserialize, de::DeserializeOwned};

    use super::*;
    use crate::config::RITHMIC_ENV_TEST_LOCK;

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
        fn into_message(self) -> rithmic_rs::rti::ResponseDepthByOrderSnapshot {
            rithmic_rs::rti::ResponseDepthByOrderSnapshot {
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
        fn into_message(self) -> rithmic_rs::rti::DepthByOrder {
            rithmic_rs::rti::DepthByOrder {
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

    #[derive(Debug, Deserialize)]
    struct CustomDataFixture {
        trade_statistics: TradeStatisticsFixture,
        quote_statistics: QuoteStatisticsFixture,
        indicator_prices: IndicatorPricesFixture,
        open_interest: OpenInterestFixture,
        end_of_day_prices: EndOfDayPricesFixture,
        order_price_limits: OrderPriceLimitsFixture,
        symbol_margin_rate: SymbolMarginRateFixture,
    }

    #[derive(Debug, Deserialize)]
    struct TradeStatisticsFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        presence_bits: u32,
        clear_bits: u32,
        is_snapshot: bool,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        ssboe: i32,
        usecs: i32,
        source_ssboe: Option<i32>,
        source_usecs: Option<i32>,
        source_nsecs: Option<i32>,
        jop_ssboe: Option<i32>,
        jop_nsecs: Option<i32>,
    }

    impl TradeStatisticsFixture {
        fn into_message(self) -> rithmic_rs::rti::TradeStatistics {
            rithmic_rs::rti::TradeStatistics {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                presence_bits: Some(self.presence_bits),
                clear_bits: Some(self.clear_bits),
                is_snapshot: Some(self.is_snapshot),
                open_price: Some(self.open_price),
                high_price: Some(self.high_price),
                low_price: Some(self.low_price),
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

    #[derive(Debug, Deserialize)]
    struct QuoteStatisticsFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        presence_bits: u32,
        clear_bits: u32,
        is_snapshot: bool,
        highest_bid_price: f64,
        lowest_ask_price: f64,
        ssboe: i32,
        usecs: i32,
    }

    impl QuoteStatisticsFixture {
        fn into_message(self) -> rithmic_rs::rti::QuoteStatistics {
            rithmic_rs::rti::QuoteStatistics {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                presence_bits: Some(self.presence_bits),
                clear_bits: Some(self.clear_bits),
                is_snapshot: Some(self.is_snapshot),
                highest_bid_price: Some(self.highest_bid_price),
                lowest_ask_price: Some(self.lowest_ask_price),
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct IndicatorPricesFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        presence_bits: u32,
        clear_bits: u32,
        is_snapshot: bool,
        opening_indicator: f64,
        closing_indicator: f64,
        ssboe: i32,
        usecs: i32,
    }

    impl IndicatorPricesFixture {
        fn into_message(self) -> rithmic_rs::rti::IndicatorPrices {
            rithmic_rs::rti::IndicatorPrices {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                presence_bits: Some(self.presence_bits),
                clear_bits: Some(self.clear_bits),
                is_snapshot: Some(self.is_snapshot),
                opening_indicator: Some(self.opening_indicator),
                closing_indicator: Some(self.closing_indicator),
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct OpenInterestFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        is_snapshot: bool,
        should_clear: bool,
        open_interest: u64,
        ssboe: i32,
        usecs: i32,
    }

    impl OpenInterestFixture {
        fn into_message(self) -> rithmic_rs::rti::OpenInterest {
            rithmic_rs::rti::OpenInterest {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                is_snapshot: Some(self.is_snapshot),
                should_clear: Some(self.should_clear),
                open_interest: Some(self.open_interest),
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct EndOfDayPricesFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        presence_bits: u32,
        clear_bits: u32,
        is_snapshot: bool,
        close_price: f64,
        close_date: String,
        adjusted_close_price: f64,
        settlement_price: f64,
        settlement_date: String,
        settlement_price_type: String,
        projected_settlement_price: f64,
        ssboe: i32,
        usecs: i32,
    }

    impl EndOfDayPricesFixture {
        fn into_message(self) -> rithmic_rs::rti::EndOfDayPrices {
            rithmic_rs::rti::EndOfDayPrices {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                presence_bits: Some(self.presence_bits),
                clear_bits: Some(self.clear_bits),
                is_snapshot: Some(self.is_snapshot),
                close_price: Some(self.close_price),
                close_date: Some(self.close_date),
                adjusted_close_price: Some(self.adjusted_close_price),
                settlement_price: Some(self.settlement_price),
                settlement_date: Some(self.settlement_date),
                settlement_price_type: Some(self.settlement_price_type),
                projected_settlement_price: Some(self.projected_settlement_price),
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct OrderPriceLimitsFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        presence_bits: u32,
        clear_bits: u32,
        is_snapshot: bool,
        high_price_limit: f64,
        low_price_limit: f64,
        ssboe: i32,
        usecs: i32,
    }

    impl OrderPriceLimitsFixture {
        fn into_message(self) -> rithmic_rs::rti::OrderPriceLimits {
            rithmic_rs::rti::OrderPriceLimits {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                presence_bits: Some(self.presence_bits),
                clear_bits: Some(self.clear_bits),
                is_snapshot: Some(self.is_snapshot),
                high_price_limit: Some(self.high_price_limit),
                low_price_limit: Some(self.low_price_limit),
                ssboe: Some(self.ssboe),
                usecs: Some(self.usecs),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct SymbolMarginRateFixture {
        template_id: i32,
        symbol: String,
        exchange: String,
        is_snapshot: bool,
        margin_rate: f64,
    }

    impl SymbolMarginRateFixture {
        fn into_message(self) -> rithmic_rs::rti::SymbolMarginRate {
            rithmic_rs::rti::SymbolMarginRate {
                template_id: self.template_id,
                symbol: Some(self.symbol),
                exchange: Some(self.exchange),
                is_snapshot: Some(self.is_snapshot),
                margin_rate: Some(self.margin_rate),
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

    fn test_instruments() -> AHashMap<String, InstrumentInfo> {
        ["ESM6", "ESZ4"]
            .into_iter()
            .map(|symbol| {
                (
                    format!("CME:{symbol}"),
                    InstrumentInfo {
                        symbol: symbol.to_string(),
                        exchange: "CME".to_string(),
                        tick_size: Some(0.25),
                        point_value: Some(50.0),
                        product_code: Some("ES".to_string()),
                        description: Some("E-mini S&P 500".to_string()),
                        currency: Some("USD".to_string()),
                        is_tradeable: true,
                    },
                )
            })
            .collect()
    }

    fn set_env(key: &str, value: Option<&str>) -> Option<String> {
        let previous = std::env::var(key).ok();

        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
        previous
    }

    fn restore_env(entries: &[(&str, Option<String>)]) {
        for (key, value) in entries {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Test helpers are called with inline message fixtures for readability"
    )]
    fn transform_market_data_for_test(
        message: RithmicMessage,
        instruments: &AHashMap<String, InstrumentInfo>,
        quote_state: &mut AHashMap<String, crate::data::QuoteTick>,
    ) -> Option<MarketDataEvent> {
        transform_market_data_message(&message, instruments, quote_state)
            .into_iter()
            .next()
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Test helpers are called with inline message fixtures for readability"
    )]
    fn transform_history_market_data_for_test(
        message: RithmicMessage,
        instruments: &AHashMap<String, InstrumentInfo>,
    ) -> Option<MarketDataEvent> {
        transform_history_market_data_message(&message, instruments)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Test helpers are called with inline message fixtures for readability"
    )]
    fn transform_pnl_for_test(message: RithmicMessage) -> Option<PnlEvent> {
        transform_pnl_message(&message)
    }

    #[rstest::rstest]
    fn test_gateway_creation() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        let gateway = RithmicGateway::new(config);

        assert_eq!(gateway.connection_state(), ConnectionState::Disconnected);
        assert!(!gateway.is_connected());
        // History handle is None before connect
        assert!(gateway.history_handle().is_none());
    }

    #[rstest::rstest]
    fn test_gateway_config_builder() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap()
        .with_ticker(true)
        .with_order(false)
        .with_pnl(true)
        .with_history(false);

        assert!(config.enable_ticker);
        assert!(!config.enable_order);
        assert!(config.enable_pnl);
        assert!(!config.enable_history);
        assert_eq!(config.app_name, "TestApp");
        assert_eq!(config.app_version, DEFAULT_APP_VERSION);
    }

    #[rstest::rstest]
    fn test_gateway_config_debug_redacts_password() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "super-secret",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        let output = format!("{config:?}");

        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("super-secret"));
    }

    #[tokio::test]
    async fn connect_config_failure_rolls_back_connecting_state() {
        let mut config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        config.app_name.clear();
        let mut gateway = RithmicGateway::new(config);

        assert!(gateway.connect().await.is_err());
        assert_eq!(gateway.connection_state(), ConnectionState::Disconnected);
        assert!(!gateway.has_connection_resources());
    }

    #[tokio::test]
    async fn order_book_bootstrap_timeout_requires_recovery() {
        let bootstraps = Arc::new(RwLock::new(AHashMap::from_iter([(
            "CME:ESM6".to_string(),
            PendingOrderBookBootstrap {
                live_deltas: Vec::new(),
                started_at: Instant::now()
                    .checked_sub(ORDER_BOOK_BOOTSTRAP_TIMEOUT)
                    .expect("test timeout must fit within monotonic clock range"),
            },
        )])));

        let result = buffer_live_order_book_events(&bootstraps, "CME:ESM6", Vec::new()).await;

        assert!(matches!(result, OrderBookBufferResult::RecoveryRequired));
        assert!(bootstraps.read().await.is_empty());
    }

    #[tokio::test]
    async fn order_book_bootstrap_delta_limit_requires_recovery() {
        let delta = crate::data::BookDelta {
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            action: "ADD".to_string(),
            side: "BUY".to_string(),
            price: 4_500.0,
            size: 1.0,
            order_id: 1,
            sequence: 1,
            flags: 0,
            price_precision: 2,
            size_precision: 0,
            ts_event: 1,
            ts_init: 1,
        };
        let bootstraps = Arc::new(RwLock::new(AHashMap::from_iter([(
            "CME:ESM6".to_string(),
            PendingOrderBookBootstrap {
                live_deltas: vec![delta.clone(); ORDER_BOOK_BOOTSTRAP_MAX_DELTAS],
                started_at: Instant::now(),
            },
        )])));

        let result = buffer_live_order_book_events(
            &bootstraps,
            "CME:ESM6",
            vec![MarketDataEvent::BookDelta(delta)],
        )
        .await;

        assert!(matches!(result, OrderBookBufferResult::RecoveryRequired));
        assert!(bootstraps.read().await.is_empty());
    }

    #[rstest::rstest]
    fn test_gateway_config_to_rithmic_config_uses_url_overrides() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "OwnApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap()
        .with_url_override("ws://127.0.0.1:12345")
        .with_beta_url_override("ws://127.0.0.1:12346");

        let rithmic = config.to_rithmic_config().unwrap();

        assert_eq!(rithmic.url, "ws://127.0.0.1:12345");
        assert_eq!(rithmic.beta_url, "ws://127.0.0.1:12346");
        assert_eq!(rithmic.system_name, "system");
    }

    #[rstest::rstest]
    fn test_gateway_config_to_rithmic_config_defaults_primary_server() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            ("RITHMIC_DEMO_URL", set_env("RITHMIC_DEMO_URL", None)),
            (
                "RITHMIC_DEMO_ALT_URL",
                set_env("RITHMIC_DEMO_ALT_URL", None),
            ),
        ];

        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "OwnApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();

        let rithmic = config.to_rithmic_config().unwrap();

        assert_eq!(rithmic.url, "wss://rprotocol.rithmic.com:443");
        assert_eq!(rithmic.beta_url, "");

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_gateway_config_to_rithmic_config_resolves_named_servers() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "OwnApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap()
        .with_server("Chicago")
        .with_alt_server("Sydney");

        let rithmic = config.to_rithmic_config().unwrap();

        assert_eq!(rithmic.url, "wss://rprotocol.rithmic.com:443");
        assert_eq!(rithmic.beta_url, "wss://rprotocol-au.rithmic.com:443");
    }

    #[rstest::rstest]
    fn test_gateway_config_from_env_uses_canonical_vars() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            ("RITHMIC_ENV", set_env("RITHMIC_ENV", Some("demo"))),
            (
                "RITHMIC_USERNAME",
                set_env("RITHMIC_USERNAME", Some("user")),
            ),
            (
                "RITHMIC_PASSWORD",
                set_env("RITHMIC_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_SYSTEM_NAME",
                set_env("RITHMIC_SYSTEM_NAME", Some("system")),
            ),
            (
                "RITHMIC_APP_NAME",
                set_env("RITHMIC_APP_NAME", Some("MyApp")),
            ),
            (
                "RITHMIC_APP_VERSION",
                set_env("RITHMIC_APP_VERSION", Some("2.0")),
            ),
            ("RITHMIC_FCM_ID", set_env("RITHMIC_FCM_ID", Some("fcm"))),
            ("RITHMIC_IB_ID", set_env("RITHMIC_IB_ID", Some("ib"))),
            (
                "RITHMIC_ACCOUNT_ID",
                set_env("RITHMIC_ACCOUNT_ID", Some("account")),
            ),
            ("RITHMIC_SERVER", set_env("RITHMIC_SERVER", Some("Chicago"))),
            (
                "RITHMIC_ALT_SERVER",
                set_env("RITHMIC_ALT_SERVER", Some("Sydney")),
            ),
        ];

        let config = GatewayConfig::from_env().unwrap();

        assert_eq!(config.environment, RithmicEnv::Demo);
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
        assert_eq!(config.system_name, "system");
        assert_eq!(config.app_name, "MyApp");
        assert_eq!(config.app_version, "2.0");
        assert_eq!(config.fcm_id, "fcm");
        assert_eq!(config.ib_id, "ib");
        assert_eq!(config.account_id, "account");
        assert_eq!(config.server.as_deref(), Some("Chicago"));
        assert_eq!(config.alt_server.as_deref(), Some("Sydney"));

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_gateway_config_from_env_requires_canonical_username() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            ("RITHMIC_ENV", set_env("RITHMIC_ENV", Some("demo"))),
            ("RITHMIC_USERNAME", set_env("RITHMIC_USERNAME", None)),
            (
                "RITHMIC_PASSWORD",
                set_env("RITHMIC_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_SYSTEM_NAME",
                set_env("RITHMIC_SYSTEM_NAME", Some("system")),
            ),
            ("RITHMIC_APP_NAME", set_env("RITHMIC_APP_NAME", None)),
            ("RITHMIC_APP_VERSION", set_env("RITHMIC_APP_VERSION", None)),
            ("RITHMIC_FCM_ID", set_env("RITHMIC_FCM_ID", None)),
            ("RITHMIC_IB_ID", set_env("RITHMIC_IB_ID", None)),
            ("RITHMIC_SERVER", set_env("RITHMIC_SERVER", None)),
            ("RITHMIC_ALT_SERVER", set_env("RITHMIC_ALT_SERVER", None)),
            (
                "RITHMIC_ACCOUNT_ID",
                set_env("RITHMIC_ACCOUNT_ID", Some("account")),
            ),
            (
                "RITHMIC_DEMO_USER",
                set_env("RITHMIC_DEMO_USER", Some("legacy-user")),
            ),
        ];

        let e = GatewayConfig::from_env().unwrap_err();
        assert!(e.to_string().contains("RITHMIC_USERNAME not set"));

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_gateway_config_to_rithmic_config_requires_app_name() {
        let e = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "",
            "fcm",
            "ib",
            "account",
        )
        .unwrap_err();

        assert!(e.to_string().contains("app_name cannot be empty"));
    }

    #[rstest::rstest]
    fn test_gateway_config_requires_system_name() {
        let e = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap_err();

        assert!(e.to_string().contains("system_name cannot be empty"));
    }

    #[rstest::rstest]
    fn test_gateway_config_from_profile_env() {
        let _guard = RITHMIC_ENV_TEST_LOCK.lock().unwrap();
        let previous = [
            (
                "RITHMIC_APEX_ENV",
                set_env("RITHMIC_APEX_ENV", Some("live")),
            ),
            (
                "RITHMIC_APEX_USERNAME",
                set_env("RITHMIC_APEX_USERNAME", Some("user")),
            ),
            (
                "RITHMIC_APEX_PASSWORD",
                set_env("RITHMIC_APEX_PASSWORD", Some("pass")),
            ),
            (
                "RITHMIC_APEX_SYSTEM_NAME",
                set_env("RITHMIC_APEX_SYSTEM_NAME", Some("Apex")),
            ),
            (
                "RITHMIC_APEX_APP_NAME",
                set_env("RITHMIC_APEX_APP_NAME", Some("MyApp")),
            ),
            (
                "RITHMIC_APEX_ACCOUNT_ID",
                set_env("RITHMIC_APEX_ACCOUNT_ID", Some("account")),
            ),
            (
                "RITHMIC_APEX_FCM_ID",
                set_env("RITHMIC_APEX_FCM_ID", Some("fcm")),
            ),
            (
                "RITHMIC_APEX_SERVER",
                set_env("RITHMIC_APEX_SERVER", Some("Frankfurt")),
            ),
            (
                "RITHMIC_APEX_LIVE_URL",
                set_env("RITHMIC_APEX_LIVE_URL", Some("wss://primary.example.test")),
            ),
            (
                "RITHMIC_APEX_LIVE_ALT_URL",
                set_env(
                    "RITHMIC_APEX_LIVE_ALT_URL",
                    Some("wss://alternate.example.test"),
                ),
            ),
        ];

        let config = GatewayConfig::from_env_with_profile(Some("Apex")).unwrap();

        assert_eq!(config.environment, RithmicEnv::Live);
        assert_eq!(config.username, "user");
        assert_eq!(config.password, "pass");
        assert_eq!(config.system_name, "Apex");
        assert_eq!(config.app_name, "MyApp");
        assert_eq!(config.account_id, "account");
        assert_eq!(config.fcm_id, "fcm");
        assert_eq!(config.server.as_deref(), Some("Frankfurt"));
        assert_eq!(
            config.url_override.as_deref(),
            Some("wss://primary.example.test")
        );
        assert_eq!(
            config.beta_url_override.as_deref(),
            Some("wss://alternate.example.test")
        );

        restore_env(&previous);
    }

    #[rstest::rstest]
    fn test_connection_state_default() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        let gateway = RithmicGateway::new(config);

        assert_eq!(gateway.connection_state(), ConnectionState::Disconnected);
    }

    #[rstest::rstest]
    fn test_instruments_initially_empty() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        let gateway = RithmicGateway::new(config);
        let instruments = gateway.instruments().blocking_read();
        assert!(instruments.is_empty());
    }

    #[rstest::rstest]
    fn test_event_receivers_support_multiple_subscribers() {
        let config = GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "TestApp",
            "fcm",
            "ib",
            "account",
        )
        .unwrap();
        let gateway = RithmicGateway::new(config);

        let _market_rx_a = gateway.subscribe_market_data_events();
        let _market_rx_b = gateway.subscribe_market_data_events();
        let _execution_rx_a = gateway.subscribe_execution_events();
        let _execution_rx_b = gateway.subscribe_execution_events();
        let _pnl_rx_a = gateway.subscribe_pnl_events();
        let _pnl_rx_b = gateway.subscribe_pnl_events();
    }

    #[rstest::rstest]
    fn test_rithmic_timestamp_to_nanos() {
        // Test with both ssboe and usecs
        let nanos = rithmic_timestamp_to_nanos(Some(1704067200), Some(123456));
        // 1704067200 * 1e9 + 123456 * 1e3 = 1704067200000000000 + 123456000
        assert_eq!(nanos, Some(1704067200123456000));

        // Test with only ssboe
        let nanos = rithmic_timestamp_to_nanos(Some(1704067200), None);
        assert_eq!(nanos, Some(1704067200000000000));

        // Test with None values
        let nanos = rithmic_timestamp_to_nanos(None, None);
        assert_eq!(nanos, None);

        assert_eq!(rithmic_timestamp_to_nanos(Some(-1), Some(0)), None);
        assert_eq!(rithmic_timestamp_to_nanos(Some(1), Some(1_000_000)), None);
    }

    #[rstest::rstest]
    fn test_transform_bbo_to_quote() {
        use rithmic_rs::rti::BestBidOffer;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let bbo = BestBidOffer {
            template_id: 150,
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(5000.25),
            bid_size: Some(100),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(5000.50),
            ask_size: Some(150),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(1704067200),
            usecs: Some(500000),
        };

        let event = transform_market_data_for_test(
            RithmicMessage::BestBidOffer(bbo),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_some());

        if let Some(MarketDataEvent::Quote(quote)) = event {
            assert_eq!(quote.symbol, "ESZ4");
            assert_eq!(quote.exchange, "CME");
            assert_eq!(quote.bid_price, 5000.25);
            assert_eq!(quote.ask_price, 5000.50);
            assert_eq!(quote.bid_size, 100.0);
            assert_eq!(quote.ask_size, 150.0);
            assert_eq!(quote.ts_event, 1704067200500000000);
        } else {
            panic!("Expected Quote event");
        }
    }

    #[rstest::rstest]
    fn test_transform_bbo_waits_for_instrument_precision() {
        use rithmic_rs::rti::BestBidOffer;

        let instruments = AHashMap::new();
        let mut quote_state = AHashMap::new();
        let bbo = BestBidOffer {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            bid_price: Some(4500.25),
            bid_size: Some(1),
            ask_price: Some(4500.50),
            ask_size: Some(2),
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        };

        let event = transform_market_data_for_test(
            RithmicMessage::BestBidOffer(bbo),
            &instruments,
            &mut quote_state,
        );

        assert!(event.is_none());
        assert!(quote_state.is_empty());
    }

    #[rstest::rstest]
    fn test_transform_bbo_preserves_last_seen_opposite_side() {
        use rithmic_rs::rti::{BestBidOffer, best_bid_offer::PresenceBits};

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();

        let initial = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some((PresenceBits::Bid as u32) | (PresenceBits::Ask as u32)),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(6619.00),
            bid_size: Some(9),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(6619.50),
            ask_size: Some(6),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(1),
            usecs: Some(0),
        });

        let partial = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Ask as u32),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(0.0),
            bid_size: Some(0),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(6619.50),
            ask_size: Some(5),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(2),
            usecs: Some(0),
        });

        let _ = transform_market_data_for_test(initial, &instruments, &mut quote_state);
        let event = transform_market_data_for_test(partial, &instruments, &mut quote_state);

        match event {
            Some(MarketDataEvent::Quote(quote)) => {
                assert_eq!(quote.bid_price, 6619.00);
                assert_eq!(quote.bid_size, 9.0);
                assert_eq!(quote.ask_price, 6619.50);
                assert_eq!(quote.ask_size, 5.0);
            }
            other => panic!("Expected Quote event, was {other:?}"),
        }
    }

    #[rstest::rstest]
    fn test_transform_bbo_preserves_last_seen_ask_when_bid_only_update_zeroes_ask_fields() {
        use rithmic_rs::rti::{BestBidOffer, best_bid_offer::PresenceBits};

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();

        let initial = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some((PresenceBits::Bid as u32) | (PresenceBits::Ask as u32)),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(6617.00),
            bid_size: Some(3),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(6617.25),
            ask_size: Some(1),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(1),
            usecs: Some(0),
        });

        let bid_only = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Bid as u32),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(6616.75),
            bid_size: Some(10),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(0.0),
            ask_size: Some(0),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(2),
            usecs: Some(0),
        });

        let _ = transform_market_data_for_test(initial, &instruments, &mut quote_state);
        let event = transform_market_data_for_test(bid_only, &instruments, &mut quote_state);

        match event {
            Some(MarketDataEvent::Quote(quote)) => {
                assert_eq!(quote.bid_price, 6616.75);
                assert_eq!(quote.bid_size, 10.0);
                assert_eq!(quote.ask_price, 6617.25);
                assert_eq!(quote.ask_size, 1.0);
            }
            other => panic!("Expected Quote event, was {other:?}"),
        }
    }

    #[rstest::rstest]
    fn test_transform_bbo_waits_until_both_sides_seen_before_emitting_quote() {
        use rithmic_rs::rti::{BestBidOffer, best_bid_offer::PresenceBits};

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();

        let ask_only = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Ask as u32),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(0.0),
            bid_size: Some(0),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(6617.25),
            ask_size: Some(1),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(1),
            usecs: Some(0),
        });

        let bid_only = RithmicMessage::BestBidOffer(BestBidOffer {
            template_id: 150,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Bid as u32),
            clear_bits: None,
            is_snapshot: Some(false),
            bid_price: Some(6617.00),
            bid_size: Some(3),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(0.0),
            ask_size: Some(0),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: Some(2),
            usecs: Some(0),
        });

        let first = transform_market_data_for_test(ask_only, &instruments, &mut quote_state);
        assert!(first.is_none());

        let second = transform_market_data_for_test(bid_only, &instruments, &mut quote_state);

        match second {
            Some(MarketDataEvent::Quote(quote)) => {
                assert_eq!(quote.bid_price, 6617.00);
                assert_eq!(quote.bid_size, 3.0);
                assert_eq!(quote.ask_price, 6617.25);
                assert_eq!(quote.ask_size, 1.0);
            }
            other => panic!("Expected Quote event, was {other:?}"),
        }
    }

    #[rstest::rstest]
    fn test_transform_bbo_clear_resets_side_until_repopulated() {
        use rithmic_rs::rti::{BestBidOffer, best_bid_offer::PresenceBits};

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let initial = BestBidOffer {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some((PresenceBits::Bid as u32) | (PresenceBits::Ask as u32)),
            bid_price: Some(4500.25),
            bid_size: Some(3),
            ask_price: Some(4500.50),
            ask_size: Some(4),
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        };
        let clear_bid = BestBidOffer {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            clear_bits: Some(PresenceBits::Bid as u32),
            ssboe: Some(1_700_000_001),
            usecs: Some(0),
            ..Default::default()
        };
        let ask_only = BestBidOffer {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Ask as u32),
            ask_price: Some(4500.75),
            ask_size: Some(2),
            ssboe: Some(1_700_000_002),
            usecs: Some(0),
            ..Default::default()
        };
        let bid_only = BestBidOffer {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: Some(PresenceBits::Bid as u32),
            bid_price: Some(4500.50),
            bid_size: Some(1),
            ssboe: Some(1_700_000_003),
            usecs: Some(0),
            ..Default::default()
        };

        assert!(
            transform_market_data_for_test(
                RithmicMessage::BestBidOffer(initial),
                &instruments,
                &mut quote_state,
            )
            .is_some()
        );
        assert!(
            transform_market_data_for_test(
                RithmicMessage::BestBidOffer(clear_bid),
                &instruments,
                &mut quote_state,
            )
            .is_none()
        );
        let cached = quote_state
            .get("CME:ESM6")
            .expect("quote state should remain");
        assert_eq!(cached.bid_price, 0.0);
        assert_eq!(cached.bid_size, 0.0);
        assert_eq!(cached.ask_price, 4500.50);
        assert!(
            transform_market_data_for_test(
                RithmicMessage::BestBidOffer(ask_only),
                &instruments,
                &mut quote_state,
            )
            .is_none()
        );

        let event = transform_market_data_for_test(
            RithmicMessage::BestBidOffer(bid_only),
            &instruments,
            &mut quote_state,
        );
        let Some(MarketDataEvent::Quote(quote)) = event else {
            panic!("expected repopulated quote")
        };
        assert_eq!(quote.bid_price, 4500.50);
        assert_eq!(quote.ask_price, 4500.75);
    }

    #[rstest::rstest]
    fn test_transform_last_trade_to_trade() {
        use rithmic_rs::rti::LastTrade;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let trade = LastTrade {
            template_id: 151,
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: Some(false),
            trade_price: Some(5000.25),
            trade_size: Some(10),
            aggressor: Some(1), // Buy
            exchange_order_id: Some("12345".to_string()),
            aggressor_exchange_order_id: None,
            net_change: None,
            percent_change: None,
            volume: None,
            vwap: None,
            trade_time: None,
            ssboe: Some(1704067200),
            usecs: Some(750000),
            source_ssboe: None,
            source_usecs: None,
            source_nsecs: None,
            jop_ssboe: None,
            jop_nsecs: None,
        };

        let event = transform_market_data_for_test(
            RithmicMessage::LastTrade(trade),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_some());

        if let Some(MarketDataEvent::Trade(trade)) = event {
            assert_eq!(trade.symbol, "ESZ4");
            assert_eq!(trade.exchange, "CME");
            assert_eq!(trade.price, 5000.25);
            assert_eq!(trade.size, 10.0);
            assert_eq!(trade.aggressor_side, "BUY");
            assert_eq!(trade.trade_id, "12345");
            assert_eq!(trade.ts_event, 1704067200750000000);
        } else {
            panic!("Expected Trade event");
        }
    }

    #[rstest::rstest]
    fn test_transform_bbo_missing_symbol_returns_none() {
        use rithmic_rs::rti::BestBidOffer;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let bbo = BestBidOffer {
            template_id: 150,
            symbol: None, // Missing symbol
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: None,
            bid_price: Some(5000.25),
            bid_size: Some(100),
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: Some(5000.50),
            ask_size: Some(150),
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: None,
            usecs: None,
        };

        let event = transform_market_data_for_test(
            RithmicMessage::BestBidOffer(bbo),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_bbo_no_prices_returns_none() {
        use rithmic_rs::rti::BestBidOffer;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let bbo = BestBidOffer {
            template_id: 150,
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: None,
            bid_price: None, // No bid
            bid_size: None,
            bid_orders: None,
            bid_implicit_size: None,
            bid_time: None,
            ask_price: None, // No ask
            ask_size: None,
            ask_orders: None,
            ask_implicit_size: None,
            ask_time: None,
            lean_price: None,
            ssboe: None,
            usecs: None,
        };

        let event = transform_market_data_for_test(
            RithmicMessage::BestBidOffer(bbo),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_trade_zero_size_returns_none() {
        use rithmic_rs::rti::LastTrade;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let trade = LastTrade {
            template_id: 151,
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: None,
            trade_price: Some(5000.25),
            trade_size: Some(0), // Zero size
            aggressor: Some(1),
            exchange_order_id: None,
            aggressor_exchange_order_id: None,
            net_change: None,
            percent_change: None,
            volume: None,
            vwap: None,
            trade_time: None,
            ssboe: None,
            usecs: None,
            source_ssboe: None,
            source_usecs: None,
            source_nsecs: None,
            jop_ssboe: None,
            jop_nsecs: None,
        };

        let event = transform_market_data_for_test(
            RithmicMessage::LastTrade(trade),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_trade_sell_aggressor() {
        use rithmic_rs::rti::LastTrade;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let trade = LastTrade {
            template_id: 151,
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: None,
            trade_price: Some(5000.25),
            trade_size: Some(5),
            aggressor: Some(2), // Sell
            exchange_order_id: Some("passive-order-id".to_string()),
            aggressor_exchange_order_id: Some("aggressor-order-id".to_string()),
            net_change: None,
            percent_change: None,
            volume: None,
            vwap: None,
            trade_time: None,
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            source_ssboe: None,
            source_usecs: None,
            source_nsecs: None,
            jop_ssboe: None,
            jop_nsecs: None,
        };

        let mut aggressor_only = trade.clone();
        aggressor_only.exchange_order_id = None;

        let event = transform_market_data_for_test(
            RithmicMessage::LastTrade(trade),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_some());

        if let Some(MarketDataEvent::Trade(trade)) = event {
            assert_eq!(trade.aggressor_side, "SELL");
            assert_eq!(trade.trade_id, "passive-order-id");
        } else {
            panic!("Expected Trade event");
        }

        let event = transform_market_data_for_test(
            RithmicMessage::LastTrade(aggressor_only),
            &instruments,
            &mut quote_state,
        );
        let Some(MarketDataEvent::Trade(trade)) = event else {
            panic!("Expected Trade event");
        };
        assert_eq!(trade.trade_id, "aggressor-order-id");
    }

    #[rstest::rstest]
    fn test_transform_order_book_to_depth10() {
        use rithmic_rs::rti::OrderBook;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let book = OrderBook {
            template_id: 152,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            update_type: Some(rithmic_rs::rti::order_book::UpdateType::SnapshotImage as i32),
            bid_price: vec![4500.25, 4500.00],
            bid_size: vec![10, 8],
            bid_orders: vec![2, 1],
            impl_bid_size: vec![],
            ask_price: vec![4500.50, 4500.75],
            ask_size: vec![12, 7],
            ask_orders: vec![3, 1],
            impl_ask_size: vec![],
            ssboe: Some(1_700_000_000),
            usecs: Some(123_456),
        };

        let event = transform_market_data_for_test(
            RithmicMessage::OrderBook(book),
            &instruments,
            &mut quote_state,
        )
        .expect("expected depth10 event");

        let MarketDataEvent::Depth10(depth) = event else {
            panic!("expected depth10 event");
        };
        assert_eq!(depth.instrument_id, InstrumentId::from("ESM6.CME.RITHMIC"));
        assert_eq!(depth.bids[0].price.as_f64(), 4500.25);
        assert_eq!(depth.asks[0].price.as_f64(), 4500.50);
        assert_eq!(depth.bid_counts[0], 2);
        assert_eq!(depth.ask_counts[0], 3);
        assert!(depth.flags & RecordFlag::F_MBP as u8 != 0);
        assert!(depth.flags & RecordFlag::F_SNAPSHOT as u8 != 0);
    }

    #[rstest::rstest]
    fn test_transform_order_book_drops_non_finite_price() {
        use rithmic_rs::rti::OrderBook;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let book = OrderBook {
            template_id: 152,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            update_type: Some(rithmic_rs::rti::order_book::UpdateType::SnapshotImage as i32),
            bid_price: vec![f64::NAN],
            bid_size: vec![10],
            bid_orders: vec![1],
            impl_bid_size: vec![],
            ask_price: vec![4500.50],
            ask_size: vec![12],
            ask_orders: vec![1],
            impl_ask_size: vec![],
            ssboe: Some(1_700_000_000),
            usecs: Some(123_456),
        };

        let event = transform_market_data_for_test(
            RithmicMessage::OrderBook(book),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_order_book_drops_misaligned_order_counts() {
        use rithmic_rs::rti::OrderBook;

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let book = OrderBook {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            update_type: Some(rithmic_rs::rti::order_book::UpdateType::SnapshotImage as i32),
            bid_price: vec![4500.25],
            bid_size: vec![10],
            bid_orders: vec![],
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        };

        let event = transform_market_data_for_test(
            RithmicMessage::OrderBook(book),
            &instruments,
            &mut quote_state,
        );
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_market_mode_to_instrument_status() {
        use rithmic_rs::rti::MarketMode;

        let instruments = AHashMap::new();
        let mut quote_state = AHashMap::new();
        let mode = MarketMode {
            template_id: 153,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            market_mode: Some("HALTED".to_string()),
            halt_reason: Some("NEWS_PENDING".to_string()),
            trade_date: Some("20260402".to_string()),
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
        };

        let event = transform_market_data_for_test(
            RithmicMessage::MarketMode(mode),
            &instruments,
            &mut quote_state,
        )
        .expect("expected instrument status event");

        let MarketDataEvent::InstrumentStatus(status) = event else {
            panic!("expected instrument status event");
        };
        assert_eq!(status.instrument_id, InstrumentId::from("ESM6.CME.RITHMIC"));
        assert_eq!(status.action, MarketStatusAction::Halt);
        assert_eq!(status.reason, Some(Ustr::from("NEWS_PENDING")));
        assert_eq!(status.trading_event, Some(Ustr::from("20260402")));
        assert_eq!(status.is_trading, Some(false));
    }

    #[rstest::rstest]
    fn test_unknown_market_mode_does_not_assert_closed_state() {
        use rithmic_rs::rti::MarketMode;

        let status = market_mode_to_status(&MarketMode {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            market_mode: Some("UNRECOGNIZED_MODE".to_string()),
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        })
        .expect("unknown mode should remain observable");

        assert_eq!(status.action, MarketStatusAction::None);
        assert_eq!(status.is_trading, None);
        assert_eq!(status.is_quoting, None);
    }

    #[rstest::rstest]
    fn test_transform_trade_statistics_to_custom_data() {
        use rithmic_rs::rti::TradeStatistics;

        let instruments = AHashMap::new();
        let mut quote_state = AHashMap::new();
        let stats = TradeStatistics {
            template_id: 154,
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            presence_bits: None,
            clear_bits: None,
            is_snapshot: Some(true),
            open_price: Some(4499.0),
            high_price: Some(4505.0),
            low_price: Some(4498.5),
            ssboe: Some(1_700_000_000),
            usecs: Some(10),
            source_ssboe: None,
            source_usecs: None,
            source_nsecs: None,
            jop_ssboe: None,
            jop_nsecs: None,
        };

        let event = transform_market_data_for_test(
            RithmicMessage::TradeStatistics(stats),
            &instruments,
            &mut quote_state,
        )
        .expect("expected custom event");

        let MarketDataEvent::Custom(RithmicCustomData::TradeStatistics(value)) = event else {
            panic!("expected trade statistics custom event");
        };
        assert_eq!(value.instrument_id, InstrumentId::from("ESM6.CME.RITHMIC"));
        assert!(value.is_snapshot);
        assert_eq!(value.high_price, Some(4505.0));
    }

    #[rstest::rstest]
    fn test_custom_data_drops_non_finite_numeric_payload() {
        use rithmic_rs::rti::TradeStatistics;

        let malformed = TradeStatistics {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            is_snapshot: Some(false),
            open_price: Some(f64::NAN),
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        };

        assert!(trade_statistics_to_custom(&malformed).is_none());
    }

    #[rstest::rstest]
    fn test_depth_snapshot_fixture_emits_clear_then_snapshot_adds() {
        let fixture = load_fixture::<DepthSnapshotFixture>("depth_snapshot_delta.json");
        let instruments = test_instruments();
        let snapshot_rows: Vec<rithmic_rs::rti::ResponseDepthByOrderSnapshot> = fixture
            .snapshots
            .into_iter()
            .map(SnapshotRowFixture::into_message)
            .collect();
        let snapshot_refs: Vec<&rithmic_rs::rti::ResponseDepthByOrderSnapshot> =
            snapshot_rows.iter().collect();

        let events = depth_snapshot_rows_to_events(&snapshot_refs, &instruments);

        assert_eq!(events.len(), 4);

        let MarketDataEvent::BookDelta(clear) = &events[0] else {
            panic!("expected first depth event to clear the local book");
        };
        assert_eq!(clear.action, "CLEAR");
        assert_eq!(clear.sequence, 101);
        assert_eq!(clear.flags, RecordFlag::F_SNAPSHOT as u8);

        let MarketDataEvent::BookDelta(first_add) = &events[1] else {
            panic!("expected first snapshot row to convert into a book delta");
        };
        assert_eq!(first_add.action, "ADD");
        assert_eq!(first_add.side, "BUY");
        assert_eq!(first_add.price, 4500.25);
        assert_eq!(first_add.size, 7.0);
        assert_eq!(
            first_add.order_id,
            crate::common::parse::rithmic_depth_order_id(Some("bid-11"), 11)
        );
        assert_eq!(first_add.flags, RecordFlag::F_SNAPSHOT as u8);

        let MarketDataEvent::BookDelta(last_add) = &events[3] else {
            panic!("expected final snapshot row to convert into a book delta");
        };
        assert_eq!(last_add.action, "ADD");
        assert_eq!(last_add.side, "SELL");
        assert_eq!(last_add.price, 4500.50);
        assert_eq!(last_add.size, 6.0);
        assert_eq!(
            last_add.order_id,
            crate::common::parse::rithmic_depth_order_id(Some("ask-21"), 21)
        );
        assert_eq!(
            last_add.flags,
            RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
        );
    }

    #[rstest::rstest]
    fn test_depth_delta_fixture_sets_last_flag_on_final_delta() {
        let fixture = load_fixture::<DepthSnapshotFixture>("depth_snapshot_delta.json");
        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();

        let events = transform_market_data_message(
            &RithmicMessage::DepthByOrder(fixture.delta.into_message()),
            &instruments,
            &mut quote_state,
        );

        assert_eq!(events.len(), 2);

        let MarketDataEvent::BookDelta(first) = &events[0] else {
            panic!("expected first fixture delta event");
        };
        assert_eq!(first.action, "UPDATE");
        assert_eq!(first.side, "BUY");
        assert_eq!(first.price, 4500.25);
        assert_eq!(first.size, 9.0);
        assert_eq!(
            first.order_id,
            crate::common::parse::rithmic_depth_order_id(Some("bid-11"), 11)
        );
        assert_eq!(first.flags, 0);

        let MarketDataEvent::BookDelta(second) = &events[1] else {
            panic!("expected second fixture delta event");
        };
        assert_eq!(second.action, "ADD");
        assert_eq!(second.side, "SELL");
        assert_eq!(second.price, 4500.75);
        assert_eq!(second.size, 4.0);
        assert_eq!(
            second.order_id,
            crate::common::parse::rithmic_depth_order_id(Some("ask-22"), 22)
        );
        assert_eq!(second.flags, RecordFlag::F_LAST as u8);
    }

    #[rstest::rstest]
    fn test_depth_delete_without_numeric_fields_preserves_order_identity() {
        use rithmic_rs::rti::{
            DepthByOrder,
            depth_by_order::{TransactionType, UpdateType},
        };

        let instruments = test_instruments();
        let mut quote_state = AHashMap::new();
        let message = DepthByOrder {
            symbol: Some("ESM6".to_string()),
            exchange: Some("CME".to_string()),
            sequence_number: Some(102),
            update_type: vec![UpdateType::Delete as i32],
            transaction_type: vec![TransactionType::Buy as i32],
            depth_price: vec![],
            depth_size: vec![],
            depth_order_priority: vec![11],
            exchange_order_id: vec!["bid-11".to_string()],
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ..Default::default()
        };

        let events = transform_market_data_message(
            &RithmicMessage::DepthByOrder(message),
            &instruments,
            &mut quote_state,
        );
        let [MarketDataEvent::BookDelta(delta)] = events.as_slice() else {
            panic!("expected one delete delta")
        };
        assert_eq!(delta.action, "REMOVE");
        assert_eq!(delta.side, "BUY");
        assert_eq!(delta.price, 0.0);
        assert_eq!(delta.size, 0.0);
        assert_eq!(
            delta.order_id,
            crate::common::parse::rithmic_depth_order_id(Some("bid-11"), 11)
        );
        assert_eq!(delta.flags, RecordFlag::F_LAST as u8);
    }

    #[rstest::rstest]
    fn test_remaining_custom_data_fixture_samples_transform_to_custom_events() {
        let fixture = load_fixture::<CustomDataFixture>("custom_data_events.json");
        let instruments = AHashMap::new();
        let mut quote_state = AHashMap::new();

        let events = [
            transform_market_data_for_test(
                RithmicMessage::TradeStatistics(fixture.trade_statistics.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected trade statistics event"),
            transform_market_data_for_test(
                RithmicMessage::QuoteStatistics(fixture.quote_statistics.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected quote statistics event"),
            transform_market_data_for_test(
                RithmicMessage::IndicatorPrices(fixture.indicator_prices.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected indicator prices event"),
            transform_market_data_for_test(
                RithmicMessage::OpenInterest(fixture.open_interest.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected open interest event"),
            transform_market_data_for_test(
                RithmicMessage::EndOfDayPrices(fixture.end_of_day_prices.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected end of day prices event"),
            transform_market_data_for_test(
                RithmicMessage::OrderPriceLimits(fixture.order_price_limits.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected order price limits event"),
            transform_market_data_for_test(
                RithmicMessage::SymbolMarginRate(fixture.symbol_margin_rate.into_message()),
                &instruments,
                &mut quote_state,
            )
            .expect("expected symbol margin rate event"),
        ];

        let MarketDataEvent::Custom(RithmicCustomData::TradeStatistics(trade_stats)) = &events[0]
        else {
            panic!("expected trade statistics fixture to map to custom data");
        };
        assert_eq!(trade_stats.open_price, Some(4499.0));
        assert_eq!(trade_stats.high_price, Some(4505.0));

        let MarketDataEvent::Custom(RithmicCustomData::QuoteStatistics(quote_stats)) = &events[1]
        else {
            panic!("expected quote statistics fixture to map to custom data");
        };
        assert_eq!(quote_stats.highest_bid_price, Some(4500.25));
        assert_eq!(quote_stats.lowest_ask_price, Some(4500.50));

        let MarketDataEvent::Custom(RithmicCustomData::IndicatorPrices(indicators)) = &events[2]
        else {
            panic!("expected indicator prices fixture to map to custom data");
        };
        assert_eq!(indicators.opening_indicator, Some(4498.75));
        assert_eq!(indicators.closing_indicator, Some(4501.25));

        let MarketDataEvent::Custom(RithmicCustomData::OpenInterest(open_interest)) = &events[3]
        else {
            panic!("expected open interest fixture to map to custom data");
        };
        assert!(open_interest.should_clear);
        assert_eq!(open_interest.open_interest, Some(123_456));

        let MarketDataEvent::Custom(RithmicCustomData::EndOfDayPrices(eod_prices)) = &events[4]
        else {
            panic!("expected end of day prices fixture to map to custom data");
        };
        assert_eq!(eod_prices.close_date.as_deref(), Some("20260401"));
        assert_eq!(eod_prices.settlement_price_type.as_deref(), Some("FINAL"));

        let MarketDataEvent::Custom(RithmicCustomData::OrderPriceLimits(limits)) = &events[5]
        else {
            panic!("expected order price limits fixture to map to custom data");
        };
        assert_eq!(limits.high_price_limit, Some(4550.0));
        assert_eq!(limits.low_price_limit, Some(4450.0));

        let MarketDataEvent::Custom(RithmicCustomData::SymbolMarginRate(margin_rate)) = &events[6]
        else {
            panic!("expected symbol margin rate fixture to map to custom data");
        };
        assert_eq!(margin_rate.margin_rate, Some(1.25));
    }

    #[rstest::rstest]
    fn test_transform_account_pnl_update() {
        use rithmic_rs::rti::AccountPnLPositionUpdate;

        let update = AccountPnLPositionUpdate {
            template_id: 450,
            is_snapshot: Some(false),
            fcm_id: Some("FCMID".to_string()),
            ib_id: Some("IBID".to_string()),
            account_id: Some("ACCOUNT123".to_string()),
            fill_buy_qty: Some(10),
            fill_sell_qty: Some(5),
            order_buy_qty: Some(0),
            order_sell_qty: Some(0),
            buy_qty: Some(10),
            sell_qty: Some(5),
            open_long_options_value: None,
            open_short_options_value: None,
            closed_options_value: None,
            option_cash_reserved: None,
            rms_account_commission: None,
            open_position_pnl: Some("1250.50".to_string()),
            open_position_quantity: Some(5),
            closed_position_pnl: Some("500.00".to_string()),
            closed_position_quantity: Some(10),
            net_quantity: Some(5),
            excess_buy_margin: None,
            margin_balance: Some("25000.00".to_string()),
            min_margin_balance: None,
            min_account_balance: None,
            account_balance: Some("100000.00".to_string()),
            cash_on_hand: Some("75000.00".to_string()),
            option_closed_pnl: None,
            percent_maximum_allowable_loss: None,
            option_open_pnl: None,
            mtm_account: None,
            available_buying_power: None,
            used_buying_power: None,
            reserved_buying_power: None,
            excess_sell_margin: None,
            day_open_pnl: None,
            day_closed_pnl: None,
            day_pnl: None,
            day_open_pnl_offset: None,
            day_closed_pnl_offset: None,
            ssboe: Some(1704067200),
            usecs: Some(123456),
        };

        let event = transform_pnl_for_test(RithmicMessage::AccountPnLPositionUpdate(update));
        assert!(event.is_some());

        if let Some(PnlEvent::Account(AccountEvent::BalanceUpdate(balance))) = event {
            assert_eq!(balance.account_id, "ACCOUNT123");
            assert_eq!(balance.currency, SUPPORTED_ACCOUNT_CURRENCY);
            assert_eq!(balance.total, 100000.0);
            assert_eq!(balance.available, 75000.0);
            assert_eq!(balance.locked, 25000.0);
            assert_eq!(balance.unrealized_pnl, 1250.50);
            assert_eq!(balance.realized_pnl, 500.0);
            assert_eq!(balance.ts_event, 1704067200123456000);
        } else {
            panic!("Expected Account BalanceUpdate event");
        }
    }

    #[rstest::rstest]
    fn test_transform_instrument_pnl_update() {
        use rithmic_rs::rti::InstrumentPnLPositionUpdate;

        let update = InstrumentPnLPositionUpdate {
            template_id: 451,
            is_snapshot: Some(false),
            fcm_id: Some("FCMID".to_string()),
            ib_id: Some("IBID".to_string()),
            account_id: Some("ACCOUNT123".to_string()),
            symbol: Some("ESZ4".to_string()),
            exchange: Some("CME".to_string()),
            product_code: Some("ES".to_string()),
            instrument_type: Some("Future".to_string()),
            fill_buy_qty: Some(10),
            fill_sell_qty: Some(5),
            order_buy_qty: Some(0),
            order_sell_qty: Some(0),
            buy_qty: Some(10),
            sell_qty: Some(5),
            avg_open_fill_price: Some(5025.50),
            day_open_pnl: Some(1500.0),
            day_closed_pnl: Some(750.0),
            day_pnl: Some(2250.0),
            day_open_pnl_offset: None,
            day_closed_pnl_offset: None,
            mtm_security: None,
            open_long_options_value: None,
            open_short_options_value: None,
            closed_options_value: None,
            option_cash_reserved: None,
            open_position_pnl: Some("1500.00".to_string()),
            open_position_quantity: Some(5),
            closed_position_pnl: Some("750.00".to_string()),
            closed_position_quantity: Some(10),
            net_quantity: Some(5),
            ssboe: Some(1704067200),
            usecs: Some(500000),
        };

        let event = transform_pnl_for_test(RithmicMessage::InstrumentPnLPositionUpdate(update));
        assert!(event.is_some());

        if let Some(PnlEvent::Position(PositionEvent::Updated(position))) = event {
            assert_eq!(position.account_id, "ACCOUNT123");
            assert_eq!(position.symbol, "ESZ4");
            assert_eq!(position.exchange, "CME");
            assert_eq!(position.quantity, 5.0); // 10 buy - 5 sell
            assert_eq!(position.avg_price, 5025.50);
            assert_eq!(position.unrealized_pnl, 1500.0); // day_open_pnl
            assert_eq!(position.realized_pnl, 750.0); // day_closed_pnl
            assert_eq!(position.ts_event, 1704067200500000000);
        } else {
            panic!("Expected Position Updated event");
        }
    }

    #[rstest::rstest]
    fn test_transform_pnl_missing_account_id() {
        use rithmic_rs::rti::AccountPnLPositionUpdate;

        let update = AccountPnLPositionUpdate {
            template_id: 450,
            is_snapshot: None,
            fcm_id: None,
            ib_id: None,
            account_id: None, // Missing account_id
            fill_buy_qty: None,
            fill_sell_qty: None,
            order_buy_qty: None,
            order_sell_qty: None,
            buy_qty: None,
            sell_qty: None,
            open_long_options_value: None,
            open_short_options_value: None,
            closed_options_value: None,
            option_cash_reserved: None,
            rms_account_commission: None,
            open_position_pnl: None,
            open_position_quantity: None,
            closed_position_pnl: None,
            closed_position_quantity: None,
            net_quantity: None,
            excess_buy_margin: None,
            margin_balance: None,
            min_margin_balance: None,
            min_account_balance: None,
            account_balance: None,
            cash_on_hand: None,
            option_closed_pnl: None,
            percent_maximum_allowable_loss: None,
            option_open_pnl: None,
            mtm_account: None,
            available_buying_power: None,
            used_buying_power: None,
            reserved_buying_power: None,
            excess_sell_margin: None,
            day_open_pnl: None,
            day_closed_pnl: None,
            day_pnl: None,
            day_open_pnl_offset: None,
            day_closed_pnl_offset: None,
            ssboe: None,
            usecs: None,
        };

        let event = transform_pnl_for_test(RithmicMessage::AccountPnLPositionUpdate(update));
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn test_transform_account_pnl_missing_required_balance_is_dropped() {
        use rithmic_rs::rti::AccountPnLPositionUpdate;

        let update = AccountPnLPositionUpdate {
            template_id: 450,
            is_snapshot: Some(false),
            account_id: Some("ACCOUNT123".to_string()),
            ssboe: Some(1_704_067_200),
            usecs: Some(0),
            ..Default::default()
        };

        let event = transform_pnl_for_test(RithmicMessage::AccountPnLPositionUpdate(update));
        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn live_time_bar_without_marker_is_dropped() {
        let instruments = AHashMap::from_iter([(
            "CME:ESM6".to_string(),
            InstrumentInfo {
                symbol: "ESM6".to_string(),
                exchange: "CME".to_string(),
                tick_size: Some(0.25),
                point_value: None,
                product_code: None,
                description: None,
                currency: None,
                is_tradeable: true,
            },
        )]);

        let event = transform_history_market_data_for_test(
            RithmicMessage::TimeBar(rithmic_rs::rti::TimeBar {
                template_id: 250,
                symbol: Some("ESM6".to_string()),
                exchange: Some("CME".to_string()),
                r#type: Some(rithmic_rs::rti::request_time_bar_update::BarType::MinuteBar as i32),
                period: Some("1".to_string()),
                marker: None,
                open_price: Some(4500.75),
                close_price: Some(4501.25),
                high_price: Some(4501.50),
                low_price: Some(4500.50),
                volume: Some(110),
                ..Default::default()
            }),
            &instruments,
        );

        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn live_time_bar_missing_required_ohlcv_is_dropped() {
        let instruments = test_instruments();
        let event = transform_history_market_data_for_test(
            RithmicMessage::TimeBar(rithmic_rs::rti::TimeBar {
                template_id: 250,
                symbol: Some("ESM6".to_string()),
                exchange: Some("CME".to_string()),
                r#type: Some(rithmic_rs::rti::request_time_bar_update::BarType::MinuteBar as i32),
                period: Some("1".to_string()),
                marker: Some(1_704_067_200),
                open_price: None,
                close_price: Some(4501.25),
                high_price: Some(4501.50),
                low_price: Some(4500.50),
                volume: Some(110),
                ..Default::default()
            }),
            &instruments,
        );

        assert!(event.is_none());
    }

    #[rstest::rstest]
    fn should_resume_time_bar_history_when_last_bar_ends_before_requested_end() {
        let message =
            RithmicMessage::ResponseTimeBarReplay(rithmic_rs::rti::ResponseTimeBarReplay {
                template_id: 203,
                request_key: Some("history-req".to_string()),
                period: Some("1".to_string()),
                marker: Some(1_700_000_000),
                ..Default::default()
            });

        assert!(should_resume_time_bar_history(
            [&message].into_iter(),
            TimeBarType::MinuteBar,
            1,
            1_700_000_180,
        ));
    }

    #[rstest::rstest]
    fn should_not_resume_time_bar_history_once_requested_end_is_covered() {
        let message =
            RithmicMessage::ResponseTimeBarReplay(rithmic_rs::rti::ResponseTimeBarReplay {
                template_id: 203,
                request_key: Some("history-req".to_string()),
                period: Some("1".to_string()),
                marker: Some(1_700_000_120),
                ..Default::default()
            });

        assert!(!should_resume_time_bar_history(
            [&message].into_iter(),
            TimeBarType::MinuteBar,
            1,
            1_700_000_180,
        ));
    }

    #[rstest::rstest]
    fn test_parse_optional_f64() {
        // Valid number
        assert_eq!(
            parse_optional_f64(&Some("123.45".to_string())),
            Some(123.45)
        );

        // Integer
        assert_eq!(parse_optional_f64(&Some("100".to_string())), Some(100.0));

        // Negative
        assert_eq!(
            parse_optional_f64(&Some("-50.25".to_string())),
            Some(-50.25)
        );

        // None
        assert_eq!(parse_optional_f64(&None), None);

        // Invalid string
        assert_eq!(parse_optional_f64(&Some("invalid".to_string())), None);

        // Empty string
        assert_eq!(parse_optional_f64(&Some(String::new())), None);

        // Non-finite protocol values
        assert_eq!(parse_optional_f64(&Some("NaN".to_string())), None);
        assert_eq!(parse_optional_f64(&Some("inf".to_string())), None);
    }
}
