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

//! Rithmic execution client implementation.

use std::{fmt::Debug, sync::Arc};

use dashmap::DashMap;
use nautilus_common::live::get_runtime;
use nautilus_model::types::Price;
use rithmic_rs::{
    OrderSide, OrderStatus, OrderType, RithmicAccount, RithmicCancelOrder,
    RithmicError as RithmicApiError, RithmicModifyOrder, RithmicOrder, TimeInForce, TrailingStop,
    api::RithmicResponse, plants::order_plant::RithmicOrderPlantHandle,
    rti::messages::RithmicMessage,
};
use thiserror::Error;
use tokio::{
    sync::{RwLock, mpsc},
    task::JoinHandle,
};

use crate::{
    common::{
        enums::ConnectionState,
        types::{ClientOrderIdStr, RithmicAccountId, RithmicOrderId, UnixNanos},
    },
    error::{Result, RithmicError},
    gateway::RithmicGateway,
};

/// Order submitted event.
#[derive(Debug, Clone)]
pub struct OrderSubmitted {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Venue order ID (from Rithmic).
    pub venue_order_id: Option<RithmicOrderId>,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Order accepted event.
#[derive(Debug, Clone)]
pub struct OrderAccepted {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Venue order ID.
    pub venue_order_id: RithmicOrderId,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Order rejected event.
#[derive(Debug, Clone)]
pub struct OrderRejected {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Rejection reason.
    pub reason: String,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Order filled event.
#[derive(Debug, Clone)]
pub struct OrderFilled {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Venue order ID.
    pub venue_order_id: RithmicOrderId,
    /// Fill price.
    pub fill_price: f64,
    /// Fill quantity.
    pub fill_qty: f64,
    /// Remaining quantity, when provided by Rithmic.
    pub leaves_qty: Option<f64>,
    /// Commission.
    pub commission: f64,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue trade identifier, when provided by Rithmic.
    pub trade_id: Option<String>,
    /// Fill currency, when provided by Rithmic.
    pub currency: Option<String>,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Order cancelled event.
#[derive(Debug, Clone)]
pub struct OrderCancelled {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Venue order ID.
    pub venue_order_id: RithmicOrderId,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Order modified event.
#[derive(Debug, Clone)]
pub struct OrderModified {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Account ID.
    pub account_id: RithmicAccountId,
    /// Venue order ID.
    pub venue_order_id: RithmicOrderId,
    /// New price (if modified).
    pub new_price: Option<f64>,
    /// New quantity (if modified).
    pub new_qty: Option<f64>,
    /// Timestamp.
    pub ts_event: UnixNanos,
    /// Venue order context captured from the notification payload.
    pub context: OrderContext,
}

/// Venue order context used to rebuild Python-side order state after reconnect.
#[derive(Debug, Clone, Default)]
pub struct OrderContext {
    /// Whether the source notification was a Rithmic snapshot/replay payload.
    pub is_snapshot: bool,
    /// Instrument symbol.
    pub symbol: Option<String>,
    /// Exchange code.
    pub exchange: Option<String>,
    /// Order side.
    pub side: Option<OrderSide>,
    /// Order type.
    pub order_type: Option<OrderType>,
    /// Time in force.
    pub time_in_force: Option<TimeInForce>,
    /// Original order quantity.
    pub quantity: Option<f64>,
    /// Cumulative filled quantity.
    pub filled_qty: Option<f64>,
    /// Remaining open quantity.
    pub leaves_qty: Option<f64>,
    /// Order price.
    pub price: Option<f64>,
    /// Stop or trigger price.
    pub trigger_price: Option<f64>,
    /// Average fill price.
    pub avg_price: Option<f64>,
    /// Parent venue basket ID for bracket child notifications.
    pub original_basket_id: Option<String>,
    /// Linked venue basket IDs for contingent orders.
    pub linked_basket_ids: Vec<String>,
    /// Venue bracket type when provided.
    pub bracket_type: Option<String>,
}

/// Execution event emitted by the execution client.
#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    /// Order was submitted.
    Submitted(OrderSubmitted),
    /// Order was accepted by venue.
    Accepted(OrderAccepted),
    /// Order was rejected.
    Rejected(OrderRejected),
    /// Order was filled (partial or complete).
    Filled(OrderFilled),
    /// Order was cancelled.
    Cancelled(OrderCancelled),
    /// Order was modified.
    Modified(OrderModified),
    /// Connection state change.
    ConnectionState(ConnectionState),
    /// Successfully reconnected after disconnect.
    Reconnected,
    /// Successfully authenticated with venue.
    Authenticated,
    /// Error event.
    Error(String),
}

impl ExecutionEvent {
    /// Returns the Rithmic account ID associated with the event, if any.
    pub fn account_id(&self) -> Option<&str> {
        match self {
            Self::Submitted(event) => Some(&event.account_id),
            Self::Accepted(event) => Some(&event.account_id),
            Self::Rejected(event) => Some(&event.account_id),
            Self::Filled(event) => Some(&event.account_id),
            Self::Cancelled(event) => Some(&event.account_id),
            Self::Modified(event) => Some(&event.account_id),
            Self::ConnectionState(_) | Self::Reconnected | Self::Authenticated | Self::Error(_) => {
                None
            }
        }
    }
}

/// Trailing stop configuration for orders.
#[derive(Debug, Clone)]
pub struct TrailingStopConfig {
    /// Number of ticks to trail behind the market price.
    pub trail_by_ticks: i32,
}

/// Order request to submit.
#[derive(Debug, Clone)]
pub struct OrderRequest {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Instrument symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Order side.
    pub side: OrderSide,
    /// Order type.
    pub order_type: OrderType,
    /// Time in force.
    pub time_in_force: TimeInForce,
    /// Quantity.
    pub quantity: f64,
    /// Limit price (for limit orders).
    pub price: Option<f64>,
    /// Stop/trigger price (for stop orders).
    pub stop_price: Option<f64>,
    /// Trailing stop configuration (optional).
    pub trailing_stop: Option<TrailingStopConfig>,
}

/// Certainty of the venue outcome after an order command fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandFailureKind {
    /// The command definitely did not result in an accepted venue operation.
    Definitive,
    /// The venue outcome is unknown and must be reconciled.
    Unknown,
}

/// Classified order-command failure which preserves the typed `rithmic-rs` source.
#[derive(Debug, Error)]
#[error("{message}")]
pub(crate) struct OrderCommandError {
    kind: CommandFailureKind,
    message: String,
    #[source]
    source: Option<OrderCommandSource>,
}

#[derive(Debug, Error)]
enum OrderCommandSource {
    #[error(transparent)]
    Api(#[from] RithmicApiError),
    #[error(transparent)]
    Adapter(#[from] RithmicError),
}

impl OrderCommandError {
    fn definitive(message: impl Into<String>) -> Self {
        Self {
            kind: CommandFailureKind::Definitive,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn unknown(message: impl Into<String>) -> Self {
        Self {
            kind: CommandFailureKind::Unknown,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn from_api(source: RithmicApiError) -> Self {
        let kind = classify_api_error(&source);
        Self {
            kind,
            message: source.to_string(),
            source: Some(source.into()),
        }
    }

    fn from_adapter(source: RithmicError) -> Self {
        Self {
            kind: CommandFailureKind::Definitive,
            message: source.to_string(),
            source: Some(source.into()),
        }
    }

    /// Returns the certainty of the venue outcome.
    pub(crate) const fn kind(&self) -> CommandFailureKind {
        self.kind
    }

    /// Returns whether the command definitively failed.
    pub(crate) const fn is_definitive(&self) -> bool {
        matches!(self.kind, CommandFailureKind::Definitive)
    }
}

/// Order state tracking.
#[derive(Debug, Clone)]
pub struct OrderState {
    /// Client order ID.
    pub client_order_id: ClientOrderIdStr,
    /// Venue order ID (basket_id from Rithmic).
    pub venue_order_id: Option<RithmicOrderId>,
    /// Instrument symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Original order side.
    pub side: OrderSide,
    /// Order type (needed for modify).
    pub order_type: OrderType,
    /// Original time-in-force.
    pub time_in_force: TimeInForce,
    /// Original working price, if applicable.
    pub price: Option<f64>,
    /// Original trigger/stop price, if applicable.
    pub trigger_price: Option<f64>,
    /// Current status.
    pub status: OrderStatus,
    /// Original quantity.
    pub quantity: f64,
    /// Filled quantity.
    pub filled_qty: f64,
    /// Remaining quantity.
    pub leaves_qty: f64,
    /// Average fill price.
    pub avg_price: f64,
}

/// Rithmic execution client for order management.
///
/// This client uses a `RithmicOrderPlantHandle` to send order commands
/// to the Rithmic order plant. Events (fills, cancels, etc.) are received
/// via the gateway's execution event channel.
///
/// # Example
///
/// ```rust,ignore
/// use rithmic_nt::RithmicExecutionClient;
///
/// // Get handle from gateway after connection
/// let handle = gateway.order_handle(&account).unwrap().clone();
/// let client = RithmicExecutionClient::new(
///     gateway,
///     rithmic_rs::RithmicAccount::new("FCM", "IB", "ACCOUNT123"),
/// );
///
/// // Submit an order
/// client.submit_order(request).await?;
/// ```
pub struct RithmicExecutionClient {
    gateway: Arc<RwLock<RithmicGateway>>,
    account: RithmicAccount,
    account_id: String,
    orders: DashMap<ClientOrderIdStr, OrderState>,
    venue_to_client: DashMap<RithmicOrderId, ClientOrderIdStr>,
    event_tx: Option<mpsc::UnboundedSender<ExecutionEvent>>,
}

fn first_response_error(responses: &[RithmicResponse]) -> Option<String> {
    responses
        .iter()
        .find_map(|response| response.error.as_ref().map(|e| e.to_string()))
}

fn classify_api_error(e: &RithmicApiError) -> CommandFailureKind {
    match e {
        RithmicApiError::RequestRejected(_) | RithmicApiError::InvalidArgument(_) => {
            CommandFailureKind::Definitive
        }
        _ => CommandFailureKind::Unknown,
    }
}

pub(crate) fn first_command_response_error(
    responses: &[RithmicResponse],
) -> Option<OrderCommandError> {
    responses.iter().find_map(|response| {
        response
            .error
            .as_ref()
            .cloned()
            .map(OrderCommandError::from_api)
    })
}

fn whole_contracts(quantity: f64) -> std::result::Result<i32, OrderCommandError> {
    if !quantity.is_finite() {
        return Err(OrderCommandError::definitive(format!(
            "Quantity must be finite, received: {quantity}"
        )));
    }

    if quantity <= 0.0 {
        return Err(OrderCommandError::definitive(format!(
            "Quantity must be positive, received: {quantity}"
        )));
    }

    if quantity.fract() != 0.0 {
        return Err(OrderCommandError::definitive(format!(
            "Quantity must be a whole number (contracts), received: {quantity}"
        )));
    }

    if quantity > f64::from(i32::MAX) {
        return Err(OrderCommandError::definitive(format!(
            "Quantity exceeds maximum: {quantity}"
        )));
    }

    // The finite, positive, integral value was bounded to `i32::MAX` above.
    Ok(quantity as i32)
}

fn is_positive_representable_price(value: f64) -> bool {
    value > 0.0 && Price::new_checked(value, 9).is_ok_and(|price| price.as_f64() > 0.0)
}

pub(crate) fn validate_command_price(
    price: f64,
    label: &str,
) -> std::result::Result<(), OrderCommandError> {
    if !is_positive_representable_price(price) {
        return Err(OrderCommandError::definitive(format!(
            "{label} must be positive and representable as a Nautilus price, received: {price}"
        )));
    }
    Ok(())
}

fn validate_order_request(request: &OrderRequest) -> std::result::Result<(), OrderCommandError> {
    if request.client_order_id.trim().is_empty() {
        return Err(OrderCommandError::definitive(
            "Client order ID must not be empty",
        ));
    }
    if request.symbol.trim().is_empty() || request.exchange.trim().is_empty() {
        return Err(OrderCommandError::definitive(
            "Order symbol and exchange must not be empty",
        ));
    }
    match request.side {
        OrderSide::Buy | OrderSide::Sell => {}
        _ => return Err(OrderCommandError::definitive("Unsupported order side")),
    }
    match request.order_type {
        OrderType::Market | OrderType::Limit | OrderType::StopMarket | OrderType::StopLimit => {}
        _ => return Err(OrderCommandError::definitive("Unsupported order type")),
    }
    match request.time_in_force {
        TimeInForce::Day | TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Fok => {}
        _ => {
            return Err(OrderCommandError::definitive("Unsupported time-in-force"));
        }
    }

    whole_contracts(request.quantity)?;
    if let Some(price) = request.price {
        validate_command_price(price, "Order price")?;
    }
    if let Some(stop_price) = request.stop_price {
        validate_command_price(stop_price, "Stop price")?;
    }
    if let Some(trailing_stop) = &request.trailing_stop {
        if trailing_stop.trail_by_ticks <= 0 {
            return Err(OrderCommandError::definitive(
                "Trailing stop ticks must be positive",
            ));
        }
        if !matches!(
            request.order_type,
            OrderType::StopMarket | OrderType::StopLimit
        ) {
            return Err(OrderCommandError::definitive(
                "Trailing stop is only supported for stop orders",
            ));
        }
    }

    if matches!(request.order_type, OrderType::Limit | OrderType::StopLimit)
        && request.price.is_none()
    {
        return Err(OrderCommandError::definitive(
            "Limit/StopLimit order requires price",
        ));
    }
    if matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit
    ) && request.stop_price.is_none()
        && request.trailing_stop.is_none()
    {
        return Err(OrderCommandError::definitive(
            "Stop order requires stop_price or trailing stop",
        ));
    }
    Ok(())
}

fn checked_rithmic_timestamp(ssboe: Option<i32>, usecs: Option<i32>) -> Option<u64> {
    let ssboe = ssboe.filter(|value| *value >= 0)?;
    let usecs = usecs.filter(|value| (0..1_000_000).contains(value))?;
    (ssboe as u64)
        .checked_mul(1_000_000_000)?
        .checked_add((usecs as u64).checked_mul(1_000)?)
}

fn validate_optional_positive(
    value: Option<f64>,
    message: &'static str,
) -> std::result::Result<(), &'static str> {
    if value.is_some_and(|value| !value.is_finite() || value <= 0.0) {
        return Err(message);
    }
    Ok(())
}

fn validate_optional_non_negative(
    value: Option<f64>,
    message: &'static str,
) -> std::result::Result<(), &'static str> {
    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(message);
    }
    Ok(())
}

fn validate_optional_price(
    value: Option<f64>,
    message: &'static str,
) -> std::result::Result<(), &'static str> {
    if value.is_some_and(|value| !is_positive_representable_price(value)) {
        return Err(message);
    }
    Ok(())
}

fn validate_order_context(context: &OrderContext) -> std::result::Result<(), &'static str> {
    validate_optional_positive(
        context.quantity,
        "Rithmic order quantity must be finite and positive",
    )?;
    validate_optional_non_negative(
        context.filled_qty,
        "Rithmic filled quantity must be finite and non-negative",
    )?;
    validate_optional_non_negative(
        context.leaves_qty,
        "Rithmic leaves quantity must be finite and non-negative",
    )?;
    validate_optional_price(
        context.price,
        "Rithmic order price must be positive and representable",
    )?;
    validate_optional_price(
        context.trigger_price,
        "Rithmic trigger price must be positive and representable",
    )?;
    validate_optional_price(
        context.avg_price,
        "Rithmic average fill price must be positive and representable",
    )?;
    Ok(())
}

pub(crate) fn validate_execution_event(
    event: &ExecutionEvent,
) -> std::result::Result<(), &'static str> {
    match event {
        ExecutionEvent::Submitted(event) => validate_order_context(&event.context),
        ExecutionEvent::Accepted(event) => validate_order_context(&event.context),
        ExecutionEvent::Rejected(event) => validate_order_context(&event.context),
        ExecutionEvent::Filled(event) => {
            validate_order_context(&event.context)?;
            validate_optional_price(
                Some(event.fill_price),
                "Rithmic fill price must be positive and representable",
            )?;
            validate_optional_positive(
                Some(event.fill_qty),
                "Rithmic fill quantity must be finite and positive",
            )?;
            validate_optional_non_negative(
                event.leaves_qty,
                "Rithmic fill leaves quantity must be finite and non-negative",
            )?;
            if !event.commission.is_finite() {
                return Err("Rithmic fill commission must be finite");
            }
            Ok(())
        }
        ExecutionEvent::Cancelled(event) => validate_order_context(&event.context),
        ExecutionEvent::Modified(event) => {
            validate_order_context(&event.context)?;
            validate_optional_price(
                event.new_price,
                "Rithmic modified price must be positive and representable",
            )?;
            validate_optional_positive(
                event.new_qty,
                "Rithmic modified quantity must be finite and positive",
            )
        }
        ExecutionEvent::ConnectionState(_)
        | ExecutionEvent::Reconnected
        | ExecutionEvent::Authenticated
        | ExecutionEvent::Error(_) => Ok(()),
    }
}

fn status_rank(status: OrderStatus) -> u8 {
    match status {
        OrderStatus::Pending => 1,
        OrderStatus::Open => 2,
        OrderStatus::Partial => 3,
        OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired => 4,
        OrderStatus::Complete => 5,
        OrderStatus::Unknown => 0,
        _ => 0,
    }
}

fn order_state_from_context(
    client_order_id: &str,
    venue_order_id: Option<&str>,
    status: OrderStatus,
    context: &OrderContext,
    fill_qty: Option<f64>,
    leaves_qty: Option<f64>,
    avg_price: Option<f64>,
) -> Option<OrderState> {
    let symbol = context.symbol.clone()?;
    let exchange = context.exchange.clone()?;
    let side = context.side?;
    let order_type = context.order_type?;
    let time_in_force = context.time_in_force?;
    let quantity = context.quantity.or_else(|| {
        fill_qty
            .zip(leaves_qty)
            .map(|(filled, leaves)| filled + leaves)
    })?;
    let reported_leaves = context.leaves_qty.or(leaves_qty);
    let filled_qty = context
        .filled_qty
        .or(fill_qty)
        .or_else(|| reported_leaves.map(|leaves| (quantity - leaves).max(0.0)))?;
    let leaves_qty = reported_leaves.unwrap_or_else(|| (quantity - filled_qty).max(0.0));

    Some(OrderState {
        client_order_id: client_order_id.to_string(),
        venue_order_id: venue_order_id.map(ToOwned::to_owned),
        symbol,
        exchange,
        side,
        order_type,
        time_in_force,
        price: context.price,
        trigger_price: context.trigger_price,
        status,
        quantity,
        filled_qty,
        leaves_qty,
        avg_price: avg_price.or(context.avg_price).unwrap_or_default(),
    })
}

impl RithmicExecutionClient {
    /// Creates a new execution client with the given order plant handle.
    ///
    /// # Arguments
    /// * `handle` - Order plant handle from the gateway
    /// * `account_id` - Trading account ID
    pub fn new(gateway: Arc<RwLock<RithmicGateway>>, account: RithmicAccount) -> Self {
        Self {
            gateway,
            account_id: account.account_id.clone(),
            account,
            orders: DashMap::new(),
            venue_to_client: DashMap::new(),
            event_tx: None,
        }
    }

    /// Returns the account ID.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Returns the typed Rithmic account identity.
    pub fn account(&self) -> &RithmicAccount {
        &self.account
    }

    fn mark_submit_rejected(&self, client_order_id: &str) {
        self.update_order_state(
            client_order_id,
            None,
            OrderStatus::Rejected,
            None,
            Some(0.0),
            None,
        );
    }

    async fn order_handle(&self) -> Result<RithmicOrderPlantHandle> {
        let gateway = self.gateway.read().await;
        gateway
            .order_handle(&self.account)
            .ok_or_else(|| RithmicError::Connection("Order plant not connected".to_string()))
    }

    async fn pnl_handle(&self) -> Result<rithmic_rs::RithmicPnlPlantHandle> {
        let gateway = self.gateway.read().await;
        gateway
            .pnl_handle(&self.account)
            .ok_or_else(|| RithmicError::Connection("PnL plant not connected".to_string()))
    }

    /// Submits an order to Rithmic.
    ///
    /// The order is tracked locally and submitted via the order plant.
    /// Order events (submitted, accepted, filled, etc.) will be received
    /// through the gateway's execution event channel.
    ///
    /// # Supported Order Types
    ///
    /// - **Market**: Execute immediately at market price
    /// - **Limit**: Execute at specified price or better
    /// - **StopMarket**: Trigger at stop_price, then execute as market order
    /// - **StopLimit**: Trigger at stop_price, then execute as limit order at price
    ///
    /// # Trailing Stops
    ///
    /// Set `trailing_stop` to enable trailing stop functionality. The stop price
    /// will trail the market by the specified number of ticks.
    pub async fn submit_order(&self, request: OrderRequest) -> Result<()> {
        self.submit_order_classified(request)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))
    }

    pub(crate) async fn submit_order_classified(
        &self,
        request: OrderRequest,
    ) -> std::result::Result<(), OrderCommandError> {
        validate_order_request(&request)?;
        let quantity = whole_contracts(request.quantity)?;
        if self.orders.contains_key(&request.client_order_id) {
            return Err(OrderCommandError::definitive(format!(
                "Duplicate client order ID: {}",
                request.client_order_id
            )));
        }

        let handle = self
            .order_handle()
            .await
            .map_err(OrderCommandError::from_adapter)?;

        // Track order locally
        let order_state = OrderState {
            client_order_id: request.client_order_id.clone(),
            venue_order_id: None,
            symbol: request.symbol.clone(),
            exchange: request.exchange.clone(),
            side: request.side,
            order_type: request.order_type,
            time_in_force: request.time_in_force,
            price: request.price,
            trigger_price: request.stop_price,
            status: OrderStatus::Pending,
            quantity: request.quantity,
            filled_qty: 0.0,
            leaves_qty: request.quantity,
            avg_price: 0.0,
        };
        self.orders
            .insert(request.client_order_id.clone(), order_state);

        // Determine prices based on order type
        // Note: For Limit/StopLimit, price is guaranteed to exist due to prior validation
        let price = match request.order_type {
            OrderType::Market | OrderType::StopMarket => 0.0,
            OrderType::Limit | OrderType::StopLimit => request.price.ok_or_else(|| {
                OrderCommandError::definitive("Limit order price is required for submission")
            })?,
            _ => return Err(OrderCommandError::definitive("Unsupported order type")),
        };

        let trigger_price = match request.order_type {
            OrderType::StopMarket | OrderType::StopLimit => request.stop_price,
            _ => None,
        };

        // Convert trailing stop config
        let trailing_stop = request.trailing_stop.map(|ts| TrailingStop {
            trail_by_ticks: ts.trail_by_ticks,
        });

        tracing::debug!(
            "Submitting order: client_id={}, symbol={}, exchange={}, qty={}, price={}, trigger={:?}, side={:?}, type={:?}, trailing={:?}",
            request.client_order_id,
            request.symbol,
            request.exchange,
            request.quantity,
            price,
            trigger_price,
            request.side,
            request.order_type,
            trailing_stop
        );

        let tracking_symbol = request.symbol.clone();
        let tracking_exchange = request.exchange.clone();

        // Build RithmicOrder - use Into traits for automatic conversion
        let order = RithmicOrder {
            symbol: request.symbol,
            exchange: request.exchange,
            quantity,
            price,
            transaction_type: request.side.into(),
            price_type: request.order_type.into(),
            user_tag: request.client_order_id.clone(),
            duration: Some(request.time_in_force.into()),
            trigger_price,
            trailing_stop,
        };

        // Submit to Rithmic using the new place_order API
        let responses = match handle.place_order(order).await {
            Ok(responses) => responses,
            Err(e) => {
                let failure = OrderCommandError::from_api(e);

                if failure.is_definitive() {
                    self.mark_submit_rejected(&request.client_order_id);
                }
                return Err(failure);
            }
        };

        let mut submitted_event: Option<ExecutionEvent> = None;

        for response in &responses {
            if let Some(source) = response.error.as_ref().cloned() {
                let failure = OrderCommandError::from_api(source);

                if failure.is_definitive() {
                    // The live adapter is the single authority for emitting the rejection.
                    // This layer only updates its local state so direct command responses cannot
                    // race or duplicate a gateway notification.
                    self.mark_submit_rejected(&request.client_order_id);
                }
                return Err(failure);
            }

            match &response.message {
                RithmicMessage::ResponseNewOrder(resp) => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        user_tag = ?resp.user_tag,
                        basket_id = ?resp.basket_id,
                        rp_code = ?resp.rp_code,
                        rq_handler_rp_code = ?resp.rq_handler_rp_code,
                        user_msg = ?resp.user_msg,
                        "place_order returned response_new_order"
                    );

                    let matches_request =
                        resp.user_tag.as_deref() == Some(request.client_order_id.as_str());
                    let has_venue_identity = resp.basket_id.is_some();

                    if matches_request || has_venue_identity {
                        let Some(ts_event) = checked_rithmic_timestamp(resp.ssboe, resp.usecs)
                        else {
                            tracing::warn!(
                                request_id = %response.request_id,
                                ssboe = ?resp.ssboe,
                                usecs = ?resp.usecs,
                                "ignoring new-order acknowledgement with invalid timestamp"
                            );
                            continue;
                        };
                        submitted_event = Some(ExecutionEvent::Submitted(OrderSubmitted {
                            client_order_id: request.client_order_id.clone(),
                            venue_order_id: resp.basket_id.clone(),
                            account_id: self.account_id.clone(),
                            ts_event,
                            context: OrderContext {
                                symbol: Some(tracking_symbol.clone()),
                                exchange: Some(tracking_exchange.clone()),
                                side: Some(request.side),
                                order_type: Some(request.order_type),
                                time_in_force: Some(request.time_in_force),
                                quantity: Some(request.quantity),
                                filled_qty: Some(0.0),
                                leaves_qty: Some(request.quantity),
                                price: request.price,
                                trigger_price: request.stop_price,
                                avg_price: None,
                                ..Default::default()
                            },
                        }));
                    } else {
                        tracing::debug!(
                            request_id = %response.request_id,
                            "ignoring response_new_order without matching user_tag or basket_id"
                        );
                    }
                }
                other => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        message_kind = ?std::mem::discriminant(other),
                        "place_order returned non-new-order response"
                    );
                }
            }
        }

        let event = submitted_event.ok_or_else(|| {
            OrderCommandError::unknown(format!(
                "No matching new-order acknowledgement for {}",
                request.client_order_id
            ))
        })?;
        self.apply_event(&event);

        tracing::debug!("Order submitted: {}", request.client_order_id);
        Ok(())
    }

    /// Modifies an existing order.
    ///
    /// The order must exist locally and have a venue_order_id (must be accepted).
    ///
    /// # Arguments
    /// * `client_order_id` - The client order ID
    /// * `new_qty` - New quantity (optional)
    /// * `new_price` - New price (optional)
    pub async fn modify_order(
        &self,
        client_order_id: &str,
        new_qty: Option<f64>,
        new_price: Option<f64>,
    ) -> Result<()> {
        self.modify_order_classified(client_order_id, new_qty, new_price)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))
    }

    pub(crate) async fn modify_order_classified(
        &self,
        client_order_id: &str,
        new_qty: Option<f64>,
        new_price: Option<f64>,
    ) -> std::result::Result<(), OrderCommandError> {
        if new_qty.is_none() && new_price.is_none() {
            return Err(OrderCommandError::definitive(
                "Modify order requires a new quantity or price",
            ));
        }
        if let Some(price) = new_price {
            validate_command_price(price, "Modified price")?;
        }
        let new_contracts = new_qty.map(whole_contracts).transpose()?;
        let (modify_request, venue_order_id) = {
            let order = self.orders.get(client_order_id).ok_or_else(|| {
                OrderCommandError::definitive(format!("Order not found: {client_order_id}"))
            })?;
            let venue_order_id = order
                .venue_order_id
                .clone()
                .ok_or_else(|| OrderCommandError::definitive("Order not yet accepted by venue"))?;
            let quantity = match new_contracts {
                Some(quantity) => quantity,
                None => whole_contracts(order.leaves_qty)?,
            };
            let price = match order.order_type {
                OrderType::Limit | OrderType::StopLimit => {
                    let price = new_price.or(order.price).ok_or_else(|| {
                        OrderCommandError::definitive(
                            "Limit/StopLimit order requires a price for modification",
                        )
                    })?;
                    validate_command_price(price, "Modified price")?;
                    price
                }
                OrderType::Market | OrderType::StopMarket => {
                    if new_price.is_some() {
                        return Err(OrderCommandError::definitive(
                            "Price modification is unsupported for Market/StopMarket orders",
                        ));
                    }
                    0.0
                }
                _ => return Err(OrderCommandError::definitive("Unsupported order type")),
            };

            (
                RithmicModifyOrder {
                    id: venue_order_id.clone(),
                    exchange: order.exchange.clone(),
                    symbol: order.symbol.clone(),
                    qty: quantity,
                    price,
                    price_type: order.order_type.into(),
                },
                venue_order_id,
            )
        };
        let handle = self
            .order_handle()
            .await
            .map_err(OrderCommandError::from_adapter)?;

        tracing::debug!(
            "Modifying order: client_id={}, venue_id={}, new_qty={:?}, new_price={:?}",
            client_order_id,
            venue_order_id,
            new_qty,
            new_price
        );

        let responses = handle
            .modify_order(modify_request)
            .await
            .map_err(OrderCommandError::from_api)?;

        if let Some(failure) = first_command_response_error(&responses) {
            return Err(failure);
        }

        for response in &responses {
            match &response.message {
                RithmicMessage::ResponseModifyOrder(resp) => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        basket_id = ?resp.basket_id,
                        rp_code = ?resp.rp_code,
                        rq_handler_rp_code = ?resp.rq_handler_rp_code,
                        user_msg = ?resp.user_msg,
                        "modify_order returned response_modify_order"
                    );
                }
                other => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        message_kind = ?std::mem::discriminant(other),
                        "modify_order returned non-modify-order response"
                    );
                }
            }
        }

        if !responses
            .iter()
            .any(|response| matches!(&response.message, RithmicMessage::ResponseModifyOrder(_)))
        {
            return Err(OrderCommandError::unknown(format!(
                "No modify-order acknowledgement for {client_order_id}"
            )));
        }

        Ok(())
    }

    /// Cancels an order.
    ///
    /// The order must exist locally and have a venue_order_id (must be accepted).
    pub async fn cancel_order(&self, client_order_id: &str) -> Result<()> {
        self.cancel_order_classified(client_order_id)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))
    }

    pub(crate) async fn cancel_order_classified(
        &self,
        client_order_id: &str,
    ) -> std::result::Result<(), OrderCommandError> {
        let venue_order_id = self
            .orders
            .get(client_order_id)
            .ok_or_else(|| {
                OrderCommandError::definitive(format!("Order not found: {client_order_id}"))
            })?
            .venue_order_id
            .clone()
            .ok_or_else(|| OrderCommandError::definitive("Order not yet accepted by venue"))?;
        let handle = self
            .order_handle()
            .await
            .map_err(OrderCommandError::from_adapter)?;

        tracing::debug!(
            "Cancelling order: client_id={}, venue_id={}",
            client_order_id,
            venue_order_id
        );

        let cancel_request = RithmicCancelOrder { id: venue_order_id };

        let responses = handle
            .cancel_order(cancel_request)
            .await
            .map_err(OrderCommandError::from_api)?;

        if let Some(failure) = first_command_response_error(&responses) {
            return Err(failure);
        }

        for response in &responses {
            match &response.message {
                RithmicMessage::ResponseCancelOrder(resp) => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        basket_id = ?resp.basket_id,
                        rp_code = ?resp.rp_code,
                        rq_handler_rp_code = ?resp.rq_handler_rp_code,
                        user_msg = ?resp.user_msg,
                        "cancel_order returned response_cancel_order"
                    );
                }
                other => {
                    tracing::debug!(
                        request_id = %response.request_id,
                        source = %response.source,
                        error = ?response.error,
                        message_kind = ?std::mem::discriminant(other),
                        "cancel_order returned non-cancel-order response"
                    );
                }
            }
        }

        if !responses
            .iter()
            .any(|response| matches!(&response.message, RithmicMessage::ResponseCancelOrder(_)))
        {
            return Err(OrderCommandError::unknown(format!(
                "No cancel-order acknowledgement for {client_order_id}"
            )));
        }

        Ok(())
    }

    /// Cancels all open orders.
    pub async fn cancel_all_orders(&self) -> Result<()> {
        self.cancel_all_orders_classified()
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))
    }

    pub(crate) async fn cancel_all_orders_classified(
        &self,
    ) -> std::result::Result<(), OrderCommandError> {
        let handle = self
            .order_handle()
            .await
            .map_err(OrderCommandError::from_adapter)?;
        tracing::debug!("Cancelling all orders");

        let response = handle
            .cancel_all_orders()
            .await
            .map_err(OrderCommandError::from_api)?;

        if let Some(e) = response.error {
            return Err(OrderCommandError::from_api(e));
        }

        if !matches!(response.message, RithmicMessage::ResponseCancelAllOrders(_)) {
            return Err(OrderCommandError::unknown(
                "No cancel-all acknowledgement from Rithmic",
            ));
        }

        Ok(())
    }

    /// Cancels a batch of orders by client order ID.
    ///
    /// This iterates through the provided order IDs and cancels each one.
    /// Every child outcome is evaluated; the batch fails if any child fails.
    ///
    /// # Returns
    ///
    /// Returns the number of acknowledged cancellations when all children succeed.
    pub async fn batch_cancel_orders(&self, client_order_ids: &[&str]) -> Result<usize> {
        tracing::debug!("Batch cancelling {} orders", client_order_ids.len());
        let outcomes = self.batch_cancel_orders_classified(client_order_ids).await;
        let mut failures = Vec::new();

        for (client_order_id, outcome) in outcomes {
            if let Err(e) = outcome {
                failures.push(format!("{client_order_id}: {e}"));
            }
        }

        if failures.is_empty() {
            Ok(client_order_ids.len())
        } else {
            Err(RithmicError::Order(format!(
                "Batch cancellation failed for {} of {} orders: {}",
                failures.len(),
                client_order_ids.len(),
                failures.join("; ")
            )))
        }
    }

    pub(crate) async fn batch_cancel_orders_classified(
        &self,
        client_order_ids: &[&str],
    ) -> Vec<(String, std::result::Result<(), OrderCommandError>)> {
        let mut outcomes = Vec::with_capacity(client_order_ids.len());

        for client_order_id in client_order_ids {
            outcomes.push((
                (*client_order_id).to_string(),
                self.cancel_order_classified(client_order_id).await,
            ));
        }
        outcomes
    }

    /// Queries all open orders from Rithmic.
    ///
    /// This triggers an order reconciliation - the response will come
    /// through the gateway's execution event channel.
    pub async fn query_orders(&self) -> Result<()> {
        let handle = self.order_handle().await?;
        tracing::debug!("Querying open orders");

        let response = handle
            .show_orders()
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Order(e.to_string()));
        }

        Ok(())
    }

    /// Ensures order updates are subscribed for this account.
    pub async fn subscribe_order_updates(&self) -> Result<()> {
        let mut gateway = self.gateway.write().await;
        gateway
            .ensure_order_processor(&self.account)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;
        Ok(())
    }

    /// Ensures PnL updates are subscribed for this account.
    pub async fn subscribe_pnl_updates(&self) -> Result<()> {
        let mut gateway = self.gateway.write().await;
        gateway
            .ensure_pnl_processor(&self.account)
            .await
            .map_err(|e| RithmicError::Connection(e.to_string()))?;
        Ok(())
    }

    /// Requests a PnL snapshot for this account.
    pub async fn request_pnl_snapshot(&self) -> Result<()> {
        let handle = self.pnl_handle().await?;
        let response = handle
            .pnl_position_snapshots()
            .await
            .map_err(|e| RithmicError::Connection(e.to_string()))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Connection(e.to_string()));
        }

        Ok(())
    }

    /// Replays execution history from Rithmic for the requested time window.
    ///
    /// The replayed venue events are emitted through the gateway execution
    /// channel and must be applied in timestamp order by the consumer.
    pub async fn replay_executions(
        &self,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> Result<()> {
        let handle = self.order_handle().await?;
        tracing::debug!(
            start_index_sec,
            finish_index_sec,
            "Replaying execution history"
        );

        let responses = handle
            .replay_executions(start_index_sec, finish_index_sec)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;

        if let Some(e) = first_response_error(&responses) {
            return Err(RithmicError::Order(e));
        }

        Ok(())
    }

    /// Returns locally tracked order state.
    pub fn get_order(&self, client_order_id: &str) -> Option<OrderState> {
        self.orders.get(client_order_id).map(|r| r.clone())
    }

    /// Returns all locally tracked orders.
    pub fn orders(&self) -> Vec<OrderState> {
        self.orders.iter().map(|r| r.clone()).collect()
    }

    /// Returns open orders count.
    pub fn open_orders_count(&self) -> usize {
        self.orders
            .iter()
            .filter(|r| !r.status.is_terminal())
            .count()
    }

    /// Returns a receiver for execution events.
    ///
    /// Note: In the typical architecture, execution events come from
    /// the gateway's execution event channel (driven by ExecutionHandler).
    /// This method is provided for direct low-level client usage.
    pub fn event_receiver(&mut self) -> mpsc::UnboundedReceiver<ExecutionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_tx = Some(tx);
        rx
    }

    /// Sends an event to the event channel.
    #[allow(dead_code)] // Used when execution events are consumed directly from the client
    pub(crate) fn emit_event(&self, event: ExecutionEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    /// Updates order state from venue message.
    ///
    /// Called by the execution handler when processing order notifications.
    pub fn update_order_state(
        &self,
        client_order_id: &str,
        venue_order_id: Option<&str>,
        status: OrderStatus,
        filled_qty: Option<f64>,
        leaves_qty: Option<f64>,
        avg_price: Option<f64>,
    ) {
        if let Some(mut order) = self.orders.get_mut(client_order_id) {
            if let Some(vid) = venue_order_id
                && order.venue_order_id.is_none()
            {
                order.venue_order_id = Some(vid.to_string());
                self.venue_to_client
                    .insert(vid.to_string(), client_order_id.to_string());
            }

            if status_rank(status) >= status_rank(order.status) {
                order.status = status;
            }

            if let Some(fq) = filled_qty {
                order.filled_qty = order.filled_qty.max(fq);
            }

            if let Some(lq) = leaves_qty {
                order.leaves_qty = lq;
            }

            if let Some(ap) = avg_price {
                order.avg_price = ap;
            }
        } else {
            tracing::warn!(
                "Received update for unknown order: client_id={}",
                client_order_id
            );
        }
    }

    /// Looks up client order ID from venue order ID.
    pub fn get_client_order_id(&self, venue_order_id: &str) -> Option<String> {
        self.venue_to_client.get(venue_order_id).map(|r| r.clone())
    }

    /// Applies an execution event to local order state and re-emits it to any
    /// attached event receiver.
    pub fn apply_event(&self, event: &ExecutionEvent) -> bool {
        if let Err(e) = validate_execution_event(event) {
            tracing::warn!("Dropping invalid Rithmic execution event: {e}");
            return false;
        }

        let mut emit_downstream = true;

        match event {
            ExecutionEvent::Submitted(e) => {
                if !self.orders.contains_key(&e.client_order_id)
                    && let Some(order_state) = order_state_from_context(
                        &e.client_order_id,
                        e.venue_order_id.as_deref(),
                        OrderStatus::Pending,
                        &e.context,
                        None,
                        None,
                        e.context.avg_price,
                    )
                {
                    if let Some(venue_order_id) = &order_state.venue_order_id {
                        self.venue_to_client
                            .insert(venue_order_id.clone(), e.client_order_id.clone());
                    }
                    self.orders.insert(e.client_order_id.clone(), order_state);
                }

                if let Some(order) = self.orders.get(&e.client_order_id)
                    && (status_rank(order.status) > status_rank(OrderStatus::Pending)
                        || (order.status == OrderStatus::Pending
                            && order.venue_order_id == e.venue_order_id))
                {
                    emit_downstream = false;
                }
                self.update_order_state(
                    &e.client_order_id,
                    e.venue_order_id.as_deref(),
                    OrderStatus::Pending,
                    e.context.filled_qty,
                    e.context.leaves_qty,
                    e.context.avg_price,
                );
            }
            ExecutionEvent::Accepted(e) => {
                if !self.orders.contains_key(&e.client_order_id)
                    && let Some(order_state) = order_state_from_context(
                        &e.client_order_id,
                        Some(&e.venue_order_id),
                        OrderStatus::Open,
                        &e.context,
                        None,
                        None,
                        e.context.avg_price,
                    )
                {
                    self.venue_to_client
                        .insert(e.venue_order_id.clone(), e.client_order_id.clone());
                    self.orders.insert(e.client_order_id.clone(), order_state);
                }

                if let Some(order) = self.orders.get(&e.client_order_id)
                    && status_rank(order.status) > status_rank(OrderStatus::Open)
                {
                    emit_downstream = false;
                }
                self.update_order_state(
                    &e.client_order_id,
                    Some(&e.venue_order_id),
                    OrderStatus::Open,
                    e.context.filled_qty,
                    e.context.leaves_qty,
                    e.context.avg_price,
                );
            }
            ExecutionEvent::Rejected(e) => {
                if let Some(order) = self.orders.get(&e.client_order_id)
                    && status_rank(order.status) >= status_rank(OrderStatus::Rejected)
                {
                    emit_downstream = false;
                }
                self.update_order_state(
                    &e.client_order_id,
                    None,
                    OrderStatus::Rejected,
                    None,
                    Some(0.0),
                    None,
                );
            }
            ExecutionEvent::Filled(e) => {
                if !self.orders.contains_key(&e.client_order_id)
                    && let Some(mut order_state) = order_state_from_context(
                        &e.client_order_id,
                        Some(&e.venue_order_id),
                        OrderStatus::Partial,
                        &e.context,
                        Some(e.fill_qty),
                        e.leaves_qty,
                        e.context.avg_price,
                    )
                {
                    order_state.status = if order_state.leaves_qty > 0.0 {
                        OrderStatus::Partial
                    } else {
                        OrderStatus::Complete
                    };
                    self.venue_to_client
                        .insert(e.venue_order_id.clone(), e.client_order_id.clone());
                    self.orders.insert(e.client_order_id.clone(), order_state);
                }

                if let Some(mut order) = self.orders.get_mut(&e.client_order_id) {
                    let prev_filled = order.filled_qty;
                    let base_quantity = order.quantity.max(prev_filled + e.fill_qty);
                    let new_filled = e
                        .context
                        .filled_qty
                        .unwrap_or(prev_filled + e.fill_qty)
                        .max(prev_filled)
                        .min(base_quantity);
                    let incremental_fill = (new_filled - prev_filled).max(0.0);
                    let leaves_qty = e
                        .context
                        .leaves_qty
                        .or(e.leaves_qty)
                        .unwrap_or_else(|| (order.quantity - new_filled).max(0.0));
                    let event_status = if leaves_qty > 0.0 {
                        OrderStatus::Partial
                    } else {
                        OrderStatus::Complete
                    };

                    if incremental_fill == 0.0
                        && status_rank(order.status) >= status_rank(event_status)
                    {
                        emit_downstream = false;
                    }

                    order.filled_qty = new_filled;
                    order.leaves_qty = leaves_qty;

                    if let Some(avg_price) = e.context.avg_price.filter(|value| *value > 0.0) {
                        order.avg_price = avg_price;
                    } else if incremental_fill > 0.0 && new_filled > 0.0 {
                        let prev_notional = order.avg_price * prev_filled;
                        let new_notional = prev_notional + e.fill_price * incremental_fill;
                        order.avg_price = new_notional / new_filled;
                    }

                    if status_rank(event_status) >= status_rank(order.status) {
                        order.status = event_status;
                    }

                    if order.venue_order_id.is_none() {
                        order.venue_order_id = Some(e.venue_order_id.clone());
                        self.venue_to_client
                            .insert(e.venue_order_id.clone(), e.client_order_id.clone());
                    }
                } else {
                    tracing::warn!(
                        "Filled event for unknown order: client_id={} venue_id={}",
                        e.client_order_id,
                        e.venue_order_id
                    );
                }
            }
            ExecutionEvent::Cancelled(e) => {
                if let Some(order) = self.orders.get(&e.client_order_id)
                    && status_rank(order.status) > status_rank(OrderStatus::Cancelled)
                {
                    emit_downstream = false;
                }
                self.update_order_state(
                    &e.client_order_id,
                    Some(&e.venue_order_id),
                    OrderStatus::Cancelled,
                    None,
                    Some(0.0),
                    None,
                );
            }
            ExecutionEvent::Modified(e) => {
                if let Some(mut order) = self.orders.get_mut(&e.client_order_id) {
                    if order.status.is_terminal() {
                        emit_downstream = false;
                    }

                    if let Some(qty) = e.new_qty {
                        order.quantity = qty;
                        // Keep filled_qty unchanged; recompute leaves based on new quantity
                        order.leaves_qty = (order.quantity - order.filled_qty).max(0.0);
                    }

                    if let Some(price) = e.new_price {
                        order.price = Some(price);
                    }

                    if order.venue_order_id.is_none() {
                        order.venue_order_id = Some(e.venue_order_id.clone());
                        self.venue_to_client
                            .insert(e.venue_order_id.clone(), e.client_order_id.clone());
                    }
                } else if let Some(order_state) = order_state_from_context(
                    &e.client_order_id,
                    Some(&e.venue_order_id),
                    OrderStatus::Open,
                    &e.context,
                    None,
                    None,
                    e.context.avg_price,
                ) {
                    self.venue_to_client
                        .insert(e.venue_order_id.clone(), e.client_order_id.clone());
                    self.orders.insert(e.client_order_id.clone(), order_state);
                } else {
                    tracing::warn!(
                        "Modify event for unknown order: client_id={} venue_id={}",
                        e.client_order_id,
                        e.venue_order_id
                    );
                }
            }
            ExecutionEvent::ConnectionState(_) => {}
            ExecutionEvent::Reconnected => {}
            ExecutionEvent::Authenticated => {}
            ExecutionEvent::Error(_) => {}
        }

        // Emit downstream if a receiver has been registered.

        if emit_downstream {
            self.emit_event(event.clone());
        }
        emit_downstream
    }

    /// Consumes execution events from a channel, applying them to local state
    /// and re-emitting to any downstream listener registered via `event_receiver`.
    ///
    /// Caller is responsible for obtaining the receiver, typically from
    /// `RithmicGateway::subscribe_execution_events()`.
    pub async fn pump_events(
        self: Arc<Self>,
        mut rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
    ) {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    self.apply_event(&event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    log::warn!("Rithmic execution client lagged by {skipped} events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Spawns an async task to process execution events from a receiver.
    ///
    /// Use this as a convenience when wiring the gateway execution channel to
    /// the client state machine.
    pub fn spawn_event_pump(
        self: Arc<Self>,
        rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
    ) -> JoinHandle<()> {
        get_runtime().spawn(self.pump_events(rx))
    }
}

impl Debug for RithmicExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicExecutionClient))
            .field("account_id", &self.account_id)
            .field("open_orders", &self.open_orders_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use rstest::rstest;

    use super::*;
    use crate::{config::RithmicEnv, gateway::GatewayConfig};

    fn sample_order_request() -> OrderRequest {
        OrderRequest {
            client_order_id: "O-1".to_string(),
            symbol: "ESM6".to_string(),
            exchange: "CME".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            quantity: 1.0,
            price: Some(5000.0),
            stop_price: None,
            trailing_stop: None,
        }
    }

    #[rstest]
    #[case(
        RithmicApiError::InvalidArgument("bad quantity".to_string()),
        CommandFailureKind::Definitive
    )]
    #[case(RithmicApiError::SendFailed, CommandFailureKind::Unknown)]
    #[case(RithmicApiError::ConnectionClosed, CommandFailureKind::Unknown)]
    #[case(RithmicApiError::EmptyResponse, CommandFailureKind::Unknown)]
    #[case(
        RithmicApiError::ProtocolError("malformed acknowledgement".to_string()),
        CommandFailureKind::Unknown
    )]
    fn classifies_order_command_outcome(
        #[case] e: RithmicApiError,
        #[case] expected: CommandFailureKind,
    ) {
        assert_eq!(classify_api_error(&e), expected);
    }

    #[rstest]
    #[case(1.0, Ok(1))]
    #[case(f64::from(i32::MAX), Ok(i32::MAX))]
    #[case(0.0, Err("positive"))]
    #[case(-1.0, Err("positive"))]
    #[case(1.5, Err("whole number"))]
    #[case(f64::INFINITY, Err("finite"))]
    #[case(f64::NAN, Err("finite"))]
    #[case(f64::from(i32::MAX) + 1.0, Err("maximum"))]
    fn converts_contract_quantity_with_checked_bounds(
        #[case] quantity: f64,
        #[case] expected: std::result::Result<i32, &str>,
    ) {
        match expected {
            Ok(expected) => assert_eq!(whole_contracts(quantity).unwrap(), expected),
            Err(expected_message) => {
                assert!(
                    whole_contracts(quantity)
                        .unwrap_err()
                        .to_string()
                        .contains(expected_message)
                );
            }
        }
    }

    #[rstest]
    #[case(Some(1), Some(500_000), Some(1_500_000_000))]
    #[case(None, Some(0), None)]
    #[case(Some(1), None, None)]
    #[case(Some(-1), Some(0), None)]
    #[case(Some(1), Some(-1), None)]
    #[case(Some(1), Some(1_000_000), None)]
    fn validates_rithmic_ack_timestamp(
        #[case] ssboe: Option<i32>,
        #[case] usecs: Option<i32>,
        #[case] expected: Option<u64>,
    ) {
        assert_eq!(checked_rithmic_timestamp(ssboe, usecs), expected);
    }

    #[rstest]
    fn classified_error_preserves_typed_source() {
        let e = OrderCommandError::from_api(RithmicApiError::InvalidArgument(
            "bad quantity".to_string(),
        ));

        assert!(e.is_definitive());
        assert!(e.source().is_some());
    }

    #[rstest]
    fn validates_low_level_order_request_before_transport() {
        assert!(validate_order_request(&sample_order_request()).is_ok());

        let mut request = sample_order_request();
        request.price = Some(f64::NAN);
        assert!(validate_order_request(&request).is_err());

        let mut request = sample_order_request();
        request.price = Some(f64::MAX);
        assert!(validate_order_request(&request).is_err());

        let mut request = sample_order_request();
        request.price = Some(f64::MIN_POSITIVE);
        assert!(validate_order_request(&request).is_err());

        let mut request = sample_order_request();
        request.quantity = 1.5;
        assert!(validate_order_request(&request).is_err());

        let mut request = sample_order_request();
        request.order_type = OrderType::StopMarket;
        request.price = None;
        request.trailing_stop = Some(TrailingStopConfig { trail_by_ticks: 0 });
        assert!(validate_order_request(&request).is_err());

        let mut request = sample_order_request();
        request.symbol.clear();
        assert!(validate_order_request(&request).is_err());
    }

    #[rstest]
    fn command_response_rejection_updates_state_without_emitting_duplicate_event() {
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
        .expect("valid gateway config");
        let gateway = Arc::new(RwLock::new(RithmicGateway::new(config)));
        let mut client =
            RithmicExecutionClient::new(gateway, RithmicAccount::new("fcm", "ib", "account"));
        let mut receiver = client.event_receiver();
        client.orders.insert(
            "O-1".to_string(),
            OrderState {
                client_order_id: "O-1".to_string(),
                venue_order_id: None,
                symbol: "ESM6".to_string(),
                exchange: "CME".to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Day,
                price: Some(5000.0),
                trigger_price: None,
                status: OrderStatus::Pending,
                quantity: 1.0,
                filled_qty: 0.0,
                leaves_qty: 1.0,
                avg_price: 0.0,
            },
        );

        client.mark_submit_rejected("O-1");

        assert_eq!(
            client.get_order("O-1").unwrap().status,
            OrderStatus::Rejected
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn duplicate_submit_and_invalid_modify_preserve_existing_order() {
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
        .expect("valid gateway config");
        let gateway = Arc::new(RwLock::new(RithmicGateway::new(config)));
        let client =
            RithmicExecutionClient::new(gateway, RithmicAccount::new("fcm", "ib", "account"));
        client.orders.insert(
            "O-1".to_string(),
            OrderState {
                client_order_id: "O-1".to_string(),
                venue_order_id: Some("V-1".to_string()),
                symbol: "ESM6".to_string(),
                exchange: "CME".to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Day,
                price: Some(5000.0),
                trigger_price: None,
                status: OrderStatus::Open,
                quantity: 1.0,
                filled_qty: 0.0,
                leaves_qty: 1.0,
                avg_price: 0.0,
            },
        );

        let duplicate = client
            .submit_order_classified(sample_order_request())
            .await
            .expect_err("duplicate ID should fail before transport");
        assert!(duplicate.is_definitive());
        assert_eq!(
            client
                .get_order("O-1")
                .expect("order should remain")
                .quantity,
            1.0
        );

        let invalid_price = client
            .modify_order_classified("O-1", None, Some(f64::INFINITY))
            .await
            .expect_err("invalid price should fail before transport");
        assert!(invalid_price.is_definitive());

        client
            .orders
            .get_mut("O-1")
            .expect("order should remain")
            .price = None;
        let missing_required_price = client
            .modify_order_classified("O-1", Some(2.0), None)
            .await
            .expect_err("limit modify cannot default a missing price to zero");
        assert!(missing_required_price.is_definitive());
    }

    #[rstest]
    fn missing_fill_leaves_is_derived_only_from_known_quantity() {
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
        .expect("valid gateway config");
        let gateway = Arc::new(RwLock::new(RithmicGateway::new(config)));
        let client =
            RithmicExecutionClient::new(gateway, RithmicAccount::new("fcm", "ib", "account"));
        client.orders.insert(
            "KNOWN".to_string(),
            OrderState {
                client_order_id: "KNOWN".to_string(),
                venue_order_id: Some("VENUE-KNOWN".to_string()),
                symbol: "ESM6".to_string(),
                exchange: "CME".to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Day,
                price: Some(5000.0),
                trigger_price: None,
                status: OrderStatus::Open,
                quantity: 5.0,
                filled_qty: 0.0,
                leaves_qty: 5.0,
                avg_price: 0.0,
            },
        );

        client.apply_event(&ExecutionEvent::Filled(OrderFilled {
            client_order_id: "KNOWN".to_string(),
            account_id: "account".to_string(),
            venue_order_id: "VENUE-KNOWN".to_string(),
            fill_price: 5000.0,
            fill_qty: 2.0,
            leaves_qty: None,
            commission: 0.0,
            ts_event: 1,
            trade_id: Some("TRADE-KNOWN".to_string()),
            currency: Some("USD".to_string()),
            context: OrderContext::default(),
        }));

        let known = client
            .get_order("KNOWN")
            .expect("known order should remain");
        assert_eq!(known.leaves_qty, 3.0);
        assert_eq!(known.status, OrderStatus::Partial);
        drop(known);

        assert!(!client.apply_event(&ExecutionEvent::Filled(OrderFilled {
            client_order_id: "KNOWN".to_string(),
            account_id: "account".to_string(),
            venue_order_id: "VENUE-KNOWN".to_string(),
            fill_price: f64::NAN,
            fill_qty: -1.0,
            leaves_qty: Some(-1.0),
            commission: 0.0,
            ts_event: 2,
            trade_id: Some("TRADE-INVALID".to_string()),
            currency: Some("USD".to_string()),
            context: OrderContext::default(),
        })));
        assert!(!client.apply_event(&ExecutionEvent::Filled(OrderFilled {
            client_order_id: "KNOWN".to_string(),
            account_id: "account".to_string(),
            venue_order_id: "VENUE-KNOWN".to_string(),
            fill_price: f64::MAX,
            fill_qty: 1.0,
            leaves_qty: Some(2.0),
            commission: 0.0,
            ts_event: 3,
            trade_id: Some("TRADE-UNREPRESENTABLE".to_string()),
            currency: Some("USD".to_string()),
            context: OrderContext::default(),
        })));
        let preserved = client
            .get_order("KNOWN")
            .expect("invalid fill must not remove known order");
        assert_eq!(preserved.filled_qty, 2.0);
        assert_eq!(preserved.leaves_qty, 3.0);
        assert_eq!(preserved.status, OrderStatus::Partial);
        drop(preserved);

        assert!(
            !client.apply_event(&ExecutionEvent::Modified(OrderModified {
                client_order_id: "KNOWN".to_string(),
                account_id: "account".to_string(),
                venue_order_id: "VENUE-KNOWN".to_string(),
                new_price: Some(f64::INFINITY),
                new_qty: Some(0.0),
                ts_event: 4,
                context: OrderContext::default(),
            }))
        );
        let preserved = client
            .get_order("KNOWN")
            .expect("invalid modify must not remove known order");
        assert_eq!(preserved.quantity, 5.0);
        assert_eq!(preserved.price, Some(5000.0));
        drop(preserved);

        client.apply_event(&ExecutionEvent::Filled(OrderFilled {
            client_order_id: "UNKNOWN".to_string(),
            account_id: "account".to_string(),
            venue_order_id: "VENUE-UNKNOWN".to_string(),
            fill_price: 5000.0,
            fill_qty: 2.0,
            leaves_qty: None,
            commission: 0.0,
            ts_event: 4,
            trade_id: Some("TRADE-UNKNOWN".to_string()),
            currency: Some("USD".to_string()),
            context: OrderContext {
                symbol: Some("ESM6".to_string()),
                exchange: Some("CME".to_string()),
                side: Some(OrderSide::Buy),
                order_type: Some(OrderType::Limit),
                time_in_force: Some(TimeInForce::Day),
                filled_qty: Some(2.0),
                ..OrderContext::default()
            },
        }));
        assert!(client.get_order("UNKNOWN").is_none());
    }
}
