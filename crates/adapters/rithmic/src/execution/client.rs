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
use rithmic_rs::{
    OrderSide, OrderStatus, OrderType, RithmicAccount, RithmicCancelOrder, RithmicModifyOrder,
    RithmicOrder, TimeInForce, TrailingStop, api::RithmicResponse,
    plants::order_plant::RithmicOrderPlantHandle, rithmic_to_unix_nanos,
    rti::messages::RithmicMessage,
};
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
    /// Remaining quantity.
    pub leaves_qty: f64,
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
    let order_type = context.order_type?;
    let quantity = context
        .quantity
        .or_else(|| {
            fill_qty
                .zip(leaves_qty)
                .map(|(filled, leaves)| filled + leaves)
        })
        .or(fill_qty)
        .unwrap_or_default();
    let filled_qty = context.filled_qty.or(fill_qty).unwrap_or_default();
    let leaves_qty = context
        .leaves_qty
        .or(leaves_qty)
        .unwrap_or_else(|| (quantity - filled_qty).max(0.0));

    Some(OrderState {
        client_order_id: client_order_id.to_string(),
        venue_order_id: venue_order_id.map(ToOwned::to_owned),
        symbol,
        exchange,
        side: context.side.unwrap_or(OrderSide::Buy),
        order_type,
        time_in_force: context.time_in_force.unwrap_or(TimeInForce::Day),
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
        let handle = self.order_handle().await?;

        // Validate quantity is a positive whole number (Rithmic uses i32 for contracts)

        if request.quantity <= 0.0 {
            return Err(RithmicError::Order(format!(
                "Quantity must be positive, received: {}",
                request.quantity
            )));
        }

        if request.quantity.fract() != 0.0 {
            return Err(RithmicError::Order(format!(
                "Quantity must be a whole number (contracts), received: {}",
                request.quantity
            )));
        }

        if request.quantity > i32::MAX as f64 {
            return Err(RithmicError::Order(format!(
                "Quantity exceeds maximum: {}",
                request.quantity
            )));
        }

        // Validate limit orders have a price

        if (request.order_type == OrderType::Limit || request.order_type == OrderType::StopLimit)
            && request.price.is_none()
        {
            return Err(RithmicError::Order(
                "Limit/StopLimit order requires price".to_string(),
            ));
        }

        // Validate stop orders have a stop price

        if (request.order_type == OrderType::StopMarket
            || request.order_type == OrderType::StopLimit)
            && request.stop_price.is_none()
        {
            return Err(RithmicError::Order(
                "Stop order requires stop_price".to_string(),
            ));
        }

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
                RithmicError::Order("Limit order price is required for submission".to_string())
            })?,
            _ => request.price.unwrap_or(0.0),
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
            quantity: request.quantity as i32,
            price,
            transaction_type: request.side.into(),
            price_type: request.order_type.into(),
            user_tag: request.client_order_id.clone(),
            duration: Some(request.time_in_force.into()),
            trigger_price,
            trailing_stop,
        };

        // Submit to Rithmic using the new place_order API
        let responses = handle
            .place_order(order)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;

        let mut submitted_event: Option<ExecutionEvent> = None;

        for response in &responses {
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

                    if let Some(e) = &response.error {
                        let event = ExecutionEvent::Rejected(OrderRejected {
                            client_order_id: request.client_order_id.clone(),
                            account_id: self.account_id.clone(),
                            reason: e.to_string(),
                            ts_event: rithmic_to_unix_nanos(
                                resp.ssboe.unwrap_or(0),
                                resp.usecs.unwrap_or(0),
                            ),
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
                        });
                        self.apply_event(&event);
                        return Err(RithmicError::Order(e.to_string()));
                    }

                    let matches_request =
                        resp.user_tag.as_deref() == Some(request.client_order_id.as_str());
                    let has_venue_identity = resp.basket_id.is_some();

                    if matches_request || has_venue_identity {
                        submitted_event = Some(ExecutionEvent::Submitted(OrderSubmitted {
                            client_order_id: request.client_order_id.clone(),
                            venue_order_id: resp.basket_id.clone(),
                            account_id: self.account_id.clone(),
                            ts_event: rithmic_to_unix_nanos(
                                resp.ssboe.unwrap_or(0),
                                resp.usecs.unwrap_or(0),
                            ),
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

        if let Some(event) = submitted_event {
            self.apply_event(&event);
        }

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
        let handle = self.order_handle().await?;

        // Validate new_qty if provided

        if let Some(qty) = new_qty {
            if qty <= 0.0 {
                return Err(RithmicError::Order(format!(
                    "Quantity must be positive, received: {qty}"
                )));
            }

            if qty.fract() != 0.0 {
                return Err(RithmicError::Order(format!(
                    "Quantity must be a whole number (contracts), received: {qty}"
                )));
            }
        }

        let order = self
            .orders
            .get(client_order_id)
            .ok_or_else(|| RithmicError::Order(format!("Order not found: {client_order_id}")))?;

        let venue_order_id = order
            .venue_order_id
            .clone()
            .ok_or_else(|| RithmicError::Order("Order not yet accepted by venue".to_string()))?;

        let modify_request = RithmicModifyOrder {
            id: venue_order_id.clone(),
            exchange: order.exchange.clone(),
            symbol: order.symbol.clone(),
            qty: new_qty.map_or(order.leaves_qty as i32, |q| q as i32),
            price: new_price.unwrap_or(0.0),
            price_type: order.order_type.into(),
        };

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
            .map_err(|e| RithmicError::Order(e.to_string()))?;

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

        if let Some(e) = first_response_error(&responses) {
            return Err(RithmicError::Order(e));
        }

        Ok(())
    }

    /// Cancels an order.
    ///
    /// The order must exist locally and have a venue_order_id (must be accepted).
    pub async fn cancel_order(&self, client_order_id: &str) -> Result<()> {
        let handle = self.order_handle().await?;
        let order = self
            .orders
            .get(client_order_id)
            .ok_or_else(|| RithmicError::Order(format!("Order not found: {client_order_id}")))?;

        let venue_order_id = order
            .venue_order_id
            .clone()
            .ok_or_else(|| RithmicError::Order("Order not yet accepted by venue".to_string()))?;

        tracing::debug!(
            "Cancelling order: client_id={}, venue_id={}",
            client_order_id,
            venue_order_id
        );

        let cancel_request = RithmicCancelOrder { id: venue_order_id };

        let responses = handle
            .cancel_order(cancel_request)
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;

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

        if let Some(e) = first_response_error(&responses) {
            return Err(RithmicError::Order(e));
        }

        Ok(())
    }

    /// Cancels all open orders.
    pub async fn cancel_all_orders(&self) -> Result<()> {
        let handle = self.order_handle().await?;
        tracing::debug!("Cancelling all orders");

        let response = handle
            .cancel_all_orders()
            .await
            .map_err(|e| RithmicError::Order(e.to_string()))?;

        if let Some(e) = response.error {
            return Err(RithmicError::Order(e.to_string()));
        }

        Ok(())
    }

    /// Cancels a batch of orders by client order ID.
    ///
    /// This iterates through the provided order IDs and cancels each one.
    /// Orders that don't exist or haven't been accepted are skipped.
    ///
    /// # Returns
    ///
    /// Returns the number of orders successfully submitted for cancellation.
    /// Note: This doesn't guarantee the cancels were accepted by the venue.
    pub async fn batch_cancel_orders(&self, client_order_ids: &[&str]) -> Result<usize> {
        tracing::debug!("Batch cancelling {} orders", client_order_ids.len());

        let mut cancelled = 0;

        for client_order_id in client_order_ids {
            match self.cancel_order(client_order_id).await {
                Ok(()) => cancelled += 1,
                Err(e) => {
                    tracing::warn!(
                        "Failed to cancel order {}: {} (continuing with batch)",
                        client_order_id,
                        e
                    );
                }
            }
        }

        tracing::debug!(
            "Batch cancel submitted {} of {} orders",
            cancelled,
            client_order_ids.len()
        );
        Ok(cancelled)
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
    pub fn apply_event(&self, event: &ExecutionEvent) {
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
                    && let Some(order_state) = order_state_from_context(
                        &e.client_order_id,
                        Some(&e.venue_order_id),
                        if e.leaves_qty > 0.0 {
                            OrderStatus::Partial
                        } else {
                            OrderStatus::Complete
                        },
                        &e.context,
                        Some(e.fill_qty),
                        Some(e.leaves_qty),
                        e.context.avg_price,
                    )
                {
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

                    if incremental_fill == 0.0
                        && status_rank(order.status)
                            >= status_rank(if e.leaves_qty > 0.0 {
                                OrderStatus::Partial
                            } else {
                                OrderStatus::Complete
                            })
                    {
                        emit_downstream = false;
                    }

                    order.filled_qty = new_filled;
                    order.leaves_qty = e.context.leaves_qty.unwrap_or(e.leaves_qty);

                    if let Some(avg_price) = e.context.avg_price.filter(|value| *value > 0.0) {
                        order.avg_price = avg_price;
                    } else if incremental_fill > 0.0 && new_filled > 0.0 {
                        let prev_notional = order.avg_price * prev_filled;
                        let new_notional = prev_notional + e.fill_price * incremental_fill;
                        order.avg_price = new_notional / new_filled;
                    }

                    let new_status = if order.leaves_qty > 0.0 {
                        OrderStatus::Partial
                    } else {
                        OrderStatus::Complete
                    };

                    if status_rank(new_status) >= status_rank(order.status) {
                        order.status = new_status;
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
                Ok(event) => self.apply_event(&event),
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
