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

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use nautilus_common::live::get_runtime;
use nautilus_core::time::get_atomic_clock_realtime;
use projectx_client::{
    Account, Hub, MarketDepth, MarketQuote, MarketTrade, Order, Position, RealtimeClient,
    RealtimeEvent, Trade,
};
use serde_json::Value;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::{
    common::{enums::ProjectXHub, urls::ProjectXUrls},
    websocket::error::ProjectXWsError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXSubscription {
    pub target: String,
    pub arguments: Vec<Value>,
}

impl ProjectXSubscription {
    #[must_use]
    pub fn new(target: impl Into<String>, arguments: Vec<Value>) -> Self {
        Self {
            target: target.into(),
            arguments,
        }
    }
}

/// Typed events delivered by the ProjectX websocket wrapper.
///
/// The provider's SignalR invocations are decoded into the `projectx-client`
/// entity types by this wrapper, so consumers never touch raw frames.
#[derive(Clone, Debug)]
pub enum ProjectXWsEvent {
    Connected,
    Disconnected,
    Reconnected,
    ReconciliationRequired,
    MarketQuote(MarketQuote),
    MarketTrade(MarketTrade),
    MarketDepth(MarketDepth),
    UserAccount(Account),
    UserOrder(Order),
    UserPosition(Position),
    UserTrade(Trade),
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXWsClient {
    hub: ProjectXHub,
    realtime: RealtimeClient,
    pump_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    subscriptions: Arc<tokio::sync::RwLock<Vec<ProjectXSubscription>>>,
    connected: Arc<AtomicBool>,
    last_message_at_ms: Arc<AtomicU64>,
    requires_reconciliation: Arc<AtomicBool>,
    event_tx: mpsc::UnboundedSender<ProjectXWsEvent>,
    event_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<ProjectXWsEvent>>>>,
}

impl Debug for ProjectXWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ProjectXWsClient))
            .field("hub", &self.hub)
            .field("is_connected", &self.connected.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl Clone for ProjectXWsClient {
    fn clone(&self) -> Self {
        Self {
            hub: self.hub,
            realtime: self.realtime.clone(),
            pump_task: Arc::clone(&self.pump_task),
            subscriptions: Arc::clone(&self.subscriptions),
            connected: Arc::clone(&self.connected),
            last_message_at_ms: Arc::clone(&self.last_message_at_ms),
            requires_reconciliation: Arc::clone(&self.requires_reconciliation),
            event_tx: self.event_tx.clone(),
            event_rx: Arc::clone(&self.event_rx),
        }
    }
}

fn to_hub(hub: ProjectXHub) -> Hub {
    match hub {
        ProjectXHub::Market => Hub::Market,
        ProjectXHub::User => Hub::User,
    }
}

fn to_ws_error(error: projectx_client::RealtimeError) -> ProjectXWsError {
    ProjectXWsError::Realtime(error)
}

impl ProjectXWsClient {
    /// Creates a new ProjectX websocket client.
    ///
    /// The wrapper owns the `projectx-client` real-time hub: connection,
    /// handshake, keep-alive, token rotation, and reconnect are handled by the
    /// published client while this wrapper decodes provider invocations into
    /// typed [`ProjectXWsEvent`] values.
    #[must_use]
    pub fn new(client: &projectx_client::Client, hub: ProjectXHub, _urls: ProjectXUrls) -> Self {
        let now_ms = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000;
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            hub,
            realtime: client.realtime(to_hub(hub)),
            pump_task: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            connected: Arc::new(AtomicBool::new(false)),
            last_message_at_ms: Arc::new(AtomicU64::new(now_ms)),
            requires_reconciliation: Arc::new(AtomicBool::new(false)),
            event_tx,
            event_rx: Arc::new(Mutex::new(Some(event_rx))),
        }
    }

    pub async fn take_event_receiver(&self) -> Option<mpsc::UnboundedReceiver<ProjectXWsEvent>> {
        self.event_rx.lock().await.take()
    }

    pub async fn connect(&self) -> Result<(), ProjectXWsError> {
        self.realtime.connect().await.map_err(to_ws_error)?;
        self.spawn_event_pump().await;
        self.connected.store(true, Ordering::SeqCst);
        let _ = self.event_tx.send(ProjectXWsEvent::Connected);
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), ProjectXWsError> {
        if let Some(task) = self.pump_task.lock().await.take() {
            task.abort();
        }

        self.realtime.disconnect().await.map_err(to_ws_error)?;

        self.connected.store(false, Ordering::SeqCst);
        let _ = self.event_tx.send(ProjectXWsEvent::Disconnected);
        Ok(())
    }

    pub async fn invoke(
        &self,
        target: impl Into<String>,
        arguments: Vec<Value>,
        track: bool,
    ) -> Result<(), ProjectXWsError> {
        let target = target.into();

        if track {
            self.track_subscription(ProjectXSubscription::new(target.clone(), arguments.clone()))
                .await;
        }
        self.realtime
            .invoke(target, arguments)
            .await
            .map_err(to_ws_error)
    }

    pub async fn unsubscribe(
        &self,
        unsubscribe_target: impl Into<String>,
        tracked_target: &str,
        tracked_arguments: &[Value],
        arguments: Vec<Value>,
    ) -> Result<(), ProjectXWsError> {
        self.remove_subscription(tracked_target, tracked_arguments)
            .await;
        self.realtime
            .invoke(unsubscribe_target.into(), arguments)
            .await
            .map_err(to_ws_error)
    }

    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn last_message_at_ms(&self) -> u64 {
        self.last_message_at_ms.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn take_reconciliation_flag(&self) -> bool {
        self.requires_reconciliation.swap(false, Ordering::SeqCst)
    }

    async fn spawn_event_pump(&self) {
        if self.pump_task.lock().await.is_some() {
            return;
        }

        let this = self.clone();

        let handle = get_runtime().spawn(async move {
            let Some(mut rx) = this.realtime.take_event_receiver() else {
                log::warn!("ProjectX {:?} event receiver already claimed", this.hub);
                return;
            };

            while let Some(event) = rx.recv().await {
                this.record_message_activity();

                match event {
                    RealtimeEvent::Connected => {}
                    RealtimeEvent::Disconnected => {
                        this.connected.store(false, Ordering::SeqCst);
                        let _ = this.event_tx.send(ProjectXWsEvent::Disconnected);
                    }
                    RealtimeEvent::Reconnected => {
                        if let Err(e) = this.resubscribe_all().await {
                            log::warn!("ProjectX {:?} resubscribe failed: {e}", this.hub);
                        }
                        this.requires_reconciliation.store(true, Ordering::SeqCst);
                        let _ = this.event_tx.send(ProjectXWsEvent::Reconnected);
                        let _ = this.event_tx.send(ProjectXWsEvent::ReconciliationRequired);
                    }
                    RealtimeEvent::TransportGap => {
                        this.requires_reconciliation.store(true, Ordering::SeqCst);
                        let _ = this.event_tx.send(ProjectXWsEvent::ReconciliationRequired);
                        rx.acknowledge_transport_gap();
                    }
                    RealtimeEvent::Invocation(invocation) => match invocation.target() {
                        "GatewayQuote" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::MarketQuote);
                        }
                        "GatewayTrade" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::MarketTrade);
                        }
                        "GatewayDepth" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::MarketDepth);
                        }
                        "GatewayUserAccount" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::UserAccount);
                        }
                        "GatewayUserOrder" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::UserOrder);
                        }
                        "GatewayUserPosition" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::UserPosition);
                        }
                        "GatewayUserTrade" => {
                            emit_batch(&this, &invocation, ProjectXWsEvent::UserTrade);
                        }
                        target => {
                            log::debug!(
                                "ProjectX {:?} unhandled invocation target: {target}",
                                this.hub
                            );
                        }
                    },
                    RealtimeEvent::Message(_) => {}
                    _ => {}
                }
            }

            this.connected.store(false, Ordering::SeqCst);
            let _ = this.event_tx.send(ProjectXWsEvent::Disconnected);
        });

        *self.pump_task.lock().await = Some(handle);
    }

    fn record_message_activity(&self) {
        self.last_message_at_ms.store(
            get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000,
            Ordering::SeqCst,
        );
    }

    async fn track_subscription(&self, subscription: ProjectXSubscription) {
        let mut guard = self.subscriptions.write().await;

        if !guard.iter().any(|existing| existing == &subscription) {
            guard.push(subscription);
        }
    }

    async fn remove_subscription(&self, target: &str, arguments: &[Value]) {
        let mut guard = self.subscriptions.write().await;
        guard.retain(|subscription| {
            subscription.target != target || subscription.arguments.as_slice() != arguments
        });
    }

    async fn resubscribe_all(&self) -> Result<(), ProjectXWsError> {
        let subscriptions = self.subscriptions.read().await.clone();

        for subscription in subscriptions {
            self.realtime
                .invoke(subscription.target, subscription.arguments)
                .await
                .map_err(to_ws_error)?;
        }
        Ok(())
    }
}

fn emit_batch<T>(
    client: &ProjectXWsClient,
    invocation: &projectx_client::SignalRInvocation,
    wrap: impl Fn(T) -> ProjectXWsEvent,
) where
    T: serde::de::DeserializeOwned,
{
    for decoded in invocation.decode_batch::<T>() {
        match decoded {
            Ok(entity) => {
                let _ = client.event_tx.send(wrap(entity));
            }
            Err(e) => {
                log::warn!(
                    "ProjectX {:?} invocation decode failed for {}: {e}",
                    client.hub,
                    invocation.target()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ProjectXSubscription, ProjectXWsClient};
    use crate::common::{enums::ProjectXHub, urls::ProjectXUrls};

    fn test_client() -> (projectx_client::Client, ProjectXWsClient) {
        let credentials =
            projectx_client::Credentials::new("test-user", "test-key").expect("credentials");
        let client = projectx_client::Client::builder(credentials)
            .endpoints(
                projectx_client::Endpoints::custom("https://example.test", "https://example.test")
                    .expect("endpoints"),
            )
            .build()
            .expect("client");
        let ws = ProjectXWsClient::new(
            &client,
            ProjectXHub::Market,
            ProjectXUrls::new("https://example.test", "https://example.test"),
        );
        (client, ws)
    }

    #[tokio::test]
    async fn track_subscription_is_deduped_and_remove_works() {
        let (_client, ws) = test_client();

        let sub = ProjectXSubscription::new("SubscribeOrders", vec![json!(1)]);
        ws.track_subscription(sub.clone()).await;
        ws.track_subscription(sub).await;
        assert_eq!(ws.subscriptions.read().await.len(), 1);

        ws.remove_subscription("SubscribeOrders", &[json!(1)]).await;
        assert_eq!(ws.subscriptions.read().await.len(), 0);
    }

    #[tokio::test]
    async fn take_event_receiver_is_single_use() {
        let (_client, ws) = test_client();

        assert!(ws.take_event_receiver().await.is_some());
        assert!(ws.take_event_receiver().await.is_none());
    }

    #[tokio::test]
    async fn reconciliation_flag_starts_clear_and_round_trips() {
        let (_client, ws) = test_client();

        assert!(!ws.take_reconciliation_flag());
        ws.requires_reconciliation
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(ws.take_reconciliation_flag());
        assert!(!ws.take_reconciliation_flag());
    }
}
