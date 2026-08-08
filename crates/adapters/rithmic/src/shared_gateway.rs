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

//! Shared gateway registry for live Rithmic clients.
//!
//! This manager deduplicates gateways by login session identity so one shared
//! gateway can own the single upstream plant connections for all local clients
//! attached to the same Rithmic login/system.

use std::sync::{
    Arc, LazyLock, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

use ahash::AHashMap;
use nautilus_common::live::get_runtime;

use crate::{
    Result,
    gateway::{GatewayConfig, RithmicGateway},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GatewayRegistryKey {
    environment: u8,
    username: String,
    password: String,
    system_name: String,
    app_name: String,
    app_version: String,
    server: Option<String>,
    alt_server: Option<String>,
    url_override: Option<String>,
    beta_url_override: Option<String>,
}

impl GatewayRegistryKey {
    fn from_config(config: &GatewayConfig) -> Self {
        Self {
            environment: config.environment as u8,
            username: config.username.clone(),
            password: config.password.clone(),
            system_name: config.system_name.clone(),
            app_name: config.app_name.clone(),
            app_version: config.app_version.clone(),
            server: config.server.clone(),
            alt_server: config.alt_server.clone(),
            url_override: config.url_override.clone(),
            beta_url_override: config.beta_url_override.clone(),
        }
    }
}

static SHARED_GATEWAYS: LazyLock<Mutex<AHashMap<GatewayRegistryKey, Weak<SharedGatewayInner>>>> =
    LazyLock::new(|| Mutex::new(AHashMap::new()));

#[derive(Debug)]
struct SharedGatewayInner {
    key: GatewayRegistryKey,
    gateway: Arc<tokio::sync::RwLock<RithmicGateway>>,
    ref_count: AtomicUsize,
}

impl SharedGatewayInner {
    fn new(config: GatewayConfig) -> Arc<Self> {
        Arc::new(Self {
            key: GatewayRegistryKey::from_config(&config),
            gateway: Arc::new(tokio::sync::RwLock::new(RithmicGateway::new(config))),
            ref_count: AtomicUsize::new(1),
        })
    }

    async fn release(self: Arc<Self>) {
        if self.ref_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        if let Err(e) = self.gateway.write().await.disconnect().await {
            log::warn!("Shared Rithmic gateway disconnect failed during release: {e}");
        }

        let mut registry = SHARED_GATEWAYS
            .lock()
            .expect("shared gateway registry poisoned");
        let remove = registry
            .get(&self.key)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, &self));

        if remove {
            registry.remove(&self.key);
        }
    }
}

/// Reference-counted lease on a shared live gateway.
pub(crate) struct SharedGatewayLease {
    inner: Option<Arc<SharedGatewayInner>>,
}

impl SharedGatewayLease {
    pub(crate) fn acquire(config: GatewayConfig) -> Self {
        let key = GatewayRegistryKey::from_config(&config);
        let mut registry = SHARED_GATEWAYS
            .lock()
            .expect("shared gateway registry poisoned");

        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            existing.ref_count.fetch_add(1, Ordering::AcqRel);
            return Self {
                inner: Some(existing),
            };
        }

        let inner = SharedGatewayInner::new(config);
        registry.insert(key, Arc::downgrade(&inner));
        Self { inner: Some(inner) }
    }

    pub(crate) fn gateway(&self) -> Arc<tokio::sync::RwLock<RithmicGateway>> {
        Arc::clone(
            &self
                .inner
                .as_ref()
                .expect("shared gateway lease already released")
                .gateway,
        )
    }

    pub(crate) async fn connect(&self, requested: &GatewayConfig) -> Result<()> {
        self.gateway()
            .write()
            .await
            .connect_requested(requested)
            .await
    }

    pub(crate) async fn release(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.release().await;
        }
    }
}

impl Drop for SharedGatewayLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            get_runtime().spawn(inner.release());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::{SinkExt, StreamExt};
    use prost::Message as ProstMessage;
    use rithmic_rs::{
        RithmicAccount,
        rti::{
            MessageType, RequestLogin, RequestLogout, RequestPnLPositionSnapshot,
            RequestPnLPositionUpdates, RequestShowOrders, RequestSubscribeForOrderUpdates,
            ResponseLogin, ResponseLogout, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates,
            ResponseShowOrders, ResponseSubscribeForOrderUpdates,
        },
    };
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

    use super::*;
    use crate::{GatewayConfig, RithmicEnv, execution::RithmicExecutionClient};

    #[derive(Clone)]
    struct MockSharedExecutionServerState {
        order_connections: Arc<AtomicUsize>,
        pnl_connections: Arc<AtomicUsize>,
        order_subscription_requests: Arc<Mutex<Vec<String>>>,
        pnl_subscription_requests: Arc<Mutex<Vec<String>>>,
        pnl_snapshot_requests: Arc<Mutex<Vec<String>>>,
        show_orders_requests: Arc<Mutex<Vec<String>>>,
    }

    impl MockSharedExecutionServerState {
        fn new() -> Self {
            Self {
                order_connections: Arc::new(AtomicUsize::new(0)),
                pnl_connections: Arc::new(AtomicUsize::new(0)),
                order_subscription_requests: Arc::new(Mutex::new(Vec::new())),
                pnl_subscription_requests: Arc::new(Mutex::new(Vec::new())),
                pnl_snapshot_requests: Arc::new(Mutex::new(Vec::new())),
                show_orders_requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct MockSharedExecutionServer {
        url: String,
        state: MockSharedExecutionServerState,
        handle: JoinHandle<()>,
    }

    impl MockSharedExecutionServer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let state = MockSharedExecutionServerState::new();
            let state_task = state.clone();

            let handle = tokio::spawn(async move {
                let mut tasks = Vec::new();

                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let state = state_task.clone();

                    tasks.push(tokio::spawn(async move {
                        let mut ws = accept_async(stream).await.unwrap();
                        let mut classified_order = false;
                        let mut classified_pnl = false;

                        while let Some(message) = ws.next().await {
                            match message.unwrap() {
                                Message::Binary(data) => {
                                    let payload = &data[4..];
                                    let message_type = MessageType::decode(payload).unwrap();

                                    match message_type.template_id {
                                        10 => {
                                            let request = RequestLogin::decode(payload).unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            send_protobuf(
                                                &mut ws,
                                                ResponseLogin {
                                                    template_id: 11,
                                                    template_version: Some("5.30".to_string()),
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                    fcm_id: Some("fcm".to_string()),
                                                    ib_id: Some("ib".to_string()),
                                                    country_code: None,
                                                    state_code: None,
                                                    unique_user_id: Some(
                                                        "mock-session".to_string(),
                                                    ),
                                                    heartbeat_interval: Some(60.0),
                                                },
                                            )
                                            .await;
                                        }
                                        308 => {
                                            if !classified_order {
                                                state
                                                    .order_connections
                                                    .fetch_add(1, Ordering::SeqCst);
                                                classified_order = true;
                                            }
                                            let request =
                                                RequestSubscribeForOrderUpdates::decode(payload)
                                                    .unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            let account_id =
                                                request.account_id.clone().unwrap_or_default();
                                            state
                                                .order_subscription_requests
                                                .lock()
                                                .expect("order subscription mutex poisoned")
                                                .push(account_id);
                                            send_protobuf(
                                                &mut ws,
                                                ResponseSubscribeForOrderUpdates {
                                                    template_id: 309,
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                },
                                            )
                                            .await;
                                        }
                                        320 => {
                                            if !classified_order {
                                                state
                                                    .order_connections
                                                    .fetch_add(1, Ordering::SeqCst);
                                                classified_order = true;
                                            }
                                            let request =
                                                RequestShowOrders::decode(payload).unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            let account_id =
                                                request.account_id.clone().unwrap_or_default();
                                            state
                                                .show_orders_requests
                                                .lock()
                                                .expect("show orders mutex poisoned")
                                                .push(account_id);
                                            send_protobuf(
                                                &mut ws,
                                                ResponseShowOrders {
                                                    template_id: 321,
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                },
                                            )
                                            .await;
                                        }
                                        400 => {
                                            if !classified_pnl {
                                                state
                                                    .pnl_connections
                                                    .fetch_add(1, Ordering::SeqCst);
                                                classified_pnl = true;
                                            }
                                            let request =
                                                RequestPnLPositionUpdates::decode(payload).unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            let account_id =
                                                request.account_id.clone().unwrap_or_default();
                                            state
                                                .pnl_subscription_requests
                                                .lock()
                                                .expect("pnl subscription mutex poisoned")
                                                .push(account_id);
                                            send_protobuf(
                                                &mut ws,
                                                ResponsePnLPositionUpdates {
                                                    template_id: 401,
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                },
                                            )
                                            .await;
                                        }
                                        402 => {
                                            if !classified_pnl {
                                                state
                                                    .pnl_connections
                                                    .fetch_add(1, Ordering::SeqCst);
                                                classified_pnl = true;
                                            }
                                            let request =
                                                RequestPnLPositionSnapshot::decode(payload)
                                                    .unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            let account_id =
                                                request.account_id.clone().unwrap_or_default();
                                            state
                                                .pnl_snapshot_requests
                                                .lock()
                                                .expect("pnl snapshot mutex poisoned")
                                                .push(account_id);
                                            send_protobuf(
                                                &mut ws,
                                                ResponsePnLPositionSnapshot {
                                                    template_id: 403,
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                },
                                            )
                                            .await;
                                        }
                                        12 => {
                                            let request = RequestLogout::decode(payload).unwrap();
                                            let request_id =
                                                request.user_msg.first().cloned().unwrap();
                                            send_protobuf(
                                                &mut ws,
                                                ResponseLogout {
                                                    template_id: 13,
                                                    user_msg: vec![request_id],
                                                    rp_code: vec![],
                                                },
                                            )
                                            .await;
                                            break;
                                        }
                                        18 => {}
                                        other => panic!("unexpected request template id {other}"),
                                    }
                                }
                                Message::Ping(payload) => {
                                    ws.send(Message::Pong(payload)).await.unwrap();
                                }
                                Message::Close(_) => break,
                                Message::Text(_) | Message::Pong(_) | Message::Frame(_) => {}
                            }
                        }
                    }));
                }

                for task in tasks {
                    task.await.unwrap();
                }
            });

            Self {
                url: format!("ws://{address}"),
                state,
                handle,
            }
        }

        fn order_connections(&self) -> usize {
            self.state.order_connections.load(Ordering::SeqCst)
        }

        fn pnl_connections(&self) -> usize {
            self.state.pnl_connections.load(Ordering::SeqCst)
        }

        fn order_subscription_requests(&self) -> Vec<String> {
            self.state
                .order_subscription_requests
                .lock()
                .expect("order subscription mutex poisoned")
                .clone()
        }

        fn pnl_subscription_requests(&self) -> Vec<String> {
            self.state
                .pnl_subscription_requests
                .lock()
                .expect("pnl subscription mutex poisoned")
                .clone()
        }

        fn pnl_snapshot_requests(&self) -> Vec<String> {
            self.state
                .pnl_snapshot_requests
                .lock()
                .expect("pnl snapshot mutex poisoned")
                .clone()
        }

        fn show_orders_requests(&self) -> Vec<String> {
            self.state
                .show_orders_requests
                .lock()
                .expect("show orders mutex poisoned")
                .clone()
        }

        async fn wait(self) {
            self.handle.await.unwrap();
        }
    }

    async fn send_protobuf(
        ws: &mut WebSocketStream<tokio::net::TcpStream>,
        message: impl ProstMessage,
    ) {
        ws.send(Message::Binary(encode_message(&message).into()))
            .await
            .unwrap();
    }

    fn encode_message(message: &impl ProstMessage) -> Vec<u8> {
        let mut bytes = Vec::new();
        let len = message.encoded_len() as u32;
        bytes.extend_from_slice(&len.to_be_bytes());
        message.encode(&mut bytes).unwrap();
        bytes
    }

    fn data_config() -> GatewayConfig {
        GatewayConfig::new(RithmicEnv::Demo, "user", "pass", "system", "", "", "")
            .with_app_name("TestApp")
            .with_ticker(true)
            .with_order(false)
            .with_pnl(false)
            .with_history(false)
    }

    fn exec_config(account_id: &str) -> GatewayConfig {
        GatewayConfig::new(
            RithmicEnv::Demo,
            "user",
            "pass",
            "system",
            "fcm",
            "ib",
            account_id,
        )
        .with_app_name("TestApp")
        .with_ticker(false)
        .with_order(true)
        .with_pnl(true)
        .with_history(false)
    }

    #[rstest::rstest]
    fn registry_key_ignores_history_enablement() {
        let config = data_config();
        let key_a = GatewayRegistryKey::from_config(&config);
        let key_b = GatewayRegistryKey::from_config(&config.with_history(true));
        assert_eq!(key_a, key_b);
    }

    #[rstest::rstest]
    fn registry_key_ignores_account_id_for_execution_clients() {
        let key_a = GatewayRegistryKey::from_config(&exec_config("account-1"));
        let key_b = GatewayRegistryKey::from_config(&exec_config("account-2"));
        assert_eq!(key_a, key_b);
    }

    #[tokio::test]
    async fn acquire_reuses_same_gateway_for_matching_key() {
        let config = data_config();
        let lease_a = SharedGatewayLease::acquire(config.clone());
        let lease_b = SharedGatewayLease::acquire(config);

        assert!(Arc::ptr_eq(&lease_a.gateway(), &lease_b.gateway()));

        let mut lease_a = lease_a;
        let mut lease_b = lease_b;
        lease_a.release().await;
        lease_b.release().await;
    }

    #[tokio::test]
    async fn shared_gateway_reuses_order_and_pnl_connections_across_accounts() {
        let server = MockSharedExecutionServer::start().await;
        let config_a = exec_config("account-1").with_url_override(server.url.clone());
        let config_b = exec_config("account-2").with_url_override(server.url.clone());

        let lease_a = SharedGatewayLease::acquire(config_a.clone());
        let lease_b = SharedGatewayLease::acquire(config_b.clone());

        assert!(Arc::ptr_eq(&lease_a.gateway(), &lease_b.gateway()));

        lease_a.connect(&config_a).await.unwrap();
        lease_b.connect(&config_b).await.unwrap();

        let client_a = RithmicExecutionClient::new(
            lease_a.gateway(),
            RithmicAccount::new("fcm", "ib", "account-1"),
        );
        let client_b = RithmicExecutionClient::new(
            lease_b.gateway(),
            RithmicAccount::new("fcm", "ib", "account-2"),
        );

        client_a.subscribe_order_updates().await.unwrap();
        client_b.subscribe_order_updates().await.unwrap();
        client_a.subscribe_pnl_updates().await.unwrap();
        client_b.subscribe_pnl_updates().await.unwrap();
        client_a.request_pnl_snapshot().await.unwrap();
        client_b.request_pnl_snapshot().await.unwrap();
        client_a.query_orders().await.unwrap();
        client_b.query_orders().await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(server.order_connections(), 1);
        assert_eq!(server.pnl_connections(), 1);
        assert_eq!(
            server.order_subscription_requests(),
            vec!["account-1".to_string(), "account-2".to_string()]
        );
        assert_eq!(
            server.pnl_subscription_requests(),
            vec!["account-1".to_string(), "account-2".to_string()]
        );
        assert_eq!(
            server.pnl_snapshot_requests(),
            vec!["account-1".to_string(), "account-2".to_string()]
        );
        assert_eq!(
            server.show_orders_requests(),
            vec!["account-1".to_string(), "account-2".to_string()]
        );

        let mut lease_a = lease_a;
        let mut lease_b = lease_b;
        lease_a.release().await;
        lease_b.release().await;
        server.wait().await;
    }
}
