#![allow(dead_code)]
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

//! Common helpers for Rithmic crate integration tests.

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use rithmic_nt::{GatewayConfig, RithmicDataClient, RithmicEnv, RithmicError, RithmicGateway};
use rithmic_rs::rti::{
    AccountPnLPositionUpdate, BestBidOffer, DepthByOrder, InstrumentPnLPositionUpdate, LastTrade,
    MessageType, RequestAccountList, RequestDepthByOrderSnapshot, RequestDepthByOrderUpdates,
    RequestLogin, RequestLogout, RequestMarketDataUpdate, RequestPnLPositionSnapshot,
    RequestPnLPositionUpdates, RequestReferenceData, RequestSubscribeForOrderUpdates,
    RequestTimeBarReplay, RequestTimeBarUpdate, ResponseAccountList, ResponseDepthByOrderSnapshot,
    ResponseDepthByOrderUpdates, ResponseLogin, ResponseLogout, ResponseMarketDataUpdate,
    ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseReferenceData,
    ResponseSubscribeForOrderUpdates, ResponseTimeBarReplay, ResponseTimeBarUpdate, TimeBar,
    request_market_data_update::{Request as MarketDataRequest, UpdateBits},
    request_pn_l_position_updates::Request as PnlPositionRequest,
    request_time_bar_replay::BarType as TimeBarReplayType,
    request_time_bar_update::{BarType as TimeBarUpdateType, Request as TimeBarUpdateRequest},
};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{net::TcpListener, sync::RwLock, task::JoinHandle};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

pub(crate) fn test_gateway_config() -> GatewayConfig {
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
    .unwrap()
    .with_history(true)
}

pub(crate) fn test_gateway() -> RithmicGateway {
    RithmicGateway::new(test_gateway_config())
}

pub(crate) fn test_gateway_arc() -> Arc<RwLock<RithmicGateway>> {
    Arc::new(RwLock::new(test_gateway()))
}

pub(crate) fn test_data_client() -> RithmicDataClient {
    RithmicDataClient::new(test_gateway_arc())
}

fn base_test_gateway_config(url: &str) -> GatewayConfig {
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
    .unwrap()
    .with_url_override(url)
}

pub(crate) fn test_ticker_only_gateway_config(url: &str) -> GatewayConfig {
    base_test_gateway_config(url)
        .with_ticker(true)
        .with_order(false)
        .with_pnl(false)
        .with_history(false)
}

pub(crate) fn test_order_only_gateway_config(url: &str) -> GatewayConfig {
    base_test_gateway_config(url)
        .with_ticker(false)
        .with_order(true)
        .with_pnl(false)
        .with_history(false)
}

pub(crate) fn test_pnl_only_gateway_config(url: &str) -> GatewayConfig {
    base_test_gateway_config(url)
        .with_ticker(false)
        .with_order(false)
        .with_pnl(true)
        .with_history(false)
}

pub(crate) fn test_history_only_gateway_config(url: &str) -> GatewayConfig {
    base_test_gateway_config(url)
        .with_ticker(false)
        .with_order(false)
        .with_pnl(false)
        .with_history(true)
}

pub(crate) fn assert_connection_error(err: RithmicError, expected: &str) {
    match err {
        RithmicError::Connection(message) => assert_eq!(message, expected),
        other => panic!("expected connection error {expected:?}, was {other:?}"),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct QuoteTradeFixture {
    quote: QuoteFixture,
    trade: TradeFixture,
}

#[derive(Debug, Clone, Deserialize)]
struct TimeBarReplayFixture {
    responses: Vec<TimeBarReplayResponseFixture>,
}

#[derive(Debug, Clone, Deserialize)]
struct DepthSnapshotFixture {
    snapshots: Vec<SnapshotRowFixture>,
    delta: DepthDeltaFixture,
}

#[derive(Debug, Clone, Deserialize)]
struct QuoteFixture {
    template_id: i32,
    symbol: String,
    exchange: String,
    presence_bits: u32,
    clear_bits: Option<u32>,
    is_snapshot: bool,
    bid_price: f64,
    bid_size: i32,
    bid_orders: Option<i32>,
    bid_implicit_size: Option<i32>,
    bid_time: Option<String>,
    ask_price: f64,
    ask_size: i32,
    ask_orders: Option<i32>,
    ask_implicit_size: Option<i32>,
    ask_time: Option<String>,
    lean_price: Option<f64>,
    ssboe: i32,
    usecs: i32,
}

impl QuoteFixture {
    fn into_message(self) -> BestBidOffer {
        BestBidOffer {
            template_id: self.template_id,
            symbol: Some(self.symbol),
            exchange: Some(self.exchange),
            presence_bits: Some(self.presence_bits),
            clear_bits: self.clear_bits,
            is_snapshot: Some(self.is_snapshot),
            bid_price: Some(self.bid_price),
            bid_size: Some(self.bid_size),
            bid_orders: self.bid_orders,
            bid_implicit_size: self.bid_implicit_size,
            bid_time: self.bid_time,
            ask_price: Some(self.ask_price),
            ask_size: Some(self.ask_size),
            ask_orders: self.ask_orders,
            ask_implicit_size: self.ask_implicit_size,
            ask_time: self.ask_time,
            lean_price: self.lean_price,
            ssboe: Some(self.ssboe),
            usecs: Some(self.usecs),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TradeFixture {
    template_id: i32,
    symbol: String,
    exchange: String,
    presence_bits: Option<u32>,
    clear_bits: Option<u32>,
    is_snapshot: bool,
    trade_price: f64,
    trade_size: i32,
    aggressor: i32,
    exchange_order_id: String,
    aggressor_exchange_order_id: Option<String>,
    net_change: Option<f64>,
    percent_change: Option<f64>,
    volume: Option<u64>,
    vwap: Option<f64>,
    trade_time: Option<String>,
    ssboe: i32,
    usecs: i32,
    source_ssboe: Option<i32>,
    source_usecs: Option<i32>,
    source_nsecs: Option<i32>,
    jop_ssboe: Option<i32>,
    jop_nsecs: Option<i32>,
}

impl TradeFixture {
    fn into_message(self) -> LastTrade {
        LastTrade {
            template_id: self.template_id,
            symbol: Some(self.symbol),
            exchange: Some(self.exchange),
            presence_bits: self.presence_bits,
            clear_bits: self.clear_bits,
            is_snapshot: Some(self.is_snapshot),
            trade_price: Some(self.trade_price),
            trade_size: Some(self.trade_size),
            aggressor: Some(self.aggressor),
            exchange_order_id: Some(self.exchange_order_id),
            aggressor_exchange_order_id: self.aggressor_exchange_order_id,
            net_change: self.net_change,
            percent_change: self.percent_change,
            volume: self.volume,
            vwap: self.vwap,
            trade_time: self.trade_time,
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

#[derive(Debug, Clone, Deserialize)]
struct TimeBarReplayResponseFixture {
    template_id: i32,
    request_key: String,
    user_msg: Vec<String>,
    rq_handler_rp_code: Vec<String>,
    rp_code: Vec<String>,
    symbol: String,
    exchange: String,
    #[serde(rename = "type")]
    bar_type: i32,
    period: String,
    marker: i32,
    num_trades: u64,
    volume: u64,
    bid_volume: u64,
    ask_volume: u64,
    open_price: f64,
    close_price: f64,
    high_price: f64,
    low_price: f64,
    settlement_price: Option<f64>,
    has_settlement_price: bool,
    must_clear_settlement_price: bool,
}

impl TimeBarReplayResponseFixture {
    fn into_message(self) -> ResponseTimeBarReplay {
        ResponseTimeBarReplay {
            template_id: self.template_id,
            request_key: Some(self.request_key),
            user_msg: self.user_msg,
            rq_handler_rp_code: self.rq_handler_rp_code,
            rp_code: self.rp_code,
            symbol: Some(self.symbol),
            exchange: Some(self.exchange),
            r#type: Some(self.bar_type),
            period: Some(self.period),
            marker: Some(self.marker),
            num_trades: Some(self.num_trades),
            volume: Some(self.volume),
            bid_volume: Some(self.bid_volume),
            ask_volume: Some(self.ask_volume),
            open_price: Some(self.open_price),
            close_price: Some(self.close_price),
            high_price: Some(self.high_price),
            low_price: Some(self.low_price),
            settlement_price: self.settlement_price,
            has_settlement_price: Some(self.has_settlement_price),
            must_clear_settlement_price: Some(self.must_clear_settlement_price),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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
        fs::read(&path).unwrap_or_else(|e| panic!("failed reading {}: {e}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("failed parsing fixture {}: {e}", path.display()))
}

pub(crate) struct MockTickerPlant {
    pub url: String,
    subscribe_requests: Arc<AtomicUsize>,
    order_book_subscribe_requests: Arc<AtomicUsize>,
    handle: JoinHandle<()>,
}

impl MockTickerPlant {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let subscribe_requests = Arc::new(AtomicUsize::new(0));
        let subscribe_requests_task = Arc::clone(&subscribe_requests);
        let order_book_subscribe_requests = Arc::new(AtomicUsize::new(0));
        let order_book_subscribe_requests_task = Arc::clone(&order_book_subscribe_requests);

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Binary(data) => {
                        let payload = &data[4..];
                        let message_type = MessageType::decode(payload).unwrap();

                        match message_type.template_id {
                            10 => {
                                let request = RequestLogin::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, login_response(request_id)).await;
                            }
                            100 => {
                                let request = RequestMarketDataUpdate::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();
                                let symbol = request.symbol.clone().unwrap();
                                let exchange = request.exchange.clone().unwrap();
                                let fixture = load_fixture::<QuoteTradeFixture>(
                                    "market_data_quote_trade.json",
                                );

                                assert_eq!(
                                    request.request,
                                    Some(MarketDataRequest::Subscribe as i32)
                                );
                                assert_eq!(
                                    request.update_bits,
                                    Some(UpdateBits::LastTrade as u32 | UpdateBits::Bbo as u32)
                                );

                                subscribe_requests_task.fetch_add(1, Ordering::SeqCst);

                                ws.send(Message::Binary(
                                    encode_message(&ResponseMarketDataUpdate {
                                        template_id: 101,
                                        user_msg: vec![request_id],
                                        rp_code: vec![],
                                    })
                                    .into(),
                                ))
                                .await
                                .unwrap();

                                let mut quote = fixture.quote.into_message();
                                quote.symbol = Some(symbol.clone());
                                quote.exchange = Some(exchange.clone());
                                ws.send(Message::Binary(encode_message(&quote).into()))
                                    .await
                                    .unwrap();

                                let mut trade = fixture.trade.into_message();
                                trade.symbol = Some(symbol);
                                trade.exchange = Some(exchange);
                                ws.send(Message::Binary(encode_message(&trade).into()))
                                    .await
                                    .unwrap();
                            }
                            14 => {
                                let request = RequestReferenceData::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();
                                let symbol = request.symbol.clone().unwrap_or_default();
                                let exchange = request.exchange.clone().unwrap_or_default();

                                ws.send(Message::Binary(
                                    encode_message(&ResponseReferenceData {
                                        template_id: 15,
                                        user_msg: vec![request_id],
                                        rp_code: vec![],
                                        symbol: Some(symbol),
                                        exchange: Some(exchange),
                                        symbol_name: Some("E-mini S&P 500".to_string()),
                                        currency: Some("USD".to_string()),
                                        product_code: Some("ES".to_string()),
                                        min_qprice_change: Some(0.25),
                                        single_point_value: Some(50.0),
                                        is_tradable: Some("true".to_string()),
                                        ..Default::default()
                                    })
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                            115 => {
                                let request = RequestDepthByOrderSnapshot::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();
                                let symbol = request.symbol.clone().unwrap_or_default();
                                let exchange = request.exchange.clone().unwrap_or_default();
                                let fixture = load_fixture::<DepthSnapshotFixture>(
                                    "depth_snapshot_delta.json",
                                );
                                for (index, snapshot) in fixture.snapshots.into_iter().enumerate() {
                                    let mut snapshot = snapshot.into_message();
                                    snapshot.template_id = 116;
                                    snapshot.user_msg = vec![request_id.clone()];
                                    snapshot.symbol = Some(symbol.clone());
                                    snapshot.exchange = Some(exchange.clone());
                                    snapshot.rq_handler_rp_code = if index == 0 {
                                        vec!["0".to_string()]
                                    } else {
                                        vec![]
                                    };
                                    ws.send(Message::Binary(encode_message(&snapshot).into()))
                                        .await
                                        .unwrap();
                                }
                            }
                            117 => {
                                let request = RequestDepthByOrderUpdates::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();
                                let symbol = request.symbol.clone().unwrap_or_default();
                                let exchange = request.exchange.clone().unwrap_or_default();
                                match request.request.and_then(|value| value.try_into().ok()) {
                                    Some(
                                        rithmic_rs::rti::request_depth_by_order_updates::Request::Subscribe,
                                    ) => {
                                        order_book_subscribe_requests_task
                                            .fetch_add(1, Ordering::SeqCst);
                                        let fixture = load_fixture::<DepthSnapshotFixture>(
                                            "depth_snapshot_delta.json",
                                        );
                                        let mut delta = fixture.delta.into_message();
                                        delta.template_id = 160;
                                        delta.symbol = Some(symbol.clone());
                                        delta.exchange = Some(exchange.clone());
                                        ws.send(Message::Binary(encode_message(&delta).into()))
                                            .await
                                            .unwrap();
                                    }
                                    Some(
                                        rithmic_rs::rti::request_depth_by_order_updates::Request::Unsubscribe,
                                    ) => {}
                                    None => panic!("unexpected depth-by-order request"),
                                }

                                ws.send(Message::Binary(
                                    encode_message(&ResponseDepthByOrderUpdates {
                                        template_id: 118,
                                        user_msg: vec![request_id],
                                        rp_code: vec![],
                                    })
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                            12 => {
                                let request = RequestLogout::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, logout_response(request_id)).await;
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
        });

        Self {
            url: format!("ws://{address}"),
            subscribe_requests,
            order_book_subscribe_requests,
            handle,
        }
    }

    pub(crate) fn subscribe_requests(&self) -> usize {
        self.subscribe_requests.load(Ordering::SeqCst)
    }

    pub(crate) fn order_book_subscribe_requests(&self) -> usize {
        self.order_book_subscribe_requests.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait(self) {
        self.handle.await.unwrap();
    }
}

pub(crate) struct MockOrderPlant {
    pub url: String,
    handle: JoinHandle<()>,
}

impl MockOrderPlant {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Binary(data) => {
                        let payload = &data[4..];
                        let message_type = MessageType::decode(payload).unwrap();

                        match message_type.template_id {
                            10 => {
                                let request = RequestLogin::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, login_response(request_id)).await;
                            }
                            302 => {
                                let request = RequestAccountList::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                assert_eq!(request.fcm_id, None);
                                assert_eq!(request.ib_id, None);

                                send_protobuf(
                                    &mut ws,
                                    ResponseAccountList {
                                        template_id: 303,
                                        user_msg: vec![request_id.clone()],
                                        rq_handler_rp_code: vec!["0".to_string()],
                                        rp_code: vec![],
                                        fcm_id: Some("fcm".to_string()),
                                        ib_id: Some("ib".to_string()),
                                        account_id: Some("account-1".to_string()),
                                        account_name: Some("Primary".to_string()),
                                        account_currency: Some("USD".to_string()),
                                        account_auto_liquidate: None,
                                        auto_liq_threshold_current_value: None,
                                    },
                                )
                                .await;

                                send_protobuf(
                                    &mut ws,
                                    ResponseAccountList {
                                        template_id: 303,
                                        user_msg: vec![request_id],
                                        rq_handler_rp_code: vec![],
                                        rp_code: vec![],
                                        fcm_id: Some("fcm".to_string()),
                                        ib_id: Some("ib".to_string()),
                                        account_id: Some("account-2".to_string()),
                                        account_name: Some("Secondary".to_string()),
                                        account_currency: Some("USD".to_string()),
                                        account_auto_liquidate: None,
                                        auto_liq_threshold_current_value: None,
                                    },
                                )
                                .await;
                            }
                            308 => {
                                let request =
                                    RequestSubscribeForOrderUpdates::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                assert_eq!(request.fcm_id.as_deref(), Some("fcm"));
                                assert_eq!(request.ib_id.as_deref(), Some("ib"));
                                assert_eq!(request.account_id.as_deref(), Some("account"));

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
                            12 => {
                                let request = RequestLogout::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, logout_response(request_id)).await;
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
        });

        Self {
            url: format!("ws://{address}"),
            handle,
        }
    }

    pub(crate) async fn wait(self) {
        self.handle.await.unwrap();
    }
}

pub(crate) struct MockPnlPlant {
    pub url: String,
    snapshot_requests: Arc<AtomicUsize>,
    handle: JoinHandle<()>,
}

impl MockPnlPlant {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let snapshot_requests = Arc::new(AtomicUsize::new(0));
        let snapshot_requests_task = Arc::clone(&snapshot_requests);

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Binary(data) => {
                        let payload = &data[4..];
                        let message_type = MessageType::decode(payload).unwrap();

                        match message_type.template_id {
                            10 => {
                                let request = RequestLogin::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, login_response(request_id)).await;
                            }
                            400 => {
                                let request = RequestPnLPositionUpdates::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                assert_eq!(
                                    request.request,
                                    Some(PnlPositionRequest::Subscribe as i32)
                                );
                                assert_eq!(request.fcm_id.as_deref(), Some("fcm"));
                                assert_eq!(request.ib_id.as_deref(), Some("ib"));
                                assert_eq!(request.account_id.as_deref(), Some("account"));

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
                                let request = RequestPnLPositionSnapshot::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                assert_eq!(request.fcm_id.as_deref(), Some("fcm"));
                                assert_eq!(request.ib_id.as_deref(), Some("ib"));
                                assert_eq!(request.account_id.as_deref(), Some("account"));
                                snapshot_requests_task.fetch_add(1, Ordering::SeqCst);

                                send_protobuf(
                                    &mut ws,
                                    ResponsePnLPositionSnapshot {
                                        template_id: 403,
                                        user_msg: vec![request_id],
                                        rp_code: vec![],
                                    },
                                )
                                .await;

                                send_protobuf(
                                    &mut ws,
                                    AccountPnLPositionUpdate {
                                        template_id: 451,
                                        is_snapshot: Some(true),
                                        fcm_id: Some("fcm".to_string()),
                                        ib_id: Some("ib".to_string()),
                                        account_id: Some("account".to_string()),
                                        fill_buy_qty: None,
                                        fill_sell_qty: None,
                                        order_buy_qty: None,
                                        order_sell_qty: None,
                                        buy_qty: Some(3),
                                        sell_qty: Some(1),
                                        open_long_options_value: None,
                                        open_short_options_value: None,
                                        closed_options_value: None,
                                        option_cash_reserved: None,
                                        rms_account_commission: None,
                                        open_position_pnl: Some("1250.75".to_string()),
                                        open_position_quantity: Some(2),
                                        closed_position_pnl: Some("100.25".to_string()),
                                        closed_position_quantity: Some(1),
                                        net_quantity: Some(2),
                                        excess_buy_margin: None,
                                        margin_balance: Some("25000.25".to_string()),
                                        min_margin_balance: None,
                                        min_account_balance: None,
                                        account_balance: Some("100000.50".to_string()),
                                        cash_on_hand: Some("75000.25".to_string()),
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
                                        ssboe: Some(1_700_000_001),
                                        usecs: Some(456_789),
                                    },
                                )
                                .await;

                                send_protobuf(
                                    &mut ws,
                                    InstrumentPnLPositionUpdate {
                                        template_id: 450,
                                        is_snapshot: Some(true),
                                        fcm_id: Some("fcm".to_string()),
                                        ib_id: Some("ib".to_string()),
                                        account_id: Some("account".to_string()),
                                        symbol: Some("ESM6".to_string()),
                                        exchange: Some("CME".to_string()),
                                        product_code: Some("ES".to_string()),
                                        instrument_type: Some("FUT".to_string()),
                                        fill_buy_qty: None,
                                        fill_sell_qty: None,
                                        order_buy_qty: None,
                                        order_sell_qty: None,
                                        buy_qty: Some(3),
                                        sell_qty: Some(1),
                                        avg_open_fill_price: Some(4500.25),
                                        day_open_pnl: Some(300.5),
                                        day_closed_pnl: Some(25.25),
                                        day_pnl: Some(325.75),
                                        day_open_pnl_offset: None,
                                        day_closed_pnl_offset: None,
                                        mtm_security: None,
                                        open_long_options_value: None,
                                        open_short_options_value: None,
                                        closed_options_value: None,
                                        option_cash_reserved: None,
                                        open_position_pnl: Some("300.50".to_string()),
                                        open_position_quantity: Some(2),
                                        closed_position_pnl: Some("25.25".to_string()),
                                        closed_position_quantity: Some(1),
                                        net_quantity: Some(2),
                                        ssboe: Some(1_700_000_001),
                                        usecs: Some(654_321),
                                    },
                                )
                                .await;
                            }
                            12 => {
                                let request = RequestLogout::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, logout_response(request_id)).await;
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
        });

        Self {
            url: format!("ws://{address}"),
            snapshot_requests,
            handle,
        }
    }

    pub(crate) fn snapshot_requests(&self) -> usize {
        self.snapshot_requests.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait(self) {
        self.handle.await.unwrap();
    }
}

pub(crate) struct MockHistoryPlant {
    pub url: String,
    bar_requests: Arc<AtomicUsize>,
    live_bar_subscriptions: Arc<AtomicUsize>,
    handle: JoinHandle<()>,
}

impl MockHistoryPlant {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let bar_requests = Arc::new(AtomicUsize::new(0));
        let bar_requests_task = Arc::clone(&bar_requests);
        let live_bar_subscriptions = Arc::new(AtomicUsize::new(0));
        let live_bar_subscriptions_task = Arc::clone(&live_bar_subscriptions);

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Binary(data) => {
                        let payload = &data[4..];
                        let message_type = MessageType::decode(payload).unwrap();

                        match message_type.template_id {
                            10 => {
                                let request = RequestLogin::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, login_response(request_id)).await;
                            }
                            202 => {
                                let request = RequestTimeBarReplay::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();
                                let fixture = load_fixture::<TimeBarReplayFixture>(
                                    "history_time_bar_replay.json",
                                );

                                assert_eq!(request.symbol.as_deref(), Some("ESM6"));
                                assert_eq!(request.exchange.as_deref(), Some("CME"));
                                assert_eq!(
                                    request.bar_type,
                                    Some(TimeBarReplayType::MinuteBar as i32)
                                );
                                assert_eq!(request.bar_type_period, Some(1));
                                assert_eq!(request.start_index, Some(1_700_000_000));
                                assert_eq!(request.finish_index, Some(1_700_000_060));

                                bar_requests_task.fetch_add(1, Ordering::SeqCst);

                                for response in fixture.responses {
                                    let mut response = response.into_message();
                                    response.user_msg = vec![request_id.clone()];
                                    send_protobuf(&mut ws, response).await;
                                }
                            }
                            200 => {
                                let request = RequestTimeBarUpdate::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                assert_eq!(request.symbol.as_deref(), Some("ESM6"));
                                assert_eq!(request.exchange.as_deref(), Some("CME"));
                                assert_eq!(
                                    request.bar_type,
                                    Some(TimeBarUpdateType::MinuteBar as i32)
                                );
                                assert_eq!(request.bar_type_period, Some(1));

                                if request.request == Some(TimeBarUpdateRequest::Subscribe as i32) {
                                    live_bar_subscriptions_task.fetch_add(1, Ordering::SeqCst);

                                    send_protobuf(
                                        &mut ws,
                                        ResponseTimeBarUpdate {
                                            template_id: 201,
                                            user_msg: vec![request_id],
                                            rp_code: vec![],
                                        },
                                    )
                                    .await;

                                    send_protobuf(
                                        &mut ws,
                                        TimeBar {
                                            template_id: 250,
                                            symbol: Some("ESM6".to_string()),
                                            exchange: Some("CME".to_string()),
                                            r#type: Some(TimeBarUpdateType::MinuteBar as i32),
                                            period: Some("1".to_string()),
                                            marker: Some(1_700_000_120),
                                            num_trades: Some(14),
                                            volume: Some(110),
                                            bid_volume: Some(50),
                                            ask_volume: Some(60),
                                            open_price: Some(4500.75),
                                            close_price: Some(4501.25),
                                            high_price: Some(4501.50),
                                            low_price: Some(4500.50),
                                            settlement_price: None,
                                            has_settlement_price: Some(false),
                                            must_clear_settlement_price: Some(false),
                                        },
                                    )
                                    .await;
                                } else {
                                    assert_eq!(
                                        request.request,
                                        Some(TimeBarUpdateRequest::Unsubscribe as i32)
                                    );

                                    send_protobuf(
                                        &mut ws,
                                        ResponseTimeBarUpdate {
                                            template_id: 201,
                                            user_msg: vec![request_id],
                                            rp_code: vec![],
                                        },
                                    )
                                    .await;
                                }
                            }
                            12 => {
                                let request = RequestLogout::decode(payload).unwrap();
                                let request_id = request.user_msg.first().cloned().unwrap();

                                send_protobuf(&mut ws, logout_response(request_id)).await;
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
        });

        Self {
            url: format!("ws://{address}"),
            bar_requests,
            live_bar_subscriptions,
            handle,
        }
    }

    pub(crate) fn bar_requests(&self) -> usize {
        self.bar_requests.load(Ordering::SeqCst)
    }

    pub(crate) fn live_bar_subscriptions(&self) -> usize {
        self.live_bar_subscriptions.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait(self) {
        self.handle.await.unwrap();
    }
}

fn login_response(request_id: String) -> ResponseLogin {
    ResponseLogin {
        template_id: 11,
        template_version: Some("5.30".to_string()),
        user_msg: vec![request_id],
        rp_code: vec![],
        fcm_id: Some("fcm".to_string()),
        ib_id: Some("ib".to_string()),
        country_code: None,
        state_code: None,
        unique_user_id: Some("mock-session".to_string()),
        heartbeat_interval: Some(60.0),
    }
}

fn logout_response(request_id: String) -> ResponseLogout {
    ResponseLogout {
        template_id: 13,
        user_msg: vec![request_id],
        rp_code: vec![],
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
