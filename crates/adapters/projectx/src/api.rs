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

//! Public adapter surface.
//!
//! Re-exports the `projectx-client` (v2) types the adapter consumes plus the
//! nautilus-compatible wrapper clients.

pub use projectx_client::{
    Account, AccountId, Bar, BarUnit, Bracket, CancelOrder, CloseContract, Contract, ContractId,
    DepthType, HistoryRequest, MarketDepth, MarketQuote, MarketTrade, ModifyOrder,
    OperationResponse, Order, OrderId, OrderResponse, OrderSearch, OrderStatus, OrderType,
    PartialCloseContract, PlaceOrder, Position, PositionType, SearchContracts, Side, Timestamp,
    Trade, TradeLogType, TradeSearch,
};

pub use crate::{
    http::client::ProjectXHttpClient,
    websocket::{
        client::{ProjectXSubscription, ProjectXWsClient, ProjectXWsEvent},
        error::ProjectXWsError,
    },
};
