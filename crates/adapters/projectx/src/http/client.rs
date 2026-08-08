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

use projectx_client::{
    Account, AccountId, Bar, CancelOrder, Client, CloseContract, Contract, ContractId, Credentials,
    Endpoints, HistoryRequest, ModifyOrder, Order, OrderResponse, OrderSearch,
    PartialCloseContract, PlaceOrder, Position, SearchContracts, Trade, TradeSearch,
};

use crate::{
    common::urls::ProjectXUrls,
    config::ProjectXConfig,
    http::{credentials::ProjectXCredential, error::ProjectXHttpError},
};

/// Nautilus-compatible wrapper around the published `projectx-client` (v2) crate.
///
/// The inner [`Client`] owns authentication, token rotation, rate limits, and
/// retries; this wrapper only adapts construction and error handling for the
/// adapter's consumers.
#[derive(Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.projectx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.projectx")
)]
pub struct ProjectXHttpClient {
    credential: ProjectXCredential,
    urls: ProjectXUrls,
    inner: Client,
}

impl Clone for ProjectXHttpClient {
    fn clone(&self) -> Self {
        Self {
            credential: self.credential.clone(),
            urls: self.urls.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl ProjectXHttpClient {
    /// Returns the inner `projectx-client` client.
    #[must_use]
    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// Creates a new ProjectX HTTP client.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials or client configuration are invalid.
    pub fn new(
        credential: ProjectXCredential,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        proxy_url: Option<String>,
    ) -> Result<Self, ProjectXHttpError> {
        let urls = credential.environment.urls();
        let credentials = Credentials::new(&credential.user_name, &credential.api_key)?;

        let endpoints = if urls.api_base.starts_with("https://api.topstepx") {
            Endpoints::topstepx()
        } else {
            Endpoints::custom(&urls.api_base, &urls.rtc_base)?
        };

        let mut builder = Client::builder(credentials).endpoints(endpoints);

        if let Some(timeout_secs) = timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs(timeout_secs));
        }

        if let Some(max_retries) = max_retries {
            builder = builder.max_retries(max_retries);
        }

        if let (Some(initial), Some(maximum)) = (retry_delay_initial_ms, retry_delay_max_ms) {
            builder = builder.retry_delays(
                std::time::Duration::from_millis(initial),
                std::time::Duration::from_millis(maximum),
            );
        }

        if let Some(proxy_url) = proxy_url {
            builder = builder.proxy(proxy_url);
        }

        let inner = builder.build()?;

        Ok(Self {
            credential,
            urls,
            inner,
        })
    }

    /// Creates a new ProjectX HTTP client from an adapter configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials or client configuration are invalid.
    pub fn from_config(config: ProjectXConfig) -> Result<Self, ProjectXHttpError> {
        Self::new(
            config.credential,
            Some(config.http_timeout_secs),
            Some(config.max_retries),
            Some(config.retry_delay_initial_ms),
            Some(config.retry_delay_max_ms),
            config.http_proxy_url,
        )
    }

    #[must_use]
    pub fn urls(&self) -> &ProjectXUrls {
        &self.urls
    }

    /// Authenticates with the ProjectX gateway.
    ///
    /// # Errors
    ///
    /// Returns an error if authentication fails.
    pub async fn start(&self) -> Result<(), ProjectXHttpError> {
        Ok(self.inner.authenticate().await?)
    }

    /// Stops the client.
    ///
    /// `projectx-client` owns token rotation and has no persistent background
    /// task to shut down, so this is a no-op retained for API compatibility.
    pub fn stop(&self) {}

    #[must_use]
    pub fn create_ws_client(
        &self,
        hub: crate::common::enums::ProjectXHub,
    ) -> crate::websocket::client::ProjectXWsClient {
        crate::websocket::client::ProjectXWsClient::new(&self.inner, hub, self.urls.clone())
    }

    /// Searches active provider accounts.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_accounts(&self) -> Result<Vec<Account>, ProjectXHttpError> {
        Ok(self.inner.search_active_accounts().await?)
    }

    /// Retrieves historical bars.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn retrieve_bars(
        &self,
        request: &HistoryRequest,
    ) -> Result<Vec<Bar>, ProjectXHttpError> {
        Ok(self.inner.retrieve_bars(request).await?)
    }

    /// Lists available contracts for a data subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn available_contracts(
        &self,
        live: bool,
    ) -> Result<Vec<Contract>, ProjectXHttpError> {
        Ok(self.inner.available_contracts(live).await?)
    }

    /// Searches contracts by text.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_contracts(
        &self,
        live: bool,
        search_text: &str,
    ) -> Result<Vec<Contract>, ProjectXHttpError> {
        Ok(self
            .inner
            .search_contracts(&SearchContracts {
                live,
                search_text: search_text.to_string(),
            })
            .await?)
    }

    /// Retrieves one contract by its provider identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn contract_by_id(
        &self,
        contract_id: &ContractId,
    ) -> Result<Contract, ProjectXHttpError> {
        Ok(self.inner.contract_by_id(contract_id).await?)
    }

    /// Searches historical orders.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_orders(
        &self,
        request: &OrderSearch,
    ) -> Result<Vec<Order>, ProjectXHttpError> {
        Ok(self.inner.search_orders(request).await?)
    }

    /// Searches open orders for an account.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_open_orders(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<Order>, ProjectXHttpError> {
        Ok(self.inner.search_open_orders(account_id).await?)
    }

    /// Places an order.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn place_order(
        &self,
        request: &PlaceOrder,
    ) -> Result<OrderResponse, ProjectXHttpError> {
        Ok(self.inner.place_order(request).await?)
    }

    /// Cancels an order.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn cancel_order(&self, request: &CancelOrder) -> Result<(), ProjectXHttpError> {
        self.inner.cancel_order(request).await?;
        Ok(())
    }

    /// Modifies an order.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn modify_order(&self, request: &ModifyOrder) -> Result<(), ProjectXHttpError> {
        self.inner.modify_order(request).await?;
        Ok(())
    }

    /// Searches open positions for an account.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_open_positions(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<Position>, ProjectXHttpError> {
        Ok(self.inner.search_open_positions(account_id).await?)
    }

    /// Closes a contract position.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn close_contract(&self, request: &CloseContract) -> Result<(), ProjectXHttpError> {
        self.inner.close_contract(request).await?;
        Ok(())
    }

    /// Partially closes a contract position.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn partial_close_contract(
        &self,
        request: &PartialCloseContract,
    ) -> Result<(), ProjectXHttpError> {
        self.inner.partial_close_contract(request).await?;
        Ok(())
    }

    /// Searches historical trades.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_trades(
        &self,
        request: &TradeSearch,
    ) -> Result<Vec<Trade>, ProjectXHttpError> {
        Ok(self.inner.search_trades(request).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectXHttpClient;
    use crate::{common::enums::ProjectXEnvironment, http::credentials::ProjectXCredential};

    fn test_client() -> ProjectXHttpClient {
        ProjectXHttpClient::new(
            ProjectXCredential::new(ProjectXEnvironment::TopstepX, "test-user", "test-key"),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("test HTTP client")
    }

    #[rstest::rstest]
    fn client_construction_preserves_environment_urls() {
        let client = test_client();
        assert_eq!(client.urls().api_base, "https://api.topstepx.com");
        assert_eq!(client.urls().rtc_base, "https://rtc.topstepx.com");
    }

    #[rstest::rstest]
    fn client_is_cloneable_and_authenticates_lazily() {
        let client = test_client();
        let cloned = client.clone();
        assert_eq!(cloned.urls().api_base, client.urls().api_base);
    }
}
