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

//! Rithmic instrument provider implementation.

use std::{fmt::Debug, sync::Arc};

use dashmap::DashMap;
use nautilus_core::UnixNanos;
use nautilus_model::instruments::{Instrument, InstrumentAny};
use rithmic_rs::plants::ticker_plant::RithmicTickerPlantHandle;

use super::{
    discovery::{
        RithmicInstrumentSymbol, discover_all_symbols_with_handle,
        discover_exchange_symbols_with_handle, discover_product_symbols_with_handle,
        enabled_exchange_names,
    },
    front_month::{
        fetch_instrument_with_handle, instrument_is_tradeable,
        load_front_month_instrument_with_handle, load_supported_front_months_with_handle,
    },
};
use crate::{
    common::consts::exchanges::KNOWN_EXCHANGES,
    error::{Result, RithmicError},
    gateway::RithmicGateway,
};

fn now_nanos() -> UnixNanos {
    crate::common::converters::now_unix_nanos()
}

fn cache_key(symbol: &str, exchange: Option<&str>) -> String {
    match exchange {
        Some(exchange) => format!("{exchange}:{symbol}"),
        None => format!(":{symbol}"),
    }
}

/// Provides instrument definitions from Rithmic.
///
/// This provider connects to Rithmic's ticker plant to fetch instrument
/// reference data and normalize it into Nautilus futures contracts.
///
/// # Example
///
/// ```rust,ignore
/// use nautilus_model::instruments::Instrument;
/// use rithmic_nt::{GatewayConfig, RithmicGateway, RithmicInstrumentProvider};
///
/// let config = GatewayConfig::from_env()?;
/// let mut gateway = RithmicGateway::new(config);
/// gateway.connect().await?;
/// let gateway = Arc::new(gateway);
/// let provider = RithmicInstrumentProvider::new(Arc::clone(&gateway));
/// let instrument = provider.load_instrument_async("ESH5", "CME").await?;
/// println!("Price increment: {}", instrument.price_increment());
/// ```
pub struct RithmicInstrumentProvider {
    gateway: Arc<RithmicGateway>,
    instruments: DashMap<String, InstrumentAny>,
    loaded_exchanges: tokio::sync::RwLock<Vec<String>>,
}

impl RithmicInstrumentProvider {
    /// Creates a new instrument provider with the given gateway.
    pub fn new(gateway: Arc<RithmicGateway>) -> Self {
        Self {
            gateway,
            instruments: DashMap::new(),
            loaded_exchanges: tokio::sync::RwLock::new(Vec::new()),
        }
    }

    /// Returns a reference to the gateway.
    pub fn gateway(&self) -> &Arc<RithmicGateway> {
        &self.gateway
    }

    /// Discovers raw contract listings across all enabled supported exchanges.
    ///
    /// This returns the light-weight `search_symbols(...)` rows rather than
    /// fully resolved Nautilus `FuturesContract` objects.
    pub async fn discover_all_symbols_async(&self) -> Result<Vec<RithmicInstrumentSymbol>> {
        let exchanges = match self.enabled_exchanges_async().await {
            Ok(enabled) if !enabled.is_empty() => enabled,
            Ok(_) => KNOWN_EXCHANGES
                .iter()
                .map(|exchange| exchange.to_string())
                .collect(),
            Err(e) => {
                tracing::warn!("Failed to load enabled exchanges, falling back to known set: {e}");
                KNOWN_EXCHANGES
                    .iter()
                    .map(|exchange| exchange.to_string())
                    .collect()
            }
        };
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        discover_all_symbols_with_handle(&ticker, &exchanges).await
    }

    /// Discovers raw contract listings for a specific supported exchange.
    pub async fn discover_exchange_symbols_async(
        &self,
        exchange: &str,
    ) -> Result<Vec<RithmicInstrumentSymbol>> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        discover_exchange_symbols_with_handle(&ticker, exchange).await
    }

    /// Discovers raw contract listings for one supported root product.
    pub async fn discover_product_symbols_async(
        &self,
        product: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicInstrumentSymbol>> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        discover_product_symbols_with_handle(&ticker, exchange, product).await
    }

    /// Loads all supported instruments from all supported exchanges.
    pub async fn load_all_async(&self) -> Result<usize> {
        self.load_all_with_filter_async(false).await
    }

    /// Loads tradeable instruments from all enabled supported exchanges.
    pub async fn load_all_tradeable_async(&self) -> Result<usize> {
        self.load_all_with_filter_async(true).await
    }

    async fn load_all_with_filter_async(&self, tradeable_only: bool) -> Result<usize> {
        let exchanges = match self.enabled_exchanges_async().await {
            Ok(enabled) if !enabled.is_empty() => enabled,
            Ok(_) => KNOWN_EXCHANGES
                .iter()
                .map(|exchange| exchange.to_string())
                .collect(),
            Err(e) => {
                tracing::warn!("Failed to load enabled exchanges, falling back to known set: {e}");
                KNOWN_EXCHANGES
                    .iter()
                    .map(|exchange| exchange.to_string())
                    .collect()
            }
        };

        let mut total_loaded = 0;
        let mut last_error = None;

        for exchange in exchanges {
            let result = if tradeable_only {
                self.load_exchange_tradeable_async(&exchange).await
            } else {
                self.load_exchange_async(&exchange).await
            };

            match result {
                Ok(instruments) => {
                    tracing::debug!("Loaded {} instruments from {}", instruments.len(), exchange);
                    total_loaded += instruments.len();
                }
                Err(e) => {
                    tracing::warn!("Failed to load instruments from {}: {}", exchange, e);
                    last_error = Some(format!("{exchange}: {e}"));
                }
            }
        }

        if total_loaded == 0
            && let Some(e) = last_error
        {
            return Err(RithmicError::Instrument(format!(
                "Failed to load any supported Rithmic front months: {e}"
            )));
        }

        Ok(total_loaded)
    }

    /// Loads instruments for a specific exchange.
    pub async fn load_exchange_async(&self, exchange: &str) -> Result<Vec<InstrumentAny>> {
        self.load_exchange_with_filter_async(exchange, false).await
    }

    /// Loads tradeable instruments for a specific exchange.
    pub async fn load_exchange_tradeable_async(
        &self,
        exchange: &str,
    ) -> Result<Vec<InstrumentAny>> {
        self.load_exchange_with_filter_async(exchange, true).await
    }

    async fn load_exchange_with_filter_async(
        &self,
        exchange: &str,
        tradeable_only: bool,
    ) -> Result<Vec<InstrumentAny>> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;
        let instruments =
            load_supported_front_months_with_handle(&ticker, exchange, now_nanos(), tradeable_only)
                .await?;

        {
            let mut loaded = self.loaded_exchanges.write().await;

            if !loaded.contains(&exchange.to_string()) {
                loaded.push(exchange.to_string());
            }
        }

        Ok(instruments)
    }

    async fn enabled_exchanges_async(&self) -> Result<Vec<String>> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        let responses = ticker
            .list_exchanges(&self.gateway.config().username)
            .await
            .map_err(|e| RithmicError::Api(format!("Exchange permissions request failed: {e}")))?;

        Ok(enabled_exchange_names(&responses).into_iter().collect())
    }

    /// Loads a single instrument by symbol and exchange.
    pub async fn load_instrument_async(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<InstrumentAny> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        self.load_instrument_with_ticker_async(&ticker, symbol, exchange, true)
            .await
    }

    async fn load_instrument_with_ticker_async(
        &self,
        ticker: &RithmicTickerPlantHandle,
        symbol: &str,
        exchange: &str,
        cache_non_tradeable: bool,
    ) -> Result<InstrumentAny> {
        let key = cache_key(symbol, Some(exchange));

        if let Some(instrument) = self.instruments.get(&key) {
            return Ok(instrument.value().clone());
        }

        let instrument =
            fetch_instrument_with_handle(ticker, symbol, exchange, now_nanos()).await?;

        if cache_non_tradeable || instrument_is_tradeable(&instrument) {
            self.cache_instrument(instrument.clone());
        }

        Ok(instrument)
    }

    /// Loads the current front month contract for a product.
    pub async fn load_front_month(&self, product: &str, exchange: &str) -> Result<InstrumentAny> {
        let ticker = self
            .gateway
            .ticker_handle()
            .cloned()
            .ok_or(RithmicError::NotConnected)?;

        let instrument =
            load_front_month_instrument_with_handle(&ticker, product, exchange, now_nanos())
                .await?;

        self.cache_instrument(instrument.clone());
        Ok(instrument)
    }

    /// Caches multiple instruments.
    pub fn cache_instruments(&self, instruments: Vec<InstrumentAny>) {
        for instrument in instruments {
            self.cache_instrument(instrument);
        }
    }

    /// Caches a single instrument.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        let symbol = instrument.raw_symbol().to_string();
        let Some(exchange) = instrument.exchange().map(|exchange| exchange.to_string()) else {
            tracing::warn!("Ignoring Rithmic instrument without an exchange: {symbol}");
            return;
        };
        let key = cache_key(&symbol, Some(&exchange));

        self.instruments.insert(key, instrument.clone());
        let exchange_count = self
            .instruments
            .iter()
            .filter(|entry| {
                entry
                    .key()
                    .split_once(':')
                    .is_some_and(|(_, cached_symbol)| cached_symbol == symbol)
            })
            .count();
        if exchange_count == 1 {
            self.instruments.insert(symbol, instrument);
        } else {
            self.instruments.remove(&symbol);
        }
    }

    /// Gets an instrument by symbol.
    pub fn get_instrument(&self, symbol: &str) -> Option<InstrumentAny> {
        self.instruments
            .get(symbol)
            .map(|instrument| instrument.value().clone())
    }

    /// Returns a loaded instrument by symbol.
    pub fn get(&self, symbol: &str) -> Option<InstrumentAny> {
        self.get_instrument(symbol)
    }

    /// Returns a loaded instrument by symbol and exchange.
    pub fn get_by_exchange(&self, symbol: &str, exchange: &str) -> Option<InstrumentAny> {
        let key = cache_key(symbol, Some(exchange));
        self.instruments
            .get(&key)
            .map(|instrument| instrument.value().clone())
    }

    /// Returns all loaded instruments.
    pub fn instruments(&self) -> Vec<InstrumentAny> {
        self.instruments
            .iter()
            .filter(|entry| entry.key().contains(':'))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Returns all instruments for a specific exchange.
    pub fn instruments_for_exchange(&self, exchange: &str) -> Vec<InstrumentAny> {
        let prefix = format!("{exchange}:");
        self.instruments
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Returns the number of unique loaded instruments.
    pub fn count(&self) -> usize {
        self.instruments
            .iter()
            .filter(|entry| entry.key().contains(':'))
            .count()
    }

    /// Returns the list of loaded exchanges.
    pub async fn loaded_exchanges(&self) -> Vec<String> {
        self.loaded_exchanges.read().await.clone()
    }

    /// Clears all cached instruments.
    pub async fn clear(&self) {
        self.instruments.clear();
        self.loaded_exchanges.write().await.clear();
    }

    /// Adds an instrument to the cache.
    #[deprecated(since = "0.1.0", note = "Use cache_instrument instead")]
    pub fn add_instrument(&self, instrument: InstrumentAny) {
        self.cache_instrument(instrument);
    }
}

impl Debug for RithmicInstrumentProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicInstrumentProvider))
            .field("instrument_count", &self.instruments.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::AssetClass,
        identifiers::Symbol,
        instruments::{FuturesContract, InstrumentAny},
        types::{Currency, Price, Quantity},
    };
    use rithmic_rs::rti::ResponseFrontMonthContract;
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        config::RithmicEnv, gateway::GatewayConfig, instruments::front_month::front_month_contract,
    };

    fn test_gateway() -> Arc<RithmicGateway> {
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
        Arc::new(RithmicGateway::new(config))
    }

    fn test_instrument(symbol: &str, exchange: &str) -> InstrumentAny {
        InstrumentAny::FuturesContract(FuturesContract::new(
            crate::common::converters::rithmic_instrument_id(symbol, exchange).unwrap(),
            Symbol::new(symbol),
            AssetClass::Index,
            Some(Ustr::from(exchange)),
            Ustr::from("ES"),
            UnixNanos::from(1),
            UnixNanos::from(2),
            Currency::USD(),
            2,
            Price::new(0.25, 2),
            Quantity::from("50"),
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
            UnixNanos::from(3),
            UnixNanos::from(3),
        ))
    }

    #[rstest]
    fn test_instrument_provider_creation() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);
        assert_eq!(provider.count(), 0);
    }

    #[rstest]
    fn test_cache_instrument() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);

        provider.cache_instrument(test_instrument("ESZ4", "CME"));
        assert_eq!(provider.count(), 1);

        let retrieved = provider.get_instrument("ESZ4").unwrap();
        assert_eq!(retrieved.raw_symbol().as_str(), "ESZ4");
        assert_eq!(retrieved.price_increment(), Price::new(0.25, 2));
    }

    #[rstest]
    fn test_cache_instruments_batch() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);

        provider.cache_instruments(vec![
            test_instrument("ESZ4", "CME"),
            test_instrument("NQZ4", "CME"),
        ]);

        assert_eq!(provider.count(), 2);
        assert!(provider.get_instrument("ESZ4").is_some());
        assert!(provider.get_instrument("NQZ4").is_some());
    }

    #[rstest]
    fn test_get_by_exchange() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);
        provider.cache_instrument(test_instrument("ESZ4", "CME"));

        let retrieved = provider.get_by_exchange("ESZ4", "CME").unwrap();
        assert_eq!(retrieved.exchange(), Some(Ustr::from("CME")));
        assert!(provider.get_by_exchange("ESZ4", "CBOT").is_none());
    }

    #[rstest]
    fn test_bare_symbol_alias_is_removed_when_exchange_is_ambiguous() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);
        provider.cache_instrument(test_instrument("ESZ4", "CME"));
        assert!(provider.get("ESZ4").is_some());

        provider.cache_instrument(test_instrument("ESZ4", "CBOT"));

        assert!(provider.get("ESZ4").is_none());
        assert_eq!(
            provider
                .get_by_exchange("ESZ4", "CME")
                .and_then(|instrument| instrument.exchange()),
            Some(Ustr::from("CME"))
        );
        assert_eq!(
            provider
                .get_by_exchange("ESZ4", "CBOT")
                .and_then(|instrument| instrument.exchange()),
            Some(Ustr::from("CBOT"))
        );
        assert!(provider.get_by_exchange("ESZ4", "NYMEX").is_none());
    }

    #[rstest]
    fn test_instruments_for_exchange() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(gateway);
        provider.cache_instrument(test_instrument("ESZ4", "CME"));

        let cme_instruments = provider.instruments_for_exchange("CME");
        assert_eq!(cme_instruments.len(), 1);

        let nymex_instruments = provider.instruments_for_exchange("NYMEX");
        assert!(nymex_instruments.is_empty());
    }

    #[rstest]
    fn test_gateway_reference() {
        let gateway = test_gateway();
        let provider = RithmicInstrumentProvider::new(Arc::clone(&gateway));

        assert!(!provider.gateway().is_connected());
    }

    #[rstest]
    fn test_front_month_contract_prefers_trading_symbol_and_exchange() {
        let response = ResponseFrontMonthContract {
            template_id: 0,
            user_msg: Vec::new(),
            rp_code: Vec::new(),
            symbol: Some("MNQ".to_string()),
            exchange: Some("CME".to_string()),
            is_front_month_symbol: Some(true),
            symbol_name: None,
            trading_symbol: Some("MNQM26".to_string()),
            trading_exchange: Some("CME".to_string()),
        };

        let (symbol, exchange) =
            front_month_contract(&response).expect("front month contract should parse");

        assert_eq!(symbol, "MNQM26");
        assert_eq!(exchange, "CME");
    }
}
