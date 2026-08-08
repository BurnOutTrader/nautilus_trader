#![allow(dead_code)]

use std::{env, path::PathBuf, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::BarType,
    enums::{BarAggregation, PriceType},
    identifiers::{InstrumentId, TraderId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use rithmic_nt::{
    GatewayConfig, RithmicDataClientConfig, RithmicExecClientConfig, RithmicGateway,
    RithmicInstrumentProvider, TimeBarType,
};

pub(crate) const DEFAULT_PRODUCT_CODE: &str = "MNQ";
pub(crate) const DEFAULT_EXCHANGE: &str = "CME";

pub(crate) fn load_env() {
    dotenvy::dotenv().ok();
}

pub(crate) fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => true,
            "0" | "false" | "no" | "n" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

pub(crate) fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(crate) fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

pub(crate) fn profile_from_env() -> Option<String> {
    env::var("RITHMIC_PROFILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn example_root_from_env(adapter_name: &str) -> anyhow::Result<PathBuf> {
    let path = env::var("NAUTILUS_PATH").map_err(|_| {
        anyhow::anyhow!(
            "Set NAUTILUS_PATH to the parent directory for the {adapter_name} example catalog, for example /tmp/nautilus-data/examples/{adapter_name}.",
        )
    })?;
    Ok(PathBuf::from(path))
}

pub(crate) fn trader_id_from_env(key: &str, default: &str) -> TraderId {
    TraderId::from(env::var(key).unwrap_or_else(|_| default.to_string()))
}

pub(crate) fn data_config_from_env(
    profile: Option<&str>,
    enable_history: bool,
) -> anyhow::Result<RithmicDataClientConfig> {
    let mut config = RithmicDataClientConfig::from_env_with_profile(profile)?;
    config.enable_history = enable_history;
    Ok(config)
}

pub(crate) fn exec_config_from_env(
    profile: Option<&str>,
) -> anyhow::Result<RithmicExecClientConfig> {
    RithmicExecClientConfig::from_env_with_profile(profile).map_err(Into::into)
}

pub(crate) fn normalize_rithmic_bar_spec(bar_spec: &str) -> anyhow::Result<String> {
    let normalized = bar_spec.trim();
    anyhow::ensure!(!normalized.is_empty(), "Rithmic bar spec cannot be empty");
    anyhow::ensure!(
        !normalized.ends_with("-INTERNAL"),
        "Rithmic historical downloads require an external bar specification",
    );
    Ok(normalized.trim_end_matches("-EXTERNAL").to_string())
}

pub(crate) fn build_external_bar_type(
    instrument_id: InstrumentId,
    bar_spec: &str,
) -> anyhow::Result<BarType> {
    let normalized = normalize_rithmic_bar_spec(bar_spec)?;
    BarType::from_str(&format!("{instrument_id}-{normalized}-EXTERNAL")).map_err(Into::into)
}

pub(crate) fn parse_bar_type_request(bar_type: BarType) -> anyhow::Result<(TimeBarType, i32)> {
    anyhow::ensure!(
        bar_type.spec().price_type == PriceType::Last,
        "Rithmic examples expect LAST bars, received {}",
        bar_type.spec().price_type,
    );

    let period = i32::try_from(bar_type.spec().step.get())?;
    let time_bar_type = match bar_type.spec().aggregation {
        BarAggregation::Second => TimeBarType::SecondBar,
        BarAggregation::Minute => TimeBarType::MinuteBar,
        BarAggregation::Day => TimeBarType::DailyBar,
        BarAggregation::Week => TimeBarType::WeeklyBar,
        other => anyhow::bail!("Unsupported Rithmic bar aggregation: {other:?}"),
    };

    Ok((time_bar_type, period))
}

pub(crate) fn parse_rithmic_instrument_id(
    instrument_id: &InstrumentId,
) -> anyhow::Result<(String, String)> {
    let symbol_str = instrument_id.symbol.as_str();
    let mut parts = symbol_str.splitn(2, '.');
    let symbol = parts.next().unwrap_or(symbol_str).to_string();
    let exchange = parts.next().unwrap_or("").to_string();
    anyhow::ensure!(
        !exchange.is_empty(),
        "Rithmic examples expect an instrument ID with an exchange, received {instrument_id}",
    );
    Ok((symbol, exchange))
}

pub(crate) async fn connect_history_gateway(
    profile: Option<&str>,
) -> anyhow::Result<Arc<RithmicGateway>> {
    let mut config = GatewayConfig::from_env_with_profile(profile)?
        .with_order(false)
        .with_pnl(false)
        .with_history(true);
    config.enable_ticker = true;

    let mut gateway = RithmicGateway::new(config);
    gateway.connect().await?;
    Ok(Arc::new(gateway))
}

pub(crate) async fn disconnect_history_gateway(gateway: Arc<RithmicGateway>) -> anyhow::Result<()> {
    let mut gateway = Arc::try_unwrap(gateway)
        .map_err(|_| anyhow::anyhow!("Rithmic history gateway still has active references"))?;
    gateway.disconnect().await?;
    Ok(())
}

pub(crate) async fn resolve_instrument_from_env(
    provider: &RithmicInstrumentProvider,
) -> anyhow::Result<InstrumentAny> {
    if let Ok(raw_instrument_id) = env::var("RITHMIC_INSTRUMENT_ID") {
        let instrument_id = InstrumentId::from(raw_instrument_id);
        let (symbol, exchange) = parse_rithmic_instrument_id(&instrument_id)?;
        return provider
            .load_instrument_async(&symbol, &exchange)
            .await
            .map_err(Into::into);
    }

    let product_code = env_string("RITHMIC_PRODUCT_CODE", DEFAULT_PRODUCT_CODE);
    let exchange = env_string("RITHMIC_EXCHANGE", DEFAULT_EXCHANGE);
    provider
        .load_front_month(&product_code, &exchange)
        .await
        .map_err(Into::into)
}

pub(crate) fn resolve_catalog_instrument_id(
    catalog: &ParquetDataCatalog,
) -> anyhow::Result<InstrumentId> {
    if let Ok(instrument_id) = env::var("RITHMIC_INSTRUMENT_ID") {
        return Ok(InstrumentId::from(instrument_id));
    }

    let instruments = catalog.instruments(None, None, None)?;
    match instruments.as_slice() {
        [] => anyhow::bail!("The Rithmic catalog does not contain any instruments"),
        [instrument] => Ok(instrument.id()),
        _ => {
            if let (Ok(product_code), Ok(exchange)) = (
                env::var("RITHMIC_PRODUCT_CODE"),
                env::var("RITHMIC_EXCHANGE"),
            ) {
                let target_product = product_code.trim().to_ascii_uppercase();
                let target_exchange = exchange.trim().to_ascii_uppercase();
                let mut matches = instruments
                    .iter()
                    .filter(|instrument| {
                        instrument
                            .raw_symbol()
                            .to_string()
                            .starts_with(target_product.as_str())
                            && instrument
                                .exchange()
                                .is_some_and(|value| value.as_str() == target_exchange)
                    })
                    .map(|instrument| instrument.id())
                    .collect::<Vec<_>>();
                matches.dedup();

                if let [instrument_id] = matches.as_slice() {
                    return Ok(*instrument_id);
                }
            }

            anyhow::bail!(
                "Could not resolve a unique Rithmic instrument from the catalog. Set RITHMIC_INSTRUMENT_ID explicitly.",
            )
        }
    }
}

pub(crate) fn resolve_catalog_backtest_window(
    catalog: &mut ParquetDataCatalog,
    instrument_id: InstrumentId,
    bar_type: BarType,
) -> anyhow::Result<(UnixNanos, UnixNanos)> {
    let bars = catalog.bars(Some(vec![instrument_id.to_string()]), None, None)?;
    let mut matching = bars
        .into_iter()
        .filter(|bar| bar.bar_type == bar_type)
        .collect::<Vec<_>>();
    matching.sort_by_key(|bar| bar.ts_event);

    let first = matching
        .first()
        .ok_or_else(|| anyhow::anyhow!("No bars found in the catalog for {bar_type}"))?;
    let last = matching
        .last()
        .ok_or_else(|| anyhow::anyhow!("No bars found in the catalog for {bar_type}"))?;

    Ok((first.ts_event, last.ts_event))
}

pub(crate) fn format_utc_nanos(value: UnixNanos) -> anyhow::Result<String> {
    let nanos = i64::try_from(value.as_u64())?;
    let timestamp = DateTime::<Utc>::from_timestamp_nanos(nanos);
    Ok(timestamp.to_rfc3339())
}
