#![allow(dead_code)]

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, bar::get_bar_interval_ns},
    enums::{BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::Instrument,
    types::{Price, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use projectx_client::{Bar as PxApiBar, BarUnit, Contract, ContractId, HistoryRequest, Timestamp};
use projectx_nt::{
    ProjectXHttpClient, factories::projectx_contract_to_instrument, http::error::ProjectXHttpError,
};
use rust_decimal::prelude::ToPrimitive;

use super::common::projectx_transport_config_from_env;

#[derive(Debug, Clone)]
pub(crate) struct ProjectXCatalogDownloadResult {
    pub catalog_path: PathBuf,
    pub instrument_id: InstrumentId,
    pub bar_type: BarType,
    pub start_time: String,
    pub end_time: String,
    pub instrument_count: usize,
    pub bar_count: usize,
}

pub(crate) fn build_external_bar_type(
    instrument_id: InstrumentId,
    bar_spec: &str,
) -> anyhow::Result<BarType> {
    let normalized = bar_spec.trim();
    anyhow::ensure!(!normalized.is_empty(), "ProjectX bar spec cannot be empty");
    anyhow::ensure!(
        !normalized.ends_with("-INTERNAL"),
        "ProjectX historical downloads require an external bar specification",
    );
    let normalized = normalized.trim_end_matches("-EXTERNAL");
    BarType::from_str(&format!("{instrument_id}-{normalized}-EXTERNAL")).map_err(Into::into)
}

fn infer_price_precision(value: f64) -> u8 {
    let formatted = format!("{value:.12}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split('.')
        .nth(1)
        .map_or(0, |fraction| fraction.len() as u8)
}

fn infer_bar_price_precision(open: f64, high: f64, low: f64, close: f64) -> u8 {
    [
        infer_price_precision(open),
        infer_price_precision(high),
        infer_price_precision(low),
        infer_price_precision(close),
    ]
    .into_iter()
    .max()
    .unwrap_or(0)
}

fn parse_ts(value: &str) -> Option<UnixNanos> {
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    let nanos = parsed.timestamp_nanos_opt()?;
    let nanos_u64 = u64::try_from(nanos).ok()?;
    Some(UnixNanos::from(nanos_u64))
}

fn map_historical_bars(bar_type: BarType, bars: Vec<PxApiBar>) -> Vec<Bar> {
    let mut mapped = bars
        .into_iter()
        .filter_map(|bar| {
            let ts_event = u64::try_from(bar.t.as_jiff().as_nanosecond())
                .ok()
                .map(UnixNanos::from)?;
            let o = bar.o.to_f64().unwrap_or_default();
            let h = bar.h.to_f64().unwrap_or_default();
            let l = bar.l.to_f64().unwrap_or_default();
            let c = bar.c.to_f64().unwrap_or_default();
            let price_prec = infer_bar_price_precision(o, h, l, c);
            Some(Bar::new(
                bar_type,
                Price::new(o, price_prec),
                Price::new(h, price_prec),
                Price::new(l, price_prec),
                Price::new(c, price_prec),
                Quantity::new(bar.v as f64, 0),
                ts_event,
                ts_event,
            ))
        })
        .collect::<Vec<_>>();
    mapped.sort_by_key(|bar| bar.ts_event);
    mapped.dedup_by_key(|bar| bar.ts_event);
    mapped
}

fn filter_completed_historical_bars(
    bar_type: BarType,
    mut bars: Vec<Bar>,
    end_nanos: UnixNanos,
) -> Vec<Bar> {
    let interval_ns = get_bar_interval_ns(&bar_type).as_u64();
    bars.retain(|bar| {
        bar.ts_event
            .checked_add(interval_ns)
            .is_some_and(|bar_close| bar_close <= end_nanos)
    });
    bars
}

fn next_historical_page_start(
    bar_type: BarType,
    bars: &[Bar],
    current_start_nanos: UnixNanos,
    end_nanos: UnixNanos,
) -> Option<UnixNanos> {
    let last_bar = bars.last()?;
    let next_start = last_bar
        .ts_event
        .checked_add(get_bar_interval_ns(&bar_type).as_u64())?;

    if next_start <= current_start_nanos || next_start >= end_nanos {
        return None;
    }

    Some(next_start)
}

fn to_client_bar_unit(unit: i32) -> anyhow::Result<BarUnit> {
    match unit {
        1 => Ok(BarUnit::Second),
        2 => Ok(BarUnit::Minute),
        3 => Ok(BarUnit::Hour),
        4 => Ok(BarUnit::Day),
        5 => Ok(BarUnit::Week),
        6 => Ok(BarUnit::Month),
        other => anyhow::bail!("Unsupported ProjectX bar aggregation: {other:?}"),
    }
}

fn map_request(
    contract_id: String,
    bar_type: BarType,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: NonZeroUsize,
    market_data_live: bool,
) -> anyhow::Result<HistoryRequest> {
    let spec = bar_type.spec();
    let step = i32::try_from(spec.step.get()).map_err(|_| anyhow::anyhow!("Invalid bar step"))?;
    let unit = match spec.aggregation {
        BarAggregation::Second => 1,
        BarAggregation::Minute => 2,
        BarAggregation::Hour => 3,
        BarAggregation::Day => 4,
        BarAggregation::Week => 5,
        BarAggregation::Month => 6,
        other => anyhow::bail!("Unsupported ProjectX bar aggregation: {other:?}"),
    };
    anyhow::ensure!(
        spec.price_type == PriceType::Last,
        "ProjectX examples expect LAST bars, received {}",
        spec.price_type,
    );

    Ok(HistoryRequest::builder(
        ContractId::new(contract_id)?,
        market_data_live,
        Timestamp::new(&start.to_rfc3339())?,
        Timestamp::new(&end.to_rfc3339())?,
        to_client_bar_unit(unit)?,
    )
    .unit_number(step)
    .limit(limit.get() as i32)
    .include_partial_bar(false)
    .build()?)
}

async fn retrieve_bars_page(
    http: &ProjectXHttpClient,
    request: &HistoryRequest,
    symbol: &str,
    allow_live_history_fallback: bool,
) -> Result<Vec<PxApiBar>, ProjectXHttpError> {
    match http.retrieve_bars(request).await {
        Err(error) if provider_error_code(&error) == Some(1) && allow_live_history_fallback => {
            println!(
                "ProjectX live historical request rejected for {symbol}; retrying with live=false because explicit fallback is enabled",
            );
            let fallback = HistoryRequest::builder(
                request.contract_id().clone(),
                false,
                request.start_time(),
                request.end_time(),
                request.unit(),
            )
            .unit_number(request.unit_number())
            .limit(request.limit())
            .include_partial_bar(false)
            .build()
            .map_err(anyhow::Error::from)?;
            http.retrieve_bars(&fallback).await
        }
        other => other,
    }
}

fn provider_error_code(error: &ProjectXHttpError) -> Option<i32> {
    match error {
        ProjectXHttpError::Client(projectx_client::Error::Provider(provider_error)) => {
            Some(provider_error.code)
        }
        _ => None,
    }
}

fn unix_nanos_to_rfc3339(value: UnixNanos) -> anyhow::Result<String> {
    let nanos = i64::try_from(value.as_u64())?;
    Ok(DateTime::<Utc>::from_timestamp_nanos(nanos).to_rfc3339())
}

#[allow(
    clippy::too_many_arguments,
    reason = "example helper keeps a flat call signature for script readability"
)]
pub(crate) async fn download_bars_to_catalog(
    catalog_path: &Path,
    contract: Contract,
    bar_spec: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: NonZeroUsize,
    market_data_live: bool,
    allow_live_history_fallback: bool,
) -> anyhow::Result<ProjectXCatalogDownloadResult> {
    let instrument = projectx_contract_to_instrument(&contract)?;
    let instrument_id = instrument.id();
    let bar_type = build_external_bar_type(instrument_id, bar_spec)?;
    let symbol = instrument_id.symbol.to_string();
    let end_nanos = UnixNanos::from(u64::try_from(
        end.timestamp_nanos_opt().unwrap_or_default(),
    )?);
    let mut current_start = UnixNanos::from(u64::try_from(
        start.timestamp_nanos_opt().unwrap_or_default(),
    )?);

    std::fs::create_dir_all(catalog_path)?;

    let http = ProjectXHttpClient::from_config(projectx_transport_config_from_env()?)?;
    http.start().await?;

    let mut request = map_request(
        contract.id.to_string(),
        bar_type,
        start,
        end,
        limit,
        market_data_live,
    )?;

    let mut bars = Vec::new();
    let mut page_count = 0usize;

    loop {
        page_count += 1;
        let response =
            retrieve_bars_page(&http, &request, &symbol, allow_live_history_fallback).await?;

        // Raw historical requests can now include the current open bar. Catalog download helpers
        // keep the safer closed-bar behavior by filtering the trailing in-progress bar before
        // writing.
        let page_bars = filter_completed_historical_bars(
            bar_type,
            map_historical_bars(bar_type, response),
            end_nanos,
        );

        if page_bars.is_empty() {
            break;
        }

        let next_start = next_historical_page_start(bar_type, &page_bars, current_start, end_nanos);
        bars.extend(page_bars);
        bars.sort_by_key(|bar| bar.ts_event);
        bars.dedup_by_key(|bar| bar.ts_event);

        let Some(next_start) = next_start else {
            break;
        };

        current_start = next_start;
        request = HistoryRequest::builder(
            request.contract_id().clone(),
            request.is_live(),
            Timestamp::new(&unix_nanos_to_rfc3339(next_start)?)?,
            request.end_time(),
            request.unit(),
        )
        .unit_number(request.unit_number())
        .limit(request.limit())
        .include_partial_bar(false)
        .build()?;
    }

    println!(
        "Downloaded {} ProjectX pages and {} bars",
        page_count,
        bars.len()
    );

    let catalog = ParquetDataCatalog::new(catalog_path, None, None, None, None);
    catalog.write_instruments(vec![instrument])?;
    catalog.write_to_parquet(&bars, None, None, None)?;

    let mut catalog = ParquetDataCatalog::new(catalog_path, None, None, None, None);
    let stored_instruments = catalog.instruments(Some(&[instrument_id.to_string()]), None, None)?;
    let stored_bars = catalog
        .bars(Some(vec![instrument_id.to_string()]), None, None)?
        .into_iter()
        .filter(|bar| bar.bar_type == bar_type)
        .count();

    Ok(ProjectXCatalogDownloadResult {
        catalog_path: catalog_path.to_path_buf(),
        instrument_id,
        bar_type,
        start_time: start.to_rfc3339(),
        end_time: end.to_rfc3339(),
        instrument_count: stored_instruments.len(),
        bar_count: stored_bars,
    })
}
