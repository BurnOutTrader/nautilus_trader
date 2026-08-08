#![allow(dead_code)]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, bar::get_bar_interval_ns},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use rithmic_nt::RithmicGateway;
use rithmic_rs::rti::{ResponseTimeBarReplay, TimeBar, messages::RithmicMessage};

use crate::support::common::{parse_bar_type_request, parse_rithmic_instrument_id};

#[derive(Debug, Clone)]
pub(crate) struct RithmicCatalogDownloadResult {
    pub catalog_path: PathBuf,
    pub instrument_id: InstrumentId,
    pub bar_type: BarType,
    pub start_time: String,
    pub end_time: String,
    pub instrument_count: usize,
    pub bar_count: usize,
}

fn time_bar_marker_to_unix_nanos(marker: Option<i32>) -> Option<UnixNanos> {
    marker.and_then(|value| {
        if value > 0 {
            Some(UnixNanos::from(value as u64 * 1_000_000_000))
        } else {
            None
        }
    })
}

fn historical_response_to_bar(
    replay: &ResponseTimeBarReplay,
    bar_type: BarType,
    price_prec: u8,
    size_prec: u8,
    ts_init: UnixNanos,
) -> Option<Bar> {
    let ts_event = time_bar_marker_to_unix_nanos(replay.marker)?;

    Some(Bar::new(
        bar_type,
        Price::new(replay.open_price.unwrap_or(0.0), price_prec),
        Price::new(replay.high_price.unwrap_or(0.0), price_prec),
        Price::new(replay.low_price.unwrap_or(0.0), price_prec),
        Price::new(replay.close_price.unwrap_or(0.0), price_prec),
        Quantity::new(replay.volume.unwrap_or(0) as f64, size_prec),
        ts_event,
        ts_init,
    ))
}

fn live_time_bar_to_bar(
    live: &TimeBar,
    bar_type: BarType,
    price_prec: u8,
    size_prec: u8,
    ts_init: UnixNanos,
) -> Option<Bar> {
    let ts_event = time_bar_marker_to_unix_nanos(live.marker)?;

    Some(Bar::new(
        bar_type,
        Price::new(live.open_price.unwrap_or(0.0), price_prec),
        Price::new(live.high_price.unwrap_or(0.0), price_prec),
        Price::new(live.low_price.unwrap_or(0.0), price_prec),
        Price::new(live.close_price.unwrap_or(0.0), price_prec),
        Quantity::new(live.volume.unwrap_or(0) as f64, size_prec),
        ts_event,
        ts_init,
    ))
}

fn filter_closed_time_history_bars(mut bars: Vec<Bar>, request_end_nanos: UnixNanos) -> Vec<Bar> {
    bars.sort_by_key(|bar| bar.ts_event);
    bars.dedup_by_key(|bar| bar.ts_event);
    bars.retain(|bar| {
        bar.ts_event
            .checked_add(get_bar_interval_ns(&bar.bar_type).as_u64())
            .is_some_and(|bar_close| bar_close <= request_end_nanos)
    });
    bars
}

pub(crate) async fn download_bars_to_catalog(
    gateway: &Arc<RithmicGateway>,
    catalog_path: &Path,
    instrument: &InstrumentAny,
    bar_spec: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<RithmicCatalogDownloadResult> {
    let instrument_id = instrument.id();
    let bar_type = crate::support::common::build_external_bar_type(instrument_id, bar_spec)?;
    let (symbol, exchange) = parse_rithmic_instrument_id(&instrument_id)?;
    let (time_bar_type, period) = parse_bar_type_request(bar_type)?;
    let start_sec = i32::try_from(start.timestamp())?;
    let end_sec = i32::try_from(end.timestamp())?;
    let ts_init = UnixNanos::from(u64::try_from(
        Utc::now().timestamp_nanos_opt().unwrap_or_default(),
    )?);
    let price_prec = instrument.price_precision();
    let size_prec = instrument.size_precision();

    std::fs::create_dir_all(catalog_path)?;

    let responses = gateway
        .request_bars(
            &symbol,
            &exchange,
            time_bar_type,
            period,
            start_sec,
            end_sec,
        )
        .await?;

    let bars = responses
        .iter()
        .filter_map(|response| match &response.message {
            RithmicMessage::ResponseTimeBarReplay(replay) => {
                historical_response_to_bar(replay, bar_type, price_prec, size_prec, ts_init)
            }
            RithmicMessage::TimeBar(live) => {
                live_time_bar_to_bar(live, bar_type, price_prec, size_prec, ts_init)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let request_end_nanos = UnixNanos::from(u64::try_from(
        end.timestamp_nanos_opt().unwrap_or_default(),
    )?);
    // Raw historical requests can now include the current open bar. Catalog download helpers keep
    // the safer closed-bar behavior by filtering the trailing in-progress bar before writing.
    let bars = filter_closed_time_history_bars(bars, request_end_nanos);

    let catalog = ParquetDataCatalog::new(catalog_path, None, None, None, None);
    catalog.write_instruments(vec![instrument.clone()])?;
    catalog.write_to_parquet(&bars, None, None, None)?;

    let mut catalog = ParquetDataCatalog::new(catalog_path, None, None, None, None);
    let stored_instruments = catalog.instruments(Some(&[instrument_id.to_string()]), None, None)?;
    let stored_bars = catalog
        .bars(Some(vec![instrument_id.to_string()]), None, None)?
        .into_iter()
        .filter(|bar| bar.bar_type == bar_type)
        .count();

    Ok(RithmicCatalogDownloadResult {
        catalog_path: catalog_path.to_path_buf(),
        instrument_id,
        bar_type,
        start_time: start.to_rfc3339(),
        end_time: end.to_rfc3339(),
        instrument_count: stored_instruments.len(),
        bar_count: stored_bars,
    })
}
