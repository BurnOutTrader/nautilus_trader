#![allow(dead_code)]

use jiff::Timestamp;
use nautilus_common::actor::DataActor;
use nautilus_indicators::{
    average::ema::ExponentialMovingAverage,
    indicator::{Indicator, MovingAverage},
};
use nautilus_model::{
    data::{Bar, BarType},
    enums::{OrderSide, PositionSide},
    identifiers::{InstrumentId, StrategyId},
    instruments::{Instrument, InstrumentAny},
    types::Quantity,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

#[derive(Debug, Clone)]
pub(crate) struct RithmicBarEmaCrossConfig {
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub live_bar_type: BarType,
    pub history_bar_type: BarType,
    pub trade_size: Quantity,
    pub fast_period: usize,
    pub slow_period: usize,
    pub warmup_minutes: usize,
    pub request_bars_on_start: bool,
    pub unsubscribe_on_stop: bool,
    pub cleanup_on_stop: bool,
}

#[derive(Debug)]
pub(crate) struct RithmicBarEmaCrossStrategy {
    core: StrategyCore,
    config: RithmicBarEmaCrossConfig,
    fast_ema: ExponentialMovingAverage,
    slow_ema: ExponentialMovingAverage,
    prev_fast_above: Option<bool>,
    instrument_ready: bool,
    warmup_complete: bool,
    historical_bar_count: usize,
    live_bar_count: usize,
}

impl RithmicBarEmaCrossStrategy {
    pub(crate) fn new(config: RithmicBarEmaCrossConfig) -> Self {
        let base = StrategyConfig {
            strategy_id: Some(config.strategy_id),
            order_id_tag: Some("RUST".to_string()),
            ..Default::default()
        };

        let warmup_complete = !config.request_bars_on_start || config.warmup_minutes == 0;

        Self {
            core: StrategyCore::new(base),
            fast_ema: ExponentialMovingAverage::new(config.fast_period, None),
            slow_ema: ExponentialMovingAverage::new(config.slow_period, None),
            prev_fast_above: None,
            instrument_ready: false,
            warmup_complete,
            historical_bar_count: 0,
            live_bar_count: 0,
            config,
        }
    }

    fn request_warmup_history(&mut self) -> anyhow::Result<()> {
        if !self.config.request_bars_on_start || self.config.warmup_minutes == 0 {
            self.warmup_complete = true;
            return Ok(());
        }

        let end = Timestamp::now();
        let start = end
            .checked_sub(jiff::Span::new().minutes(self.config.warmup_minutes as i64))
            .unwrap_or(end);
        self.request_bars(
            self.config.history_bar_type,
            Some(start),
            Some(end),
            None,
            None,
            None,
        )?;
        println!(
            "Requesting Rithmic warmup bars for {} over the last {} minutes",
            self.config.history_bar_type, self.config.warmup_minutes,
        );
        Ok(())
    }

    fn handle_bar_sample(&mut self, bar: &Bar, allow_trading: bool) -> anyhow::Result<()> {
        self.fast_ema.handle_bar(bar);
        self.slow_ema.handle_bar(bar);

        if !self.fast_ema.initialized() || !self.slow_ema.initialized() {
            return Ok(());
        }

        let fast_above = self.fast_ema.value() > self.slow_ema.value();

        if allow_trading
            && self.instrument_ready
            && let Some(previous) = self.prev_fast_above
        {
            if fast_above && !previous {
                self.submit_signal(OrderSide::Buy)?;
            } else if !fast_above && previous {
                self.submit_signal(OrderSide::Sell)?;
            }
        }

        self.prev_fast_above = Some(fast_above);
        Ok(())
    }

    fn submit_signal(&mut self, side: OrderSide) -> anyhow::Result<()> {
        println!(
            "Rithmic EMA signal: side={side} instrument={} live_bars={} history_bars={}",
            self.config.instrument_id, self.live_bar_count, self.historical_bar_count,
        );

        let order = self.order().market(
            self.config.instrument_id,
            side,
            self.config.trade_size,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None)
    }

    fn refresh_instrument_ready(&mut self) {
        let cached = {
            let cache = self.cache();
            cache.instrument(&self.config.instrument_id)
        };

        if let Some(instrument) = cached {
            self.on_instrument(&instrument).ok();
        }
    }
}

nautilus_strategy!(RithmicBarEmaCrossStrategy, {
    fn external_order_claims(&self) -> Option<Vec<InstrumentId>> {
        Some(vec![self.config.instrument_id])
    }
});

impl DataActor for RithmicBarEmaCrossStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        <Self as Strategy>::on_start(self)?;
        self.refresh_instrument_ready();

        if !self.instrument_ready {
            let _ = self.request_instrument(self.config.instrument_id, None, None, None, None)?;
            self.subscribe_instrument(self.config.instrument_id, None, None);
        }

        self.subscribe_bars(self.config.live_bar_type, None, None);
        self.request_warmup_history()?;
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        if self.config.cleanup_on_stop {
            self.cancel_all_orders(self.config.instrument_id, None, None, None)?;
            self.close_all_positions(
                self.config.instrument_id,
                Option::<PositionSide>::None,
                None,
                None,
                None,
                None,
                None,
                None,
            )?;
        }

        if self.config.unsubscribe_on_stop {
            self.unsubscribe_bars(self.config.live_bar_type, None, None);
            self.unsubscribe_instrument(self.config.instrument_id, None, None);
        }

        println!(
            "Stopped Rithmic EMA strategy: live_bars={} history_bars={} warmup_complete={}",
            self.live_bar_count, self.historical_bar_count, self.warmup_complete,
        );
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        if instrument.id() == self.config.instrument_id && !self.instrument_ready {
            self.instrument_ready = true;
            println!("Resolved Rithmic instrument {}", instrument.id());
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        let matching = bars
            .iter()
            .filter(|bar| bar.bar_type == self.config.history_bar_type)
            .collect::<Vec<_>>();

        if matching.is_empty() {
            self.warmup_complete = true;
            return Ok(());
        }

        for bar in matching {
            self.historical_bar_count += 1;
            self.handle_bar_sample(bar, false)?;
        }

        self.warmup_complete = true;
        println!(
            "Rithmic warmup complete: loaded {} historical bars for {}",
            self.historical_bar_count, self.config.history_bar_type,
        );
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        if bar.bar_type != self.config.live_bar_type {
            return Ok(());
        }

        self.live_bar_count += 1;

        if !self.warmup_complete {
            return Ok(());
        }

        self.handle_bar_sample(bar, true)
    }
}
