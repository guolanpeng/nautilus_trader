use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use anyhow::Context;
use log::LevelFilter;
use nautilus_binance::{
    common::{
        consts::BINANCE_CLIENT_ID,
        enums::{BinanceEnvironment, BinanceProductType},
    },
    config::{BinanceDataClientConfig, BinanceSpotMarketDataMode},
    factories::BinanceDataClientFactory,
};
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_core::datetime::NANOSECONDS_IN_MINUTE;
use nautilus_hyperliquid::{
    HyperliquidDataClientConfig, HyperliquidDataClientFactory,
    common::{consts::HYPERLIQUID_CLIENT_ID, enums::HyperliquidEnvironment},
};
use nautilus_lighter::{
    common::enums::LighterEnvironment, config::LighterDataClientConfig,
    factories::LighterDataClientFactory,
};
use nautilus_live::node::{LiveNode, LiveNodeBuilder};
use nautilus_model::identifiers::{ClientId, InstrumentId, TraderId};
use nautilus_system::config::{RotationConfig, StreamingConfig};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "collector.yaml";
const DEFAULT_CATALOG_PATH: &str = "./catalog/collector";
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500;
const NODE_NAME: &str = "DATA-COLLECTOR-001";
const TRADER_ID: &str = "COLLECTOR-001";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Exchange {
    Binance,
    Hyperliquid,
    Lighter,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ExchangePlan {
    instruments: BTreeSet<String>,
    subscribe_book_deltas: bool,
    subscribe_quotes: bool,
    subscribe_trades: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectorConfig {
    #[serde(default)]
    book_deltas: BTreeMap<ExchangeKey, Vec<String>>,
    #[serde(default)]
    quotes: BTreeMap<ExchangeKey, Vec<String>>,
    #[serde(default)]
    trades: BTreeMap<ExchangeKey, Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExchangeKey {
    Binance,
    Hyperliquid,
    Lighter,
}

impl From<ExchangeKey> for Exchange {
    fn from(value: ExchangeKey) -> Self {
        match value {
            ExchangeKey::Binance => Self::Binance,
            ExchangeKey::Hyperliquid => Self::Hyperliquid,
            ExchangeKey::Lighter => Self::Lighter,
        }
    }
}

impl CollectorConfig {
    fn from_yaml_str(input: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(input)
    }

    fn exchange_plans(&self) -> anyhow::Result<BTreeMap<Exchange, ExchangePlan>> {
        let mut plans = BTreeMap::new();
        add_subscriptions(&mut plans, &self.book_deltas, |plan| {
            plan.subscribe_book_deltas = true
        });
        add_subscriptions(&mut plans, &self.quotes, |plan| {
            plan.subscribe_quotes = true;
        });
        add_subscriptions(&mut plans, &self.trades, |plan| {
            plan.subscribe_trades = true;
        });

        if plans.is_empty() {
            anyhow::bail!("no subscriptions configured");
        }

        for (exchange, plan) in &plans {
            if plan.instruments.is_empty() {
                anyhow::bail!("{exchange:?} has no instruments configured");
            }
        }

        Ok(plans)
    }
}

fn add_subscriptions<F>(
    plans: &mut BTreeMap<Exchange, ExchangePlan>,
    subscriptions: &BTreeMap<ExchangeKey, Vec<String>>,
    mut enable: F,
) where
    F: FnMut(&mut ExchangePlan),
{
    for (exchange_key, instruments) in subscriptions {
        let plan = plans.entry((*exchange_key).into()).or_default();
        enable(plan);
        plan.instruments.extend(instruments.iter().cloned());
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let config_yaml = std::fs::read_to_string(&config_path).with_context(|| {
        format!(
            "failed to read collector config from {}",
            config_path.display()
        )
    })?;
    let config = CollectorConfig::from_yaml_str(&config_yaml).with_context(|| {
        format!(
            "failed to parse collector config from {}",
            config_path.display()
        )
    })?;
    let plans = config.exchange_plans()?;

    let catalog_path = std::env::var("COLLECTOR_CATALOG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_CATALOG_PATH));
    std::fs::create_dir_all(&catalog_path).with_context(|| {
        format!(
            "failed to create collector catalog directory {}",
            catalog_path.display()
        )
    })?;

    let streaming_config = StreamingConfig::builder()
        .catalog_path(catalog_path.to_string_lossy().into_owned())
        .fs_protocol("file".to_string())
        .flush_interval_ms(DEFAULT_FLUSH_INTERVAL_MS)
        .replace_existing(false)
        .rotation_config(RotationConfig::Interval {
            interval_ns: NANOSECONDS_IN_MINUTE,
        })
        .build()?;

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };

    let mut builder = LiveNode::builder(TraderId::from(TRADER_ID), Environment::Live)?
        .with_name(NODE_NAME.to_string())
        .with_logging(log_config)
        .with_streaming_config(streaming_config)
        .with_delay_post_stop_secs(2);

    for exchange in plans.keys() {
        builder = add_data_client(builder, *exchange)?;
    }

    let mut node = builder.build()?;
    for (exchange, plan) in plans {
        node.add_actor(DataTester::new(data_tester_config(exchange, plan)?))?;
    }

    log::info!(
        "Collecting market data from configured exchanges into streaming catalog: {}",
        catalog_path.display()
    );
    log::info!("Press Ctrl+C to stop collection and flush files");

    let handle = node.handle();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::error!("Failed to listen for Ctrl+C: {e}");
            return;
        }
        log::info!("Ctrl+C received, stopping collector");
        handle.stop();
    });

    node.run().await?;

    Ok(())
}

fn add_data_client(
    builder: LiveNodeBuilder,
    exchange: Exchange,
) -> anyhow::Result<LiveNodeBuilder> {
    match exchange {
        Exchange::Binance => builder.add_data_client(
            None,
            Box::new(BinanceDataClientFactory::new()),
            Box::new(BinanceDataClientConfig {
                product_type: BinanceProductType::UsdM,
                environment: BinanceEnvironment::Live,
                spot_market_data_mode: BinanceSpotMarketDataMode::Json,
                api_key: None,
                api_secret: None,
                ..Default::default()
            }),
        ),
        Exchange::Hyperliquid => builder.add_data_client(
            None,
            Box::new(HyperliquidDataClientFactory::new()),
            Box::new(HyperliquidDataClientConfig {
                environment: HyperliquidEnvironment::Mainnet,
                ..Default::default()
            }),
        ),
        Exchange::Lighter => builder.add_data_client(
            None,
            Box::new(LighterDataClientFactory::new()),
            Box::new(
                LighterDataClientConfig::builder()
                    .environment(LighterEnvironment::Mainnet)
                    .build(),
            ),
        ),
    }
}

fn data_tester_config(exchange: Exchange, plan: ExchangePlan) -> anyhow::Result<DataTesterConfig> {
    let client_id = match exchange {
        Exchange::Binance => *BINANCE_CLIENT_ID,
        Exchange::Hyperliquid => *HYPERLIQUID_CLIENT_ID,
        Exchange::Lighter => ClientId::new("LIGHTER"),
    };
    let instrument_ids = plan
        .instruments
        .into_iter()
        .map(|instrument| {
            instrument
                .parse::<InstrumentId>()
                .with_context(|| format!("invalid instrument id {instrument:?}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(instrument_ids)
        .request_instruments(true)
        .subscribe_book_deltas(plan.subscribe_book_deltas)
        .subscribe_quotes(plan.subscribe_quotes)
        .subscribe_trades(plan.subscribe_trades)
        .manage_book(true)
        .book_levels_to_print(5)
        .stats_interval_secs(30)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_subscription_roots() {
        let config = CollectorConfig::from_yaml_str(
            r#"
book_deltas:
  binance:
    - BTCUSDT-PERP.BINANCE
quotes:
  hyperliquid:
    - BTC-USD-PERP.HYPERLIQUID
trades:
  lighter:
    - BTC-PERP.LIGHTER
"#,
        )
        .unwrap();

        let plans = config.exchange_plans().unwrap();

        assert!(plans[&Exchange::Binance].subscribe_book_deltas);
        assert!(plans[&Exchange::Hyperliquid].subscribe_quotes);
        assert!(plans[&Exchange::Lighter].subscribe_trades);
    }

    #[test]
    fn rejects_unknown_data_type() {
        let error = CollectorConfig::from_yaml_str(
            r#"
details:
  lighter:
    - BTC-PERP.LIGHTER
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field `details`"));
    }

    #[test]
    fn rejects_unknown_exchange() {
        let error = CollectorConfig::from_yaml_str(
            r#"
book_deltas:
  okx:
    - BTC-USDT.OKX
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown variant `okx`"));
    }

    #[test]
    fn merges_data_types_for_same_exchange() {
        let config = CollectorConfig::from_yaml_str(
            r#"
book_deltas:
  lighter:
    - BTC-PERP.LIGHTER
quotes:
  lighter:
    - ETH-PERP.LIGHTER
trades:
  lighter:
    - BTC-PERP.LIGHTER
"#,
        )
        .unwrap();

        let plans = config.exchange_plans().unwrap();
        let lighter = &plans[&Exchange::Lighter];

        assert!(lighter.subscribe_book_deltas);
        assert!(lighter.subscribe_quotes);
        assert!(lighter.subscribe_trades);
        assert_eq!(
            lighter.instruments,
            BTreeSet::from([
                "BTC-PERP.LIGHTER".to_string(),
                "ETH-PERP.LIGHTER".to_string()
            ])
        );
    }

    #[test]
    fn rejects_invalid_instrument_id_without_panicking() {
        let plan = ExchangePlan {
            instruments: BTreeSet::from(["BTCUSDT".to_string()]),
            subscribe_trades: true,
            ..Default::default()
        };

        let error = data_tester_config(Exchange::Binance, plan).unwrap_err();

        assert!(error.to_string().contains("BTCUSDT"));
    }
}
