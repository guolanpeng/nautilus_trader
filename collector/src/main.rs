use std::path::PathBuf;

use actor::CollectorActor;
use adapters::add_data_client;
use anyhow::Context;
use config::CollectorConfig;
use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_DAY};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::TraderId;
use nautilus_system::config::{RotationConfig, StreamingConfig};

mod actor;
mod adapters;
mod config;

const DEFAULT_CONFIG_PATH: &str = "collector.yaml";
const DEFAULT_CATALOG_PATH: &str = "./catalog/collector";
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1000;
const TRADER_ID: &str = "COLLECTOR-001";

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
        .rotation_config(RotationConfig::ScheduledDates {
            interval_ns: NANOSECONDS_IN_DAY,
            schedule_ns: UnixNanos::new(0),
        })
        .build()?;

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };

    let mut builder = LiveNode::builder(TraderId::from(TRADER_ID), Environment::Live)?
        .with_logging(log_config)
        .with_streaming_config(streaming_config);

    for exchange in plans.keys() {
        builder = add_data_client(builder, *exchange)?;
    }

    let mut node = builder.build()?;
    node.add_actor(CollectorActor::from_exchange_plans(plans)?)?;

    log::info!(
        "Collecting market data from configured exchanges into streaming catalog: {}",
        catalog_path.display()
    );

    node.run().await?;

    Ok(())
}
