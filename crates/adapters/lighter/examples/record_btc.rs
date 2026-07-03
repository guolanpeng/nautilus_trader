// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Records live Lighter BTC market data into a Nautilus streaming catalog.
//!
//! Run with:
//! `cargo run --example lighter-record-btc --package nautilus-lighter --features examples`
//!
//! Optional environment variables:
//! - `LIGHTER_RECORD_ENVIRONMENT=testnet|mainnet` (defaults to `mainnet`)
//! - `LIGHTER_RECORD_CATALOG_PATH=./catalog/lighter_btc_recording`

use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_core::datetime::NANOSECONDS_IN_MINUTE;
use nautilus_lighter::{
    common::enums::LighterEnvironment, config::LighterDataClientConfig,
    factories::LighterDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{ClientId, InstrumentId, TraderId};
use nautilus_system::config::{RotationConfig, StreamingConfig};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

const TRADER_ID: &str = "RECORDER-001";
const NODE_NAME: &str = "LIGHTER-BTC-RECORDER-001";
const CLIENT_ID: &str = "LIGHTER";
const INSTRUMENT_ID: &str = "BTC-PERP.LIGHTER";
const DEFAULT_CATALOG_PATH: &str = "./catalog/lighter_btc_recording";
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let trader_id = TraderId::from(TRADER_ID);
    let client_id = ClientId::new(CLIENT_ID);
    let instrument_id = InstrumentId::from(INSTRUMENT_ID);
    let catalog_path = std::env::var("LIGHTER_RECORD_CATALOG_PATH")
        .unwrap_or_else(|_| DEFAULT_CATALOG_PATH.into());
    let lighter_environment = lighter_environment_from_env();

    std::fs::create_dir_all(&catalog_path)?;

    let lighter_config = LighterDataClientConfig::builder()
        .environment(lighter_environment)
        .build();

    let streaming_config = StreamingConfig::builder()
        .catalog_path(catalog_path.clone())
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

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(NODE_NAME.to_string())
        .with_logging(log_config)
        .with_streaming_config(streaming_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(LighterDataClientFactory::new()),
            Box::new(lighter_config),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(vec![instrument_id])
        .request_instruments(true)
        .subscribe_book_deltas(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .manage_book(true)
        .book_levels_to_print(5)
        .stats_interval_secs(30)
        .build()?;

    node.add_actor(DataTester::new(tester_config))?;

    log::info!(
        "Recording {INSTRUMENT_ID} from {lighter_environment:?} to streaming catalog: {catalog_path}"
    );
    log::info!("Press Ctrl+C to stop recording and flush files");

    let handle = node.handle();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::error!("Failed to listen for Ctrl+C: {e}");
            return;
        }
        log::info!("Ctrl+C received, stopping recorder");
        handle.stop();
    });

    node.run().await?;

    Ok(())
}

fn lighter_environment_from_env() -> LighterEnvironment {
    match std::env::var("LIGHTER_RECORD_ENVIRONMENT")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    }
}
