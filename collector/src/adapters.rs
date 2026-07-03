use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::{BinanceDataClientConfig, BinanceSpotMarketDataMode},
    factories::BinanceDataClientFactory,
};
use nautilus_hyperliquid::{
    HyperliquidDataClientConfig, HyperliquidDataClientFactory,
    common::enums::HyperliquidEnvironment,
};
use nautilus_lighter::{
    common::enums::LighterEnvironment, config::LighterDataClientConfig,
    factories::LighterDataClientFactory,
};
use nautilus_live::node::LiveNodeBuilder;

use crate::config::Exchange;

pub(crate) fn add_data_client(
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
