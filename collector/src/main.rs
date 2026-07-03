use std::collections::{BTreeMap, BTreeSet};

use nautilus_model::identifiers::InstrumentId;
use serde::Deserialize;

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
        add_subscriptions(
            &mut plans,
            &self.book_deltas,
            |plan| plan.subscribe_book_deltas = true,
        );
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

fn instrument_ids(plan: &ExchangePlan) -> Vec<InstrumentId> {
    plan.instruments
        .iter()
        .map(|instrument| InstrumentId::from(instrument.as_str()))
        .collect()
}

fn main() {
    println!("collector implementation pending");
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
}
