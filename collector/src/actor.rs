use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use nautilus_binance::common::consts::BINANCE_CLIENT_ID;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
};
use nautilus_hyperliquid::common::consts::HYPERLIQUID_CLIENT_ID;
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId},
};

use crate::config::{Exchange, ExchangePlan};

#[derive(Debug, Clone)]
struct SubscriptionPlan {
    client_id: ClientId,
    instrument_ids: Vec<InstrumentId>,
    subscribe_book_deltas: bool,
    subscribe_quotes: bool,
    subscribe_trades: bool,
}

fn subscription_plans(
    plans: BTreeMap<Exchange, ExchangePlan>,
) -> anyhow::Result<Vec<SubscriptionPlan>> {
    plans
        .into_iter()
        .map(|(exchange, plan)| {
            let instrument_ids = plan
                .instruments
                .into_iter()
                .map(|instrument| {
                    instrument
                        .parse::<InstrumentId>()
                        .with_context(|| format!("invalid instrument id {instrument:?}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            Ok(SubscriptionPlan {
                client_id: client_id(exchange),
                instrument_ids,
                subscribe_book_deltas: plan.subscribe_book_deltas,
                subscribe_quotes: plan.subscribe_quotes,
                subscribe_trades: plan.subscribe_trades,
            })
        })
        .collect()
}

fn client_id(exchange: Exchange) -> ClientId {
    match exchange {
        Exchange::Binance => *BINANCE_CLIENT_ID,
        Exchange::Hyperliquid => *HYPERLIQUID_CLIENT_ID,
        Exchange::Lighter => ClientId::new("LIGHTER"),
    }
}

#[derive(Debug)]
pub(crate) struct CollectorActor {
    core: DataActorCore,
    subscriptions: Vec<SubscriptionPlan>,
}

nautilus_actor!(CollectorActor);

impl CollectorActor {
    pub(crate) fn from_exchange_plans(
        plans: BTreeMap<Exchange, ExchangePlan>,
    ) -> anyhow::Result<Self> {
        Ok(Self::new(subscription_plans(plans)?))
    }

    fn new(subscriptions: Vec<SubscriptionPlan>) -> Self {
        Self {
            core: DataActorCore::new(DataActorConfig::default()),
            subscriptions,
        }
    }
}

impl DataActor for CollectorActor {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for subscription in self.subscriptions.clone() {
            let client_id = Some(subscription.client_id);
            request_instruments_for_subscription(self, &subscription, client_id);

            for instrument_id in subscription.instrument_ids {
                if subscription.subscribe_book_deltas {
                    self.subscribe_book_deltas(
                        instrument_id,
                        BookType::L2_MBP,
                        None,
                        client_id,
                        true,
                        None,
                    );
                }

                if subscription.subscribe_quotes {
                    self.subscribe_quotes(instrument_id, client_id, None);
                }

                if subscription.subscribe_trades {
                    self.subscribe_trades(instrument_id, client_id, None);
                }
            }
        }

        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        for subscription in self.subscriptions.clone() {
            let client_id = Some(subscription.client_id);

            for instrument_id in subscription.instrument_ids {
                if subscription.subscribe_book_deltas {
                    self.unsubscribe_book_deltas(instrument_id, client_id, None);
                }

                if subscription.subscribe_quotes {
                    self.unsubscribe_quotes(instrument_id, client_id, None);
                }

                if subscription.subscribe_trades {
                    self.unsubscribe_trades(instrument_id, client_id, None);
                }
            }
        }

        Ok(())
    }
}

fn request_instruments_for_subscription(
    actor: &mut CollectorActor,
    subscription: &SubscriptionPlan,
    client_id: Option<ClientId>,
) {
    let venues = subscription
        .instrument_ids
        .iter()
        .map(|instrument_id| instrument_id.venue)
        .collect::<BTreeSet<_>>();

    for venue in venues {
        let _ = actor.request_instruments(Some(venue), None, None, client_id, None);
    }
}

#[cfg(test)]
fn subscription_plan_for_test(
    exchange: Exchange,
    plan: ExchangePlan,
) -> anyhow::Result<SubscriptionPlan> {
    subscription_plans(BTreeMap::from([(exchange, plan)]))?
        .into_iter()
        .next()
        .context("missing subscription plan")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_instrument_id_without_panicking() {
        let plan = ExchangePlan {
            instruments: BTreeSet::from(["BTCUSDT".to_string()]),
            subscribe_trades: true,
            ..Default::default()
        };

        let error = subscription_plan_for_test(Exchange::Binance, plan).unwrap_err();

        assert!(error.to_string().contains("BTCUSDT"));
    }
}
