# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from decimal import Decimal

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import (
    Bar,
    BarType,
)
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.trading.strategy import Strategy

# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class EMALongOnlyConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``EMALongOnly`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the strategy.
    bar_type : BarType
        The bar type for the strategy.
    ema_period : int, default 20
        The EMA period.
    max_trade_allocation : Decimal, default Decimal("0.995")
        The fraction of free settlement balance to use when entering a position.
    request_historical_bars : bool, default True
        If historical bars should be requested on start.
    close_positions_on_stop : bool, default True
        If all open positions should be closed on strategy stop.

    """

    instrument_id: InstrumentId
    bar_type: BarType
    ema_period: PositiveInt = 20
    max_trade_allocation: Decimal = Decimal("0.995")
    close_positions_on_stop: bool = True


class EMALongOnly(Strategy):
    """
    A simple single EMA LONG ONLY example strategy.

    This strategy is suitable for trading equities on a CASH account.

    When the bar close is above the EMA, enter a LONG position.
    When the bar close falls below the EMA, flatten any existing LONG position.

    Parameters
    ----------
    config : EMALongOnlyConfig
        The configuration for the instance.

    """

    def __init__(self, config: EMALongOnlyConfig) -> None:
        super().__init__(config)

        self.instrument: Instrument = None  # Initialized in on_start
        self.last_close_price: Decimal | None = None

        # Create the indicator for the strategy
        self.ema = ExponentialMovingAverage(config.ema_period)

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        # Register the indicator for updating
        self.register_indicator_for_bars(self.config.bar_type, self.ema)

        # Subscribe to live data
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when the strategy is running and receives a bar.

        Parameters
        ----------
        bar : Bar
            The bar received.

        """
        self.log.info(repr(bar), LogColor.CYAN)

        # Check if indicators ready
        if not self.indicators_initialized():
            self.log.info(
                f"Waiting for indicators to warm up [{self.cache.bar_count(self.config.bar_type)}]",
                color=LogColor.BLUE,
            )
            return  # Wait for indicators to warm up...

        if bar.is_single_price():
            # Implies no market information for this bar
            return

        close = float(bar.close)
        ema_value = float(self.ema.value)
        self.last_close_price = Decimal(str(close))
        # signal = close - ema_value
        # print(f"Close: {close:.2f}, EMA: {ema_value:.2f}, Signal: {signal:.2f}")
        # BUY LOGIC
        if close >= ema_value:
            if self.portfolio.is_flat(self.config.instrument_id):
                self.buy()
        # SELL LOGIC
        elif close < ema_value and self.portfolio.is_net_long(
            self.config.instrument_id,
        ):
            self.log.info(
                f"Closing long position because close {close:.2f} < EMA {ema_value:.2f}",
                color=LogColor.YELLOW,
            )
            self.close_all_positions(self.config.instrument_id)

    def buy(self) -> None:
        """
        Buy using (almost) all available settlement currency.
        """
        if self.last_close_price is None or self.last_close_price <= 0:
            self.log.warning("Skipping buy: no valid reference price available.")
            return

        account = self.portfolio.account(venue=self.config.instrument_id.venue)
        if account is None:
            self.log.warning("Skipping buy: account not available.")
            return

        settlement_currency = self.instrument.get_settlement_currency()
        free_balance = account.balance_free(settlement_currency)
        if free_balance is None:
            self.log.warning(
                f"Skipping buy: no free balance found for {settlement_currency}.",
            )
            return

        available_notional = (
            free_balance.as_decimal() * self.config.max_trade_allocation
        )
        if available_notional <= 0:
            self.log.info("Skipping buy: free balance is zero.", color=LogColor.BLUE)
            return

        min_notional = (
            self.instrument.min_notional.as_decimal()
            if self.instrument.min_notional is not None
            else Decimal("0")
        )
        if available_notional < min_notional:
            self.log.info(
                f"Skipping buy: available notional {available_notional} below min_notional {min_notional}.",
                color=LogColor.BLUE,
            )
            return

        quantity = self.instrument.make_qty(available_notional / self.last_close_price)
        if quantity <= 0:
            self.log.warning("Skipping buy: computed quantity is zero after rounding.")
            return

        self.log.info(
            "Submitting BUY order "
            f"free_balance={free_balance} "
            f"available_notional={available_notional} "
            f"last_close_price={self.last_close_price} "
            f"quantity={quantity}",
            color=LogColor.GREEN,
        )

        order: MarketOrder = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            time_in_force=TimeInForce.IOC,
        )

        self.submit_order(order)

    def on_data(self, data: Data) -> None:
        """
        Actions to be performed when the strategy is running and receives data.

        Parameters
        ----------
        data : Data
            The data received.

        """

    def on_event(self, event: Event) -> None:
        """
        Actions to be performed when the strategy is running and receives an event.

        Parameters
        ----------
        event : Event
            The event received.

        """

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        self.cancel_all_orders(self.config.instrument_id)
        if self.config.close_positions_on_stop:
            self.close_all_positions(self.config.instrument_id)

        # Unsubscribe from data
        self.unsubscribe_bars(self.config.bar_type)
        # self.unsubscribe_quote_ticks(self.config.instrument_id)
        self.unsubscribe_trade_ticks(self.config.instrument_id)
        # self.unsubscribe_order_book_deltas(self.config.instrument_id)
        # self.unsubscribe_order_book_at_interval(self.config.instrument_id)

    def on_reset(self) -> None:
        """
        Actions to be performed when the strategy is reset.
        """
        # Reset indicator here
        self.ema.reset()

    def on_save(self) -> dict[str, bytes]:
        """
        Actions to be performed when the strategy is saved.

        Create and return a state dictionary of values to be saved.

        Returns
        -------
        dict[str, bytes]
            The strategy state dictionary.

        """
        return {}

    def on_load(self, state: dict[str, bytes]) -> None:
        """
        Actions to be performed when the strategy is loaded.

        Saved state values will be contained in the give state dictionary.

        Parameters
        ----------
        state : dict[str, bytes]
            The strategy state dictionary.

        """

    def on_dispose(self) -> None:
        """
        Actions to be performed when the strategy is disposed.

        Cleanup any resources used by the strategy here.

        """
