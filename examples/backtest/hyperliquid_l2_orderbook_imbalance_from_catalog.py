#!/usr/bin/env python3
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

from __future__ import annotations

import argparse
import os
import tempfile
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from nautilus_trader.adapters.hyperliquid.constants import HYPERLIQUID_VENUE
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalance
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalanceConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import book_type_from_str
from nautilus_trader.model.enums import book_type_to_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import FIXED_PRECISION_BYTES
from nautilus_trader.model.objects import FIXED_SCALAR
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.wranglers_v2 import OrderBookDeltaDataWranglerV2


class CleanOrderBookImbalance(OrderBookImbalance):
    """
    OrderBookImbalance without Betfair test subscriptions.
    """

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        if self.config.use_quote_ticks:
            self.book_type = BookType.L1_MBP
            self.subscribe_quote_ticks(self.instrument.id)
        else:
            self.book_type = book_type_from_str(self.config.book_type)
            self.subscribe_order_book_deltas(self.instrument.id, self.book_type)

        self._last_trigger_timestamp = None


def _discover_instruments(catalog_path: Path) -> list[str]:
    root = catalog_path / "order_book_deltas"
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def _build_hyperliquid_perp(
    instrument_id: InstrumentId,
    price_precision: int,
    size_precision: int,
) -> CryptoPerpetual:
    symbol = instrument_id.symbol.value  # e.g. ETH-USD-PERP
    base = symbol.split("-")[0]
    quote = symbol.split("-")[1]

    return CryptoPerpetual(
        instrument_id=instrument_id,
        raw_symbol=Symbol(base),
        base_currency=Currency.from_str(base),
        quote_currency=Currency.from_str(quote),
        settlement_currency=Currency.from_str(quote),
        is_inverse=False,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=Price(10 ** (-price_precision), precision=price_precision),
        size_increment=Quantity(10 ** (-size_precision), precision=size_precision),
        max_quantity=None,
        min_quantity=None,
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.025"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        ts_event=0,
        ts_init=0,
    )


def _prepare_catalog_root(path: Path) -> tuple[Path, tempfile.TemporaryDirectory[str] | None]:
    # Standard Nautilus catalog root has `data/` subdirectory.
    if (path / "data").exists():
        return path, None

    # Allow using flat example layout:
    #   examples/data/order_book_deltas/<instrument_id>/*.parquet
    if (path / "order_book_deltas").exists():
        tempdir = tempfile.TemporaryDirectory(prefix="nautilus_catalog_")
        os.symlink(path, Path(tempdir.name) / "data", target_is_directory=True)
        return Path(tempdir.name), tempdir

    return path, None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run OrderBookImbalance backtest with local L2 parquet data.",
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("examples/data"),
        help="Path to parquet catalog root (default: examples/data).",
    )
    parser.add_argument(
        "--instrument-id",
        type=str,
        default=None,
        help="Instrument ID, e.g. ETH-USD-PERP.HYPERLIQUID. Default: auto-detect first.",
    )
    parser.add_argument(
        "--max-trade-size",
        type=Decimal,
        default=Decimal("0.01"),
        help="Max trade size per trigger.",
    )
    parser.add_argument(
        "--trigger-min-size",
        type=float,
        default=100.0,
        help="Minimum larger-side best level size to trigger.",
    )
    parser.add_argument(
        "--trigger-imbalance-ratio",
        type=float,
        default=0.2,
        help="Smaller/larger best-level size ratio threshold.",
    )
    parser.add_argument(
        "--min-seconds-between-triggers",
        type=float,
        default=1.0,
        help="Minimum seconds between triggers.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Enable dry run mode for strategy (no order submission).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Only load first N parquet files (useful for quick tests).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="ERROR",
        help="Engine log level (DEBUG/INFO/WARNING/ERROR).",
    )
    return parser.parse_args()


def _discover_precisions(catalog_path: Path, instrument_id_str: str) -> tuple[int, int]:
    source_dir = catalog_path / "order_book_deltas" / instrument_id_str
    files = sorted(source_dir.glob("*.parquet"), key=lambda p: int(p.stem.split("_")[-1]))
    if not files:
        return 5, 6

    first = pq.read_schema(files[0])
    metadata = first.metadata or {}
    price_precision = int((metadata.get(b"price_precision") or b"5").decode())
    size_precision = int((metadata.get(b"size_precision") or b"6").decode())
    return price_precision, size_precision


def _load_deltas_from_flat_parquet(
    catalog_path: Path,
    instrument_id_str: str,
    max_files: int | None = None,
) -> list[OrderBookDelta]:
    source_dir = catalog_path / "order_book_deltas" / instrument_id_str
    if not source_dir.exists():
        return []

    files = sorted(source_dir.glob("*.parquet"), key=lambda p: int(p.stem.split("_")[-1]))
    if max_files is not None:
        files = files[:max_files]
    if not files:
        return []

    output: list[OrderBookDelta] = []
    wrangler: OrderBookDeltaDataWranglerV2 | None = None

    for file_path in files:
        table_raw = pq.read_table(file_path)
        metadata = table_raw.schema.metadata or {}
        price_precision = int((metadata.get(b"price_precision") or b"5").decode())
        size_precision = int((metadata.get(b"size_precision") or b"6").decode())

        if wrangler is None:
            wrangler = OrderBookDeltaDataWranglerV2(
                instrument_id=instrument_id_str,
                price_precision=price_precision,
                size_precision=size_precision,
            )

        df = table_raw.to_pandas()
        price = (
            df["price"]
            .apply(lambda x: int(float(x) * FIXED_SCALAR))
            .apply(lambda x: x.to_bytes(FIXED_PRECISION_BYTES, byteorder="little", signed=True))
            .to_numpy()
         )
        size = (
            df["size"]
            .apply(lambda x: int(float(x) * FIXED_SCALAR))
            .apply(lambda x: x.to_bytes(FIXED_PRECISION_BYTES, byteorder="little", signed=False))
            .to_numpy()
        )

        converted = pa.Table.from_arrays(
            arrays=[
                pa.array(df["action"].to_numpy(dtype="uint8"), type=pa.uint8()),
                pa.array(df["side"].to_numpy(dtype="uint8"), type=pa.uint8()),
                pa.array(price, type=pa.binary(FIXED_PRECISION_BYTES)),
                pa.array(size, type=pa.binary(FIXED_PRECISION_BYTES)),
                pa.array(df["order_id"].to_numpy(dtype="uint64"), type=pa.uint64()),
                pa.array(df["flags"].to_numpy(dtype="uint8"), type=pa.uint8()),
                pa.array(df["sequence"].to_numpy(dtype="uint64"), type=pa.uint64()),
                pa.array(df["ts_event"].to_numpy(dtype="uint64"), type=pa.uint64()),
                pa.array(df["ts_init"].to_numpy(dtype="uint64"), type=pa.uint64()),
            ],
            names=[
                "action",
                "side",
                "price",
                "size",
                "order_id",
                "flags",
                "sequence",
                "ts_event",
                "ts_init",
            ],
        )

        pyo3_deltas = wrangler.from_arrow(converted)
        output.extend(OrderBookDelta.from_pyo3_list(pyo3_deltas))

    return output


if __name__ == "__main__":
    args = _parse_args()
    catalog_path = args.catalog_path.resolve()

    available_instruments = _discover_instruments(catalog_path)
    if not available_instruments:
        raise FileNotFoundError(
            f"No L2 data found under {catalog_path / 'order_book_deltas'}.",
        )

    instrument_id_str = args.instrument_id or available_instruments[0]
    if instrument_id_str not in available_instruments:
        raise ValueError(
            f"`{instrument_id_str}` not found in catalog. Available: {available_instruments}",
        )

    instrument_id = InstrumentId.from_str(instrument_id_str)
    price_precision, size_precision = _discover_precisions(catalog_path, instrument_id_str)
    instrument = _build_hyperliquid_perp(
        instrument_id=instrument_id,
        price_precision=price_precision,
        size_precision=size_precision,
    )

    print(f"Catalog path: {catalog_path}")
    print(f"Instrument: {instrument_id}")

    deltas: list = []

    try:
        catalog_root, tempdir = _prepare_catalog_root(catalog_path)
        catalog = ParquetDataCatalog(catalog_root)
        deltas = catalog.order_book_deltas(instrument_ids=[instrument_id_str], batched=False)
        if tempdir is not None:
            tempdir.cleanup()
    except Exception:
        deltas = []

    if not deltas:
        deltas = _load_deltas_from_flat_parquet(
            catalog_path=catalog_path,
            instrument_id_str=instrument_id_str,
            max_files=args.max_files,
        )

    if not deltas:
        raise RuntimeError(f"No order book deltas loaded for {instrument_id_str}.")
    print(f"Loaded {len(deltas)} OrderBookDelta rows")

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            logging=LoggingConfig(log_level=args.log_level),
        ),
    )

    book_type = BookType.L2_MBP
    engine.add_venue(
        venue=HYPERLIQUID_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(100_000, USD)],
        book_type=book_type,
        default_leverage=Decimal(3),
    )
    engine.add_instrument(instrument)
    engine.add_data(deltas)

    strategy_config = OrderBookImbalanceConfig(
        instrument_id=instrument_id,
        max_trade_size=args.max_trade_size,
        trigger_min_size=args.trigger_min_size,
        trigger_imbalance_ratio=args.trigger_imbalance_ratio,
        min_seconds_between_triggers=args.min_seconds_between_triggers,
        book_type=book_type_to_str(book_type),
        dry_run=args.dry_run,
    )
    strategy = CleanOrderBookImbalance(config=strategy_config)
    engine.add_strategy(strategy)

    engine.run()

    with pd.option_context(
        "display.max_rows",
        100,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(engine.trader.generate_account_report(HYPERLIQUID_VENUE))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    engine.dispose()
