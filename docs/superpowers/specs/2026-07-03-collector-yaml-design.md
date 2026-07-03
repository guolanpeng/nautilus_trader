# Collector YAML Design

## Goal

Build the `collector` binary as a live market-data recorder using local Nautilus crates. It should follow the `crates/adapters/lighter/examples/record_btc.rs` pattern, but choose subscriptions from a YAML file instead of hard-coded constants.

## YAML Schema

The YAML root keys are subscription data types. The initial supported keys are:

- `book_deltas`
- `quotes`
- `trades`

Each data type maps to exchanges. The initial supported exchanges are:

- `binance`
- `hyperliquid`
- `lighter`

Each exchange maps to a list of full Nautilus `InstrumentId` strings.

Example:

```yaml
book_deltas:
  binance:
    - BTCUSDT-PERP.BINANCE
  hyperliquid:
    - BTC-USD-PERP.HYPERLIQUID
  lighter:
    - BTC-PERP.LIGHTER

quotes:
  lighter:
    - BTC-PERP.LIGHTER

trades:
  lighter:
    - BTC-PERP.LIGHTER
```

Unknown root keys or exchange keys are configuration errors.

## Architecture

`collector` remains a small binary crate. It will:

1. Read a YAML config path from CLI argument or a documented default.
2. Deserialize the YAML into a strict config model.
3. Group requested instruments and subscription flags by exchange.
4. Build one `LiveNode` with streaming catalog enabled.
5. Register one data client per configured exchange.
6. Add one `DataTester` actor per configured exchange with the requested instruments and subscription flags.
7. Stop cleanly on Ctrl+C so streaming files flush.

Adapter setup uses existing local crates:

- `nautilus-binance` with `BinanceDataClientFactory`.
- `nautilus-hyperliquid` with `HyperliquidDataClientFactory`.
- `nautilus-lighter` with `LighterDataClientFactory`.

## Defaults

Use conservative defaults from the current examples:

- Nautilus environment: `Environment::Live`.
- Trader ID: `COLLECTOR-001`.
- Node name: `DATA-COLLECTOR-001`.
- Catalog path: `./catalog/collector`.
- Flush interval: `500` ms.
- Rotation interval: one minute.
- Logging: stdout info level.

Exchange adapter environments default to mainnet/live unless overridden later. The initial YAML schema does not add adapter-specific options.

## Validation And Errors

The collector should fail before starting the node when:

- The YAML file cannot be read.
- The YAML has an unsupported data type.
- The YAML has an unsupported exchange.
- A configured exchange has no instruments after grouping.
- No subscriptions are configured.

Instrument strings are parsed with `InstrumentId::from`, matching the examples.

## Testing

The first implementation should include focused unit tests for YAML parsing and grouping:

- Parses valid `book_deltas`, `quotes`, and `trades`.
- Rejects an unknown data type.
- Rejects an unknown exchange.
- Merges multiple data types for the same exchange into one exchange plan.

Build verification should run `cargo check -p collector`.
