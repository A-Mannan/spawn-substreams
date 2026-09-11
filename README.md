# spawn_substreams

Spawn launchpad data layer: indexes every Spawn launchpad contract event plus Uniswap v4 PoolManager swaps for launchpad pools, and loads them into PostgreSQL via the Substreams SQL sink (Database Changes mode).

## Overview

Decodes all 26 `MilestoneHook` events (launch, graduation, bonding-curve deployments, ladder bands, milestone harvests, payout pots, plugin payouts, creator/protocol revenue), the `PayoutPluginRegistry`, `ProtocolController`, `RevenueNFT`, launch-token `Transfer`s (dynamic data sources discovered from `Launched`), and Uniswap v4 `PoolManager` `Swap` events filtered to launchpad pools. Aggregates (buy/sell volume, revenue by source, ATH price, burned supply) are maintained with reorg-safe delta operations; derived metrics (mcap, FDV) are plain SQL views.

## Modules

| Module | Kind | Output | Description |
|---|---|---|---|
| `map_spawn_events` | map | `spawn.v1.SpawnEvents` | All protocol contract events, ABI-decoded |
| `map_token_burns` | map | `spawn.v1.SpawnEvents` | Launch-token burn Transfers (to `0x0`) for burned supply |
| `map_token_meta` | map | `spawn.v1.SpawnEvents` | Token name/symbol/decimals via RPC enrichment (`RpcBatch`) |
| `map_v4_swaps` | map | `spawn.v1.TradedSwaps` | v4 PoolManager swaps for launchpad pools |
| `store_pools` | store | `proto:spawn.v1.Launch` | `pool_id -> Launch` and `token -> Launch` pivot |
| `index_spawn_activity` | blockIndex | `sf.substreams.index.v1.Keys` | `log_addr:`/`topic0:` keys per block |
| `db_out` | map | `sf.substreams.sink.database.v1.DatabaseChanges` | SQL sink payload: facts + state + delta aggregates |

## Prerequisites

- [Substreams CLI](https://docs.substreams.dev/how-to-guides/installing-the-cli) v1.20.2+ (hosted endpoints). The local dev Firehose image in `docker-compose.yml` predates s2 block compression, so run against it with CLI v1.17.11.
- Rust with `wasm32-unknown-unknown`
- `buf` for protobuf codegen
- PostgreSQL 14+ for the sink

## Quick Start

Local dev chain (Firehose-instrumented geth + Postgres) — see `docker-compose.yml`:

```bash
docker compose up -d
substreams build
substreams run -e localhost:9000 --plaintext ./substreams.yaml map_spawn_events -s 0 --stop-block +1000
```

Load into Postgres:

```bash
export DSN='psql://postgres:postgres@localhost:5433/spawn?sslmode=disable'
substreams sink postgres setup ./substreams.yaml --dsn "$DSN"
substreams sink postgres ./substreams.yaml --dsn "$DSN"
```

## Contract addresses

All addresses are manifest `params` (the hook is CREATE2-mined, so they differ per deployment). Pin them in `substreams.yaml` under `params:` or override at run time with `-p map_spawn_events="hook=0x… registry=0x… controller=0x… revenue_nft=0x…"`.
