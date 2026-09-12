# Spawn Data Layer — Backend Guide

How the Spawn launchpad database is structured, what every table means, how to
query it, and how real-time trades & chart updates reach the frontend.

Data source: a Substreams package (`spawn-substreams`) indexes every Spawn
contract event plus Uniswap v4 `PoolManager` swaps for launchpad pools, and
writes to PostgreSQL via the Substreams SQL sink in **Database Changes mode**.

```
geth → Firehose → Substreams modules (ABI-decode + aggregate in Rust/WASM)
     → db_out (DatabaseChanges) → SQL sink (single flush txn per block)
     → Postgres  →  REST/WS (broadcaster)  →  frontend
```

Guarantees you inherit:

- **Exactly-once, idempotent writes.** The sink tracks its cursor; replaying a
  range produces the same rows. Every flush is one transaction.
- **Reorg-safe.** On a reorg the sink deletes/rewrites affected rows and
  reverses aggregates via its journal. Facts and aggregates never disagree
  after the sink's commit.
- **Atomic aggregates.** Counters and OHLC columns are maintained by sink
  delta-ops (`ADD/SUB/MAX/MIN/SET/SET_IF_NULL`), not SQL triggers. Never
  mutate aggregate columns by hand and never add triggers to them — a sink
  flush plus a trigger update on the same row will deadlock or double-count.

---

## 1. Conventions

| Convention | Meaning |
|---|---|
| **All amounts are raw units** | `NUMERIC(78,0)` — wei for ETH, raw smallest unit for tokens (18 decimals). Divide by `10^18` at the API edge only. |
| **IDs are hex text** | `pool_id` = 32-byte v4 poolId (`0x` + 64 hex, `VARCHAR(66)`); addresses `0x` + 40 hex (`VARCHAR(42)`). |
| **Fact tables are insert-only** | PK `ordinal_key = "<block_number>:<log_ordinal>"`. Never updated; deleted only by reorg-undo. One row per on-chain event. |
| **State tables are upserted** | Last write wins; idempotent under replay. |
| **Aggregate tables are sink-only** | Columns only ever move via delta ops. |
| **No FKs, no CHECK constraints** | The sink flushes per table; zero-value events are legal on-chain. |

**Price orientation (important).** The v4 pool quotes ETH as currency0 and the
launch token as currency1. `sqrt_price_x96` is the raw Q16x96
`sqrt(token1/token0)` = `sqrt(token per ETH)`. Therefore:

```
token_price_in_eth (raw)  = sqrt_price_x96²  / 2^192
eth_price_of_token (raw)  = 2^192            / sqrt_price_x96²   ← inverse!
```

Both are raw units; scale by `10^18` for human display (token decimals are
fixed at 18). **Because the
ETH-price is the inverse, a bucket's highest ETH price corresponds to the
LOWEST sqrt_price_x96.** Any OHLC computed in ETH-per-token must convert
first, then take max/min (the sink stores OHLC on the raw sqrt axis; see §3).

**`swaps.is_buy`** = the trader bought tokens with ETH (v4 event: `amount0 < 0`
— ETH flowed into the pool). Stored amounts are absolute values; direction is
the boolean.

**`swaps.fee_eth` / `fee_tokens`** = the realized fee on that swap, computed as
`input_amount × fee / 1_000_000` (v4 fees are in millionths).

---

## 2. Tables

### 2.1 State (current values, upserted)

| Table | Grain | What it holds |
|---|---|---|
| `tokens` | token address | ERC-20 metadata (`name`, `symbol`, `uri`) from the `Launched` event, RPC fallback |
| `pools` | pool_id | One row per launch: creator, token, supply, opening/far/graduation levels, payout plan, dev-buy share, `status` (`bonding` → `graduated`), launch/graduation block+time |
| `bands` | (pool_id, band_index) | Post-graduation ladder bands: level range, liquidity, token inventory, `status` (`live`/`harvested`/`skipped`) |
| `pots` | pool_id | Milestone payout pot: current `balance`, lifetime `funded_total`, `service_fee_total` |
| `plugin_registry` | registry_index | Payout plugin registry entries (address, take-rate `take_wad`, gas limit, role, suspension) |
| `economic_configs` | version | Economic parameter sets (harvest fee, creator/pot shares) with `effective_block` |
| `protocol_state` | singleton | Current economic version, protocol recipient, last indexed block |

### 2.2 Aggregates (sink delta-ops only)

| Table | Grain | Notes |
|---|---|---|
| `pool_stats` | pool_id | **The per-pool rollup.** Buy/sell volume (ETH+tokens), `swap_count`, `last_price_sqrt_x96`, `ath_sqrt_x96`, revenue by source (`creator_revenue_{total,curve,swap_fees}`, `protocol_revenue_{total,curve,swap_fees,harvest_fees}`, `creator_path_revenue_total`, `plugin_revenue_total`), `pot_funded_total`, `tips_total`, `bands_deployed`, `harvest_count`, `harvest_quote_total`, `swap_fee_burned_tokens`, `burned_total`, `last_swap_block` |
| `pool_minute_stats` | (pool_id, minute) | **Chart base granularity.** 1-minute OHLCV (`open/close/high/low_sqrt_x96` on the raw sqrt axis) + buy/sell volumes + `swap_count`. Maintained by delta ops per swap. |
| `pool_hour_stats` | (pool_id, hour) | Same shape, hourly bucket |
| `pool_day_stats` | (pool_id, day) | Same shape, daily bucket + `creator_revenue_eth`, `protocol_revenue_eth` |
| `protocol_day_stats` | day | Protocol-wide daily volume/revenue/graduation count |
| `protocol_stats` | singleton | Lifetime protocol revenue by source + `claimed_total` |

### 2.3 Facts (insert-only event log)

| Table | Event | Key columns |
|---|---|---|
| `launches` | `Launched` | pool, creator, token, name/symbol/uri, supply, levels, config_hash |
| `graduations` | `Graduated` | level, quote proceeds split (lp_seed/creator/protocol), full-range + wall liquidity |
| `swaps` | v4 `Swap` (launchpad pools only) | `is_buy`, absolute `amount0_eth`/`amount1_tokens`, `sqrt_price_x96`, `tick`, `fee`, `fee_eth`, `fee_tokens`, `tx_hash`, `log_index`, block, timestamp |
| `curve_deployments` | bonding-curve deploy | minted/deployed/settled token counts |
| `milestone_harvests` | `MilestoneHarvest` | band_index, quote_proceeds, token_residue, completed_milestones |
| `harvest_payouts` | state+fact hybrid | per milestone: `gross_quote`, `service_fee`, `net_quote` (= the pot funding) |
| `band_skips` | band skipped at deploy | carried_inventory |
| `dev_buys` | creator dev buy | tokens_bought, eth_spent |
| `dev_buy_skips` | dev buy attempted, failed | relayer, tokens_requested |
| `payout_pot_fundings` | pot funded | gross/service_fee/net, economic_version |
| `payout_pot_redemptions` | pot redeemed | amount |
| `payout_tips` | flush tip | recipient, amount |
| `plugin_payouts` | plugin share paid | plugin, `outcome`, current_share, previous_carry, amount |
| `creator_accruals` / `protocol_accruals` | `CreatorAccrued`/`ProtocolAccrued` | **authoritative revenue ledger.** amount + `source` (`curve` \| `swap_fees`) + economic_version |
| `creator_path_accruals` | creator-path payout | amount |
| `claims` | `Claimed` | claim_type, holder, amount |
| `creator_path_claim_failures` | failed claim | holder, amount |
| `fee_collections` | `FeesCollected` | quote_fees, token_fees, caller |
| `fee_routings` | `FeesRouted` | creator/protocol split, `diverted_to_next_band`, `tokens_burned` |
| `token_burns` | token `Transfer(to=0x0)` | burner, amount — the only feed of `burned_total` |

> **Double-count warning (by design):** `Graduated`, `FeesRouted`,
> `PayoutPotFunded`, `MilestoneHarvest` rows carry amounts, but revenue
> aggregates are fed **only** by the accrual facts. Use accruals for money
> math; use the other facts for sequencing/audit. Sanity check:
> `SUM(creator_accruals.amount) = pool_stats.creator_revenue_total`.

### 2.4 Derived views

| View | Purpose |
|---|---|
| `pool_metrics` | Card/detail join: pools + pool_stats + circulating supply (`total_supply − burned_total`) + `mcap_wei` + `ath_mcap_wei` |
| `pool_candles` | Daily OHLC passthrough |
| `protocol_metrics_daily` | Protocol daily health chart |
| `candles_5m` / `candles_15m` | Roll-ups over `pool_minute_stats` (see §3) |
| `candles_4h` | Roll-up over `pool_hour_stats` |
| `leaderboard_daily` | Materialized view: pools ranked by 24h volume (see §3) |

Sink internals (`cursors`, `substreams_history`): do not touch — that's the
sink's reorg journal.

---

## 3. Charts

**Design principle:** everything ≥ 1 minute is precomputed at write time;
everything < 1 minute is stream state (§4). Nothing below 1m is stored.

- `pool_minute_stats` is the **single write-time candle table** — one extra
  delta-op upsert per swap, ~13 MB/year/pool. It caps every ≥1m chart read at
  ~0.15 ms regardless of trade count (benchmarked against a 100k-trade pool:
  aggregating raw `swaps` for the same 30-day chart was ~500 ms).
- Coarser granularities are **read-time roll-up views** — OHLC algebra
  composes: `open = first(minute open)`, `close = last(minute close)`,
  `high = max(high)`, `low = min(low)`, volumes/counts `sum()`:

```sql
CREATE OR REPLACE VIEW candles_5m AS
WITH b AS (
  SELECT pool_id,
         minute - (minute % 5) AS bucket, minute,   -- 15m/60m likewise; candles_4h rolls hours
         open_sqrt_x96, close_sqrt_x96, high_sqrt_x96, low_sqrt_x96,
         buy_volume_eth, sell_volume_eth, buy_volume_tokens, sell_volume_tokens, swap_count
  FROM pool_minute_stats)
SELECT pool_id, bucket,
       (array_agg(open_sqrt_x96  ORDER BY minute))[1]     AS open_sqrt_x96,
       (array_agg(close_sqrt_x96 ORDER BY minute DESC))[1] AS close_sqrt_x96,
       max(high_sqrt_x96) AS high_sqrt_x96, min(low_sqrt_x96) AS low_sqrt_x96,
       sum(buy_volume_eth) AS buy_volume_eth, sum(sell_volume_eth) AS sell_volume_eth,
       sum(buy_volume_tokens) AS buy_volume_tokens, sum(sell_volume_tokens) AS sell_volume_tokens,
       sum(swap_count) AS swap_count
FROM b GROUP BY pool_id, bucket;
```

- **Arbitrary windows** (e.g. 7s, 2h) rebuild from `swaps` — O(trades-in-window),
  fine for hours-scale windows thanks to `idx_swaps_pool_time (pool_id, timestamp)`:

```sql
SELECT to_timestamp((timestamp / $bucket_seconds) * $bucket_seconds) AS time,
       (array_agg(sqrt_price_x96 ORDER BY timestamp, log_index))[1]           AS open,
       max(sqrt_price_x96)                                                    AS high,
       min(sqrt_price_x96)                                                    AS low,
       (array_agg(sqrt_price_x96 ORDER BY timestamp DESC, log_index DESC))[1] AS close,
       sum(amount0_eth)                                                       AS volume_eth,
       count(*)                                                               AS trades
FROM swaps
WHERE pool_id = $1 AND timestamp >= $from AND timestamp < $to
GROUP BY (timestamp / $bucket_seconds) * $bucket_seconds
ORDER BY time;
```

- **Gaps:** buckets with zero trades return **no row** — the client renders the
  flat bar; don't `generate_series` server-side for large ranges.
- **Leaderboard:** `leaderboard_daily` is a materialized view over
  `pools + pool_stats + tokens` ordered by daily volume, refreshed every 30 s
  by a sidecar container (`spawn-leaderboard-refresher` in docker-compose).
  In prod, replace with `pg_cron`: `SELECT cron.schedule('lb','*/1 * * * *',
  $$REFRESH MATERIALIZED VIEW CONCURRENTLY leaderboard_daily$$);`
  (needs a unique index for `CONCURRENTLY`).

---

## 4. Streaming (trades → frontend)

Sub-minute candles and the live ticker come from a **stream**, not the DB.
Postgres is the source of truth; the stream is delivery.

```
sink writes swap ─ AFTER INSERT trigger (notify-only) ─ pg_notify('spawn_trades', JSON)
                                                            │ delivered at COMMIT
Broadcaster service (Node): pg LISTEN ─ WS hub /ws?pool=0x…
    ├─ "tick"       every trade (price converted to ETH-per-token)
    ├─ "bar"        current 1m bar, throttled ~250 ms per pool
    └─ "bar_close"  final bar at minute roll
REST: GET /candles/:pool?interval=1m  → pool_minute_stats
      GET /candles/:pool?interval=5s  → §3 arbitrary-window query on swaps
```

Properties that come for free from `pg_notify` semantics: notifications fire
only at **commit** (frontend never sees an uncommitted trade), reorg-deleted
rows **never notify**, and the trigger is notify-only + exception-guarded so it
can never fail a sink flush.

### Trigger contract

```sql
CREATE OR REPLACE FUNCTION notify_spawn_trade() RETURNS trigger AS $$
BEGIN
  BEGIN
    PERFORM pg_notify('spawn_trades', json_build_object(
      'pool_id', NEW.pool_id, 'is_buy', NEW.is_buy,
      'eth', NEW.amount0_eth::text, 'tokens', NEW.amount1_tokens::text,
      'sqrt', NEW.sqrt_price_x96::text, 'ts', NEW.timestamp,
      'block', NEW.block_number, 'tx', NEW.tx_hash, 'log_index', NEW.log_index
    )::text);
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_spawn_trade AFTER INSERT ON swaps
FOR EACH ROW EXECUTE FUNCTION notify_spawn_trade();
-- plus a pools trigger on 'spawn_pools' for token-card events (launch/status)
```

### Broadcaster contract (to be built; spec frozen here)

WS messages (all JSON, per-pool subscription via `/ws?pool=0x…`):

```jsonc
{ "type": "tick", "pool": "0x…", "isBuy": true,
  "priceEth": "0.0004217",      // 2^192 / sqrt², ETH per token — converted server-side (BigInt)
  "sqrt": "688…",               // raw, for precision-sensitive clients
  "eth": "250000000000000000", "tokens": "5929…",
  "ts": 1757001234, "block": 1800123, "tx": "0x…" }

{ "type": "bar", "interval": "1m", "pool": "0x…", "start": 1757001200,
  "open": "0.0004201", "high": "0.0004230", "low": "0.0004195",
  "close": "0.0004217", "volEth": "1.834", "trades": 37 }

{ "type": "bar_close", … }      // same fields, final bar of the minute
```

The broadcaster maintains the current 1m bar per active pool in memory with the
same delta algebra as the sink (open = set-if-null, close = set, high = max,
low = min, vol +=). **OHLC must be computed after converting sqrt → ETH-price**
(the inversion, §1). Sub-minute bars (5s/10s/15s) are built **client-side**
from `tick`s — one code path in the browser, nothing stored anywhere.

### Frontend consumption (entire job)

```js
const bars = await fetch(`/candles/${pool}?interval=1m&limit=500`);
series.setData(bars.map(toChartPoint));          // lightweight-charts / TradingView

ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.type === 'bar')  series.update({ time: m.start, open: +m.open, high: +m.high,
                                         low: +m.low, close: +m.close });
  if (m.type === 'tick') /* ticker tape, recent-trades feed, client-side sub-minute bar */;
};
// reconnect → refetch ?limit=50 via REST, series.update() to splice, resume
```

- `series.update()` per message — the chart library merges into the current
  candle; no diffing, no polling.
- **Reconciliation:** WS is at-most-once; REST is truth. Reconnect = short REST
  re-fetch, always converges with `pool_minute_stats`.
- **Reorgs:** run the sink with `--final-blocks-only` in prod → the stream
  carries only irreversible blocks; reorg handling for the frontend becomes a
  non-problem at ~2 s latency (Base block time).
- **Scale:** one LISTEN connection handles thousands of msgs/s. If ever
  outgrown, swap the broadcaster's *source* (sink's native WebSocket output);
  WS clients don't change.

---

## 5. Common recipes

```sql
-- Token grid / pool cards
SELECT * FROM pool_metrics WHERE status = 'bonding' ORDER BY launch_time DESC;

-- Pool detail header
SELECT * FROM pool_metrics WHERE pool_id = $1;

-- Recent trades for a pool
SELECT timestamp, is_buy, amount0_eth, amount1_tokens, sqrt_price_x96, tx_hash
FROM swaps WHERE pool_id = $1 ORDER BY block_number DESC, log_index DESC LIMIT 50;

-- 1m chart (fast path)
SELECT * FROM pool_minute_stats WHERE pool_id = $1 ORDER BY minute DESC LIMIT 1440;

-- Token price now (ETH per token, raw)
SELECT (2::numeric^192) / last_price_sqrt_x96 AS eth_per_token_raw
FROM pool_stats WHERE pool_id = $1;

-- Circulating supply
SELECT total_supply - burned_total FROM pool_metrics WHERE pool_id = $1;
```

Revenue conservation (should always hold per pool):
`SUM(creator_accruals.amount WHERE source) = pool_stats.creator_revenue_curve
+ creator_revenue_swap_fees`, and the same for protocol accruals.

---

## 6. Operations

```bash
docker compose up -d                    # firehose geth + postgres (host port 5433) + leaderboard refresher
export DSN='psql://postgres:postgres@localhost:5433/spawn?sslmode=disable'
substreams sink postgres setup ./substreams.yaml --dsn "$DSN"   # applies schema.sql
substreams sink postgres ./substreams.yaml --dsn "$DSN"         # run the sink
```

- Contract addresses are **manifest `params`** in `substreams.yaml` — re-pin
  them per deployment (`-p map_spawn_events="hook=0x… registry=0x…" -p
  map_v4_swaps="pool_manager=0x…"`). The hook is CREATE2-mined; addresses
  change on every redeploy.
- `schema.sql` is applied by `sink setup`; add new tables there, not by hand.
- Do not `UPDATE`/`DELETE` anything the sink owns; do not add triggers on
  aggregate tables (§1). Reindex/vacuum as usual for `swaps` growth; consider
  month-partitioning `swaps` and detaching old partitions to cold storage
  when volume warrants.
