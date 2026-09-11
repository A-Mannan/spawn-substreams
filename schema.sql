-- Spawn launchpad data layer — PostgreSQL schema for the Substreams SQL sink
-- (Database Changes mode). Applied by `substreams sink postgres setup`.
--
-- Conventions:
--   * facts are insert-only, PK = ordinal_key ("<block>:<log_index>") or
--     natural composite keys; the sink deletes them on reorg.
--   * state tables are upserted (last write wins, idempotent under replay).
--   * aggregate columns are maintained ONLY by the sink's delta ops
--     (add/sub/max/set_if_null) — never by triggers.
--   * wei values are NUMERIC(78,0); levels/ticks are ints; ids are hex text.
--   * no CHECK constraints (zero-value on-chain events are legal).
--   * no cross-table FKs (sink flushes per table).

-- ===========================================================================
-- STATE (upsert)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS tokens (
    token        VARCHAR(42)  PRIMARY KEY,
    name         VARCHAR(64)  NOT NULL DEFAULT '',
    symbol       VARCHAR(32)  NOT NULL DEFAULT '',
    decimals     INTEGER      NOT NULL DEFAULT 18
);

CREATE TABLE IF NOT EXISTS pools (
    pool_id             VARCHAR(66)  PRIMARY KEY,
    creator             VARCHAR(42)  NOT NULL DEFAULT '',
    token               VARCHAR(42)  NOT NULL DEFAULT '',
    total_supply        NUMERIC(78,0) NOT NULL DEFAULT 0,
    opening_level       INTEGER      NOT NULL DEFAULT 0,
    far_level           INTEGER      NOT NULL DEFAULT 0,
    graduation_level    INTEGER,
    config_hash         VARCHAR(66)  NOT NULL DEFAULT '',
    payout_plan         NUMERIC(78,0),
    dev_buy_share_wad   NUMERIC(78,0),
    status              VARCHAR(16)  NOT NULL DEFAULT 'bonding',
    launch_block        BIGINT,
    launch_time         BIGINT,
    graduation_block    BIGINT,
    graduation_time     BIGINT
);
CREATE INDEX IF NOT EXISTS idx_pools_token    ON pools(token);
CREATE INDEX IF NOT EXISTS idx_pools_status   ON pools(status);
CREATE INDEX IF NOT EXISTS idx_pools_launch_block ON pools(launch_block);

CREATE TABLE IF NOT EXISTS bands (
    pool_id         VARCHAR(66)  NOT NULL,
    band_index      INTEGER      NOT NULL,
    level_lower     INTEGER      NOT NULL,
    level_upper     INTEGER      NOT NULL,
    liquidity       NUMERIC(78,0) NOT NULL DEFAULT 0,
    token_inventory NUMERIC(78,0) NOT NULL DEFAULT 0,
    status          VARCHAR(16)  NOT NULL DEFAULT 'live',
    deployed_block  BIGINT,
    deployed_time   BIGINT,
    harvested_block BIGINT,
    PRIMARY KEY (pool_id, band_index)
);
CREATE INDEX IF NOT EXISTS idx_bands_status ON bands(status);

CREATE TABLE IF NOT EXISTS pots (
    pool_id           VARCHAR(66)  PRIMARY KEY,
    balance           NUMERIC(78,0) NOT NULL DEFAULT 0,
    funded_total      NUMERIC(78,0) NOT NULL DEFAULT 0,
    service_fee_total NUMERIC(78,0) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS plugin_registry (
    registry_index     INTEGER     PRIMARY KEY,
    plugin             VARCHAR(42) NOT NULL,
    take_wad           NUMERIC(78,0) NOT NULL DEFAULT 0,
    gas_limit          INTEGER     NOT NULL DEFAULT 0,
    code_hash          VARCHAR(66) NOT NULL DEFAULT '',
    role               INTEGER     NOT NULL DEFAULT 0,
    suspended          BOOLEAN     NOT NULL DEFAULT FALSE,
    registered_block   BIGINT,
    suspension_set_block BIGINT
);
CREATE INDEX IF NOT EXISTS idx_plugin_registry_plugin ON plugin_registry(plugin);

CREATE TABLE IF NOT EXISTS economic_configs (
    version                      BIGINT      PRIMARY KEY,
    harvest_service_fee_wad      NUMERIC(78,0) NOT NULL,
    quote_creator_share_wad      NUMERIC(78,0) NOT NULL,
    token_milestone_fund_share_wad NUMERIC(78,0) NOT NULL,
    effective_block              BIGINT      NOT NULL
);

CREATE TABLE IF NOT EXISTS protocol_state (
    id                 VARCHAR(16)  PRIMARY KEY,
    economic_version   BIGINT,
    protocol_recipient VARCHAR(42),
    recipient_set_block BIGINT,
    last_indexed_block BIGINT
);

-- ===========================================================================
-- AGGREGATES (delta ops only; no triggers)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS pool_stats (
    pool_id                      VARCHAR(66) PRIMARY KEY,
    -- volumes (ETH = currency0 side of v4 swaps)
    buy_volume_eth               NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_eth              NUMERIC(78,0) NOT NULL DEFAULT 0,
    buy_volume_tokens            NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_tokens           NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_count                   BIGINT       NOT NULL DEFAULT 0,
    -- price (sqrtPriceX96 of token-per-ETH orientation)
    last_price_sqrt_x96          NUMERIC(78,0),
    ath_sqrt_x96                 NUMERIC(78,0),
    -- revenue, by source
    creator_revenue_total        NUMERIC(78,0) NOT NULL DEFAULT 0,
    creator_revenue_curve        NUMERIC(78,0) NOT NULL DEFAULT 0,
    creator_revenue_swap_fees    NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_total       NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_curve       NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_swap_fees  NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_harvest_fees NUMERIC(78,0) NOT NULL DEFAULT 0,
    creator_path_revenue_total   NUMERIC(78,0) NOT NULL DEFAULT 0,
    plugin_revenue_total         NUMERIC(78,0) NOT NULL DEFAULT 0,
    pot_funded_total             NUMERIC(78,0) NOT NULL DEFAULT 0,
    -- ladder
    tips_total                    NUMERIC(78,0) NOT NULL DEFAULT 0,
    bands_deployed                BIGINT      NOT NULL DEFAULT 0,
    harvest_count                 BIGINT      NOT NULL DEFAULT 0,
    harvest_quote_total           NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_fee_burned_tokens        NUMERIC(78,0) NOT NULL DEFAULT 0,
    -- supply tracking
    burned_total                  NUMERIC(78,0) NOT NULL DEFAULT 0,
    -- activity
    last_activity_block           BIGINT,
    last_swap_block                BIGINT
);

CREATE TABLE IF NOT EXISTS pool_day_stats (
    pool_id              VARCHAR(66) NOT NULL,
    day                  DATE        NOT NULL,
    buy_volume_eth       NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_eth      NUMERIC(78,0) NOT NULL DEFAULT 0,
    buy_volume_tokens    NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_tokens   NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_count           BIGINT      NOT NULL DEFAULT 0,
    open_sqrt_x96        NUMERIC(78,0),
    close_sqrt_x96       NUMERIC(78,0),
    high_sqrt_x96        NUMERIC(78,0),
    low_sqrt_x96         NUMERIC(78,0),
    creator_revenue_eth  NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_eth NUMERIC(78,0) NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_id, day)
);

CREATE TABLE IF NOT EXISTS harvest_payouts (
    pool_id          VARCHAR(66) NOT NULL,
    milestone_index  BIGINT       NOT NULL,
    gross_quote      NUMERIC(78,0),
    service_fee      NUMERIC(78,0),   -- to protocol ledger
    net_quote        NUMERIC(78,0),   -- the pot
    economic_version BIGINT,
    funded_block     BIGINT,
    funded_time      BIGINT,
    PRIMARY KEY (pool_id, milestone_index)
);

CREATE TABLE IF NOT EXISTS pool_minute_stats (
    pool_id              VARCHAR(66) NOT NULL,
    minute               BIGINT      NOT NULL,   -- unix minute bucket
    buy_volume_eth       NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_eth      NUMERIC(78,0) NOT NULL DEFAULT 0,
    buy_volume_tokens    NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_tokens   NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_count           BIGINT      NOT NULL DEFAULT 0,
    open_sqrt_x96        NUMERIC(78,0),
    close_sqrt_x96       NUMERIC(78,0),
    high_sqrt_x96        NUMERIC(78,0),
    low_sqrt_x96         NUMERIC(78,0),
    PRIMARY KEY (pool_id, minute)
);

CREATE TABLE IF NOT EXISTS pool_hour_stats (
    pool_id              VARCHAR(66) NOT NULL,
    hour                 BIGINT      NOT NULL,   -- unix hour bucket
    buy_volume_eth       NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_eth      NUMERIC(78,0) NOT NULL DEFAULT 0,
    buy_volume_tokens    NUMERIC(78,0) NOT NULL DEFAULT 0,
    sell_volume_tokens   NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_count           BIGINT      NOT NULL DEFAULT 0,
    open_sqrt_x96        NUMERIC(78,0),
    close_sqrt_x96       NUMERIC(78,0),
    high_sqrt_x96        NUMERIC(78,0),
    low_sqrt_x96         NUMERIC(78,0),
    PRIMARY KEY (pool_id, hour)
);

CREATE TABLE IF NOT EXISTS protocol_day_stats (
    day                   DATE       PRIMARY KEY,
    swap_volume_eth       NUMERIC(78,0) NOT NULL DEFAULT 0,
    swap_count            BIGINT     NOT NULL DEFAULT 0,
    creator_revenue_eth   NUMERIC(78,0) NOT NULL DEFAULT 0,
    protocol_revenue_eth  NUMERIC(78,0) NOT NULL DEFAULT 0,
    graduations           BIGINT     NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS protocol_stats (
    id                     VARCHAR(16) PRIMARY KEY,
    revenue_curve          NUMERIC(78,0) NOT NULL DEFAULT 0,
    revenue_swap_fees      NUMERIC(78,0) NOT NULL DEFAULT 0,
    revenue_milestone_harvest NUMERIC(78,0) NOT NULL DEFAULT 0,
    revenue_total          NUMERIC(78,0) NOT NULL DEFAULT 0,
    claimed_total          NUMERIC(78,0) NOT NULL DEFAULT 0
);

-- ===========================================================================
-- FACTS (insert-only)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS launches (
    ordinal_key   VARCHAR(32)  PRIMARY KEY,
    pool_id       VARCHAR(66)  NOT NULL,
    creator       VARCHAR(42)  NOT NULL,
    token         VARCHAR(42)  NOT NULL,
    total_supply  NUMERIC(78,0) NOT NULL,
    opening_level INTEGER      NOT NULL,
    far_level     INTEGER      NOT NULL,
    config_hash   VARCHAR(66)  NOT NULL,
    block_number  BIGINT       NOT NULL,
    timestamp     BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_launches_pool ON launches(pool_id);

CREATE TABLE IF NOT EXISTS graduations (
    ordinal_key          VARCHAR(32)  PRIMARY KEY,
    pool_id              VARCHAR(66)  NOT NULL,
    graduation_level     INTEGER      NOT NULL,
    quote_proceeds       NUMERIC(78,0) NOT NULL,
    lp_seed_quote        NUMERIC(78,0) NOT NULL,
    creator_quote        NUMERIC(78,0) NOT NULL,
    protocol_quote       NUMERIC(78,0) NOT NULL,
    full_range_liquidity NUMERIC(78,0) NOT NULL,
    block_number         BIGINT       NOT NULL,
    timestamp            BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_graduations_pool ON graduations(pool_id);

CREATE TABLE IF NOT EXISTS swaps (
    tx_hash          VARCHAR(66) NOT NULL,
    log_index        INTEGER     NOT NULL,
    pool_id          VARCHAR(66) NOT NULL,
    token            VARCHAR(42) NOT NULL,
    sender           VARCHAR(42) NOT NULL,
    is_buy           BOOLEAN     NOT NULL,
    amount0_eth      NUMERIC(78,0) NOT NULL,
    amount1_tokens   NUMERIC(78,0) NOT NULL,
    sqrt_price_x96   NUMERIC(78,0) NOT NULL,
    liquidity         NUMERIC(78,0) NOT NULL,
    tick              BIGINT      NOT NULL,
    fee               BIGINT      NOT NULL,
    fee_eth           NUMERIC(78,0) NOT NULL DEFAULT 0,
    fee_tokens        NUMERIC(78,0) NOT NULL DEFAULT 0,
    block_number      BIGINT      NOT NULL,
    timestamp         BIGINT      NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_swaps_pool_block ON swaps(pool_id, block_number);

CREATE INDEX IF NOT EXISTS idx_swaps_pool_time ON swaps(pool_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_swaps_token_block ON swaps(token, block_number);
CREATE TABLE IF NOT EXISTS curve_deployments (
    ordinal_key   VARCHAR(32)  PRIMARY KEY,
    pool_id       VARCHAR(66)  NOT NULL,
    minted        NUMERIC(78,0) NOT NULL,
    deployed      BIGINT       NOT NULL,
    token_settled NUMERIC(78,0) NOT NULL,
    block_number  BIGINT       NOT NULL,
    timestamp     BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS milestone_harvests (
    ordinal_key         VARCHAR(32)  PRIMARY KEY,
    pool_id             VARCHAR(66)  NOT NULL,
    band_index          BIGINT       NOT NULL,
    quote_proceeds      NUMERIC(78,0) NOT NULL,
    token_residue       NUMERIC(78,0) NOT NULL,
    completed_milestones BIGINT      NOT NULL,
    block_number        BIGINT       NOT NULL,
    timestamp           BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_harvests_pool ON milestone_harvests(pool_id);

CREATE TABLE IF NOT EXISTS band_skips (
    ordinal_key       VARCHAR(32)  PRIMARY KEY,
    pool_id           VARCHAR(66)  NOT NULL,
    band_index        BIGINT       NOT NULL,
    carried_inventory NUMERIC(78,0) NOT NULL,
    block_number      BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS dev_buys (
    ordinal_key   VARCHAR(32)  PRIMARY KEY,
    pool_id       VARCHAR(66)  NOT NULL,
    tokens_bought NUMERIC(78,0) NOT NULL,
    eth_spent     NUMERIC(78,0) NOT NULL,
    block_number  BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS dev_buy_skips (
    ordinal_key      VARCHAR(32)  PRIMARY KEY,
    pool_id          VARCHAR(66)  NOT NULL,
    relayer          VARCHAR(42)  NOT NULL,
    tokens_requested NUMERIC(78,0) NOT NULL,
    block_number     BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS payout_pot_fundings (
    ordinal_key      VARCHAR(32)  PRIMARY KEY,
    pool_id          VARCHAR(66)  NOT NULL,
    milestone_index  BIGINT       NOT NULL,
    gross_quote      NUMERIC(78,0) NOT NULL,
    service_fee      NUMERIC(78,0) NOT NULL,
    net_quote        NUMERIC(78,0) NOT NULL,
    economic_version BIGINT       NOT NULL,
    block_number     BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pot_fundings_pool ON payout_pot_fundings(pool_id);

CREATE TABLE IF NOT EXISTS payout_pot_redemptions (
    ordinal_key VARCHAR(32)  PRIMARY KEY,
    pool_id     VARCHAR(66)  NOT NULL,
    amount      NUMERIC(78,0) NOT NULL,
    block_number BIGINT      NOT NULL
);

CREATE TABLE IF NOT EXISTS payout_tips (
    ordinal_key VARCHAR(32)  PRIMARY KEY,
    pool_id     VARCHAR(66)  NOT NULL,
    flusher     VARCHAR(42)  NOT NULL,
    amount      NUMERIC(78,0) NOT NULL,
    block_number BIGINT      NOT NULL
);

CREATE TABLE IF NOT EXISTS plugin_payouts (
    ordinal_key    VARCHAR(32)  PRIMARY KEY,
    pool_id        VARCHAR(66)  NOT NULL,
    plugin_index   BIGINT       NOT NULL,
    plugin         VARCHAR(42)  NOT NULL,
    outcome        VARCHAR(16)  NOT NULL,
    current_share  NUMERIC(78,0) NOT NULL,
    previous_carry NUMERIC(78,0) NOT NULL,
    amount         NUMERIC(78,0) NOT NULL,
    block_number   BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plugin_payouts_pool ON plugin_payouts(pool_id);

CREATE TABLE IF NOT EXISTS creator_accruals (
    ordinal_key      VARCHAR(32)  PRIMARY KEY,
    pool_id          VARCHAR(66)  NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    source           VARCHAR(32)  NOT NULL,
    economic_version BIGINT       NOT NULL,
    block_number     BIGINT       NOT NULL,
    timestamp        BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_creator_accruals_pool ON creator_accruals(pool_id);

CREATE TABLE IF NOT EXISTS protocol_accruals (
    ordinal_key      VARCHAR(32)  PRIMARY KEY,
    pool_id          VARCHAR(66)  NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    source           VARCHAR(32)  NOT NULL,
    economic_version BIGINT       NOT NULL,
    block_number     BIGINT       NOT NULL,
    timestamp        BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS creator_path_accruals (
    ordinal_key VARCHAR(32)  PRIMARY KEY,
    pool_id     VARCHAR(66)  NOT NULL,
    amount      NUMERIC(78,0) NOT NULL,
    block_number BIGINT      NOT NULL
);

CREATE TABLE IF NOT EXISTS claims (
    ordinal_key  VARCHAR(32)  PRIMARY KEY,
    claim_type   VARCHAR(16)  NOT NULL,
    pool_id      VARCHAR(66)  NOT NULL DEFAULT '',
    holder       VARCHAR(42)  NOT NULL,
    amount       NUMERIC(78,0) NOT NULL,
    block_number BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claims_pool ON claims(pool_id);

CREATE TABLE IF NOT EXISTS creator_path_claim_failures (
    ordinal_key  VARCHAR(32)  PRIMARY KEY,
    pool_id      VARCHAR(66)  NOT NULL,
    holder       VARCHAR(42)  NOT NULL,
    amount       NUMERIC(78,0) NOT NULL,
    block_number BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS fee_collections (
    ordinal_key  VARCHAR(32)  PRIMARY KEY,
    pool_id      VARCHAR(66)  NOT NULL,
    caller       VARCHAR(42)  NOT NULL,
    quote_fees   NUMERIC(78,0) NOT NULL,
    token_fees   NUMERIC(78,0) NOT NULL,
    block_number BIGINT       NOT NULL,
    timestamp    BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS fee_routings (
    ordinal_key         VARCHAR(32)  PRIMARY KEY,
    pool_id             VARCHAR(66)  NOT NULL,
    creator_quote       NUMERIC(78,0) NOT NULL,
    protocol_quote      NUMERIC(78,0) NOT NULL,
    diverted_to_next_band NUMERIC(78,0) NOT NULL,
    tokens_burned       NUMERIC(78,0) NOT NULL,
    economic_version    BIGINT       NOT NULL,
    block_number        BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fee_routings_pool ON fee_routings(pool_id);

CREATE TABLE IF NOT EXISTS token_burns (
    ordinal_key  VARCHAR(32)  PRIMARY KEY,
    token        VARCHAR(42)  NOT NULL,
    burner       VARCHAR(42)  NOT NULL,
    amount       NUMERIC(78,0) NOT NULL,
    block_number BIGINT       NOT NULL,
    timestamp    BIGINT       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_token_burns_token ON token_burns(token);

-- ===========================================================================
-- DERIVED VIEWS (computed at read time — always consistent with state)
-- ===========================================================================

-- Price orientation note: pool price is token-per-ETH (tick falls = price
-- rises; level = -tick). sqrtPriceX96 is the raw v4 value: sqrt(token/ETH)
-- in Q96 — a *higher* sqrtPriceX96 means more ETH per token? No: v4 price
-- = token1/token0 = token/ETH, so higher sqrtPriceX96 = higher token price
-- in ETH. ath_sqrt_x96 = MAX over history, correct for "ATH token price".
CREATE OR REPLACE VIEW pool_metrics AS
SELECT
    p.pool_id,
    p.token,
    p.creator,
    p.status,
    p.launch_block,
    p.graduation_block,
    ps.buy_volume_eth,
    ps.sell_volume_eth,
    ps.swap_count,
    ps.last_price_sqrt_x96,
    ps.ath_sqrt_x96,
    ps.creator_revenue_total,
    ps.protocol_revenue_total,
    ps.burned_total,
    p.total_supply - ps.burned_total                    AS circulating_supply,
    -- price in ETH per token = (sqrtPriceX96 / 2^96)^2 * 10^18 (token units)
    -- stored raw; API converts with full precision.
    p.total_supply                                      AS total_supply_raw,
    -- mcap in wei-ETH = price_eth_per_token_wei * circulating_supply
    ((ps.last_price_sqrt_x96::numeric * ps.last_price_sqrt_x96::numeric)
        / (2::numeric ^ 192) * (p.total_supply - ps.burned_total)) AS mcap_wei,
    ((ps.ath_sqrt_x96::numeric * ps.ath_sqrt_x96::numeric)
        / (2::numeric ^ 192) * (p.total_supply - ps.burned_total)) AS ath_mcap_wei
FROM pools p
JOIN pool_stats ps ON ps.pool_id = p.pool_id;

-- Per-pool OHLC by day, volumes included; price conversion left to the API.
CREATE OR REPLACE VIEW pool_candles AS
SELECT
    pool_id,
    day,
    open_sqrt_x96,
    high_sqrt_x96,
    close_sqrt_x96,
    buy_volume_eth,
    sell_volume_eth,
    buy_volume_tokens,
    sell_volume_tokens,
    swap_count,
    creator_revenue_eth,
    protocol_revenue_eth
FROM pool_day_stats;

-- Protocol-wide daily health chart.
CREATE OR REPLACE VIEW protocol_metrics_daily AS
SELECT
    day,
    swap_volume_eth,
    swap_count,
    creator_revenue_eth,
    protocol_revenue_eth,
    graduations
FROM protocol_day_stats
ORDER BY day;

-- Candle roll-ups: OHLC algebra composes from the base granularity.
--   open  = first(open)  by time ASC
--   close = last(close)  by time DESC
--   high  = max(high), low = min(low), volumes/counts = sum()
CREATE OR REPLACE VIEW candles_5m AS
WITH b AS (
    SELECT pool_id,
           minute - (minute % 5) AS bucket,
           minute,
           open_sqrt_x96, close_sqrt_x96, high_sqrt_x96, low_sqrt_x96,
           buy_volume_eth, sell_volume_eth, buy_volume_tokens, sell_volume_tokens,
           swap_count
    FROM pool_minute_stats)
SELECT pool_id, bucket,
       (array_agg(open_sqrt_x96  ORDER BY minute))[1]      AS open_sqrt_x96,
       (array_agg(close_sqrt_x96 ORDER BY minute DESC))[1] AS close_sqrt_x96,
       max(high_sqrt_x96)                                  AS high_sqrt_x96,
       min(low_sqrt_x96)                                   AS low_sqrt_x96,
       sum(buy_volume_eth)                                 AS buy_volume_eth,
       sum(sell_volume_eth)                                AS sell_volume_eth,
       sum(buy_volume_tokens)                              AS buy_volume_tokens,
       sum(sell_volume_tokens)                             AS sell_volume_tokens,
       sum(swap_count)                                     AS swap_count
FROM b GROUP BY pool_id, bucket;

CREATE OR REPLACE VIEW candles_15m AS
WITH b AS (
    SELECT pool_id,
           minute - (minute % 15) AS bucket,
           minute,
           open_sqrt_x96, close_sqrt_x96, high_sqrt_x96, low_sqrt_x96,
           buy_volume_eth, sell_volume_eth, buy_volume_tokens, sell_volume_tokens,
           swap_count
    FROM pool_minute_stats)
SELECT pool_id, bucket,
       (array_agg(open_sqrt_x96  ORDER BY minute))[1]      AS open_sqrt_x96,
       (array_agg(close_sqrt_x96 ORDER BY minute DESC))[1] AS close_sqrt_x96,
       max(high_sqrt_x96)                                  AS high_sqrt_x96,
       min(low_sqrt_x96)                                   AS low_sqrt_x96,
       sum(buy_volume_eth)                                 AS buy_volume_eth,
       sum(sell_volume_eth)                                AS sell_volume_eth,
       sum(buy_volume_tokens)                              AS buy_volume_tokens,
       sum(sell_volume_tokens)                             AS sell_volume_tokens,
       sum(swap_count)                                     AS swap_count
FROM b GROUP BY pool_id, bucket;

CREATE OR REPLACE VIEW candles_4h AS
WITH b AS (
    SELECT pool_id,
           hour - (hour % 4) AS bucket,
           hour,
           open_sqrt_x96, close_sqrt_x96, high_sqrt_x96, low_sqrt_x96,
           buy_volume_eth, sell_volume_eth, buy_volume_tokens, sell_volume_tokens,
           swap_count
    FROM pool_hour_stats)
SELECT pool_id, bucket,
       (array_agg(open_sqrt_x96  ORDER BY hour))[1]      AS open_sqrt_x96,
       (array_agg(close_sqrt_x96 ORDER BY hour DESC))[1] AS close_sqrt_x96,
       max(high_sqrt_x96)                                AS high_sqrt_x96,
       min(low_sqrt_x96)                                 AS low_sqrt_x96,
       sum(buy_volume_eth)                               AS buy_volume_eth,
       sum(sell_volume_eth)                              AS sell_volume_eth,
       sum(buy_volume_tokens)                            AS buy_volume_tokens,
       sum(sell_volume_tokens)                           AS sell_volume_tokens,
       sum(swap_count)                                   AS swap_count
FROM b GROUP BY pool_id, bucket;

-- Leaderboard snapshot: pools ranked by lifetime volume (refreshed periodically;
-- dev: leaderboard-refresher sidecar every 30s; prod: pg_cron every 1-5 min with
-- REFRESH MATERIALIZED VIEW CONCURRENTLY — requires the unique index below).
CREATE MATERIALIZED VIEW IF NOT EXISTS leaderboard_daily AS
SELECT
    p.pool_id,
    p.token,
    t.symbol,
    p.status,
    ps.buy_volume_eth,
    ps.sell_volume_eth,
    ps.swap_count,
    ps.creator_revenue_total,
    ps.protocol_revenue_total,
    ps.ath_sqrt_x96,
    ps.last_price_sqrt_x96
FROM pools p
JOIN pool_stats ps ON ps.pool_id = p.pool_id
LEFT JOIN tokens t ON t.token = p.token
ORDER BY (ps.buy_volume_eth + ps.sell_volume_eth) DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_leaderboard_daily_pool ON leaderboard_daily(pool_id);
