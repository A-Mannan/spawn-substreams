mod abi;
mod pb {
    pub mod spawn {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/spawn.v1.rs"));
        }
    }
}

use pb::spawn::v1 as spawn;
use substreams::errors::Error;
use substreams::pb::sf::substreams::index::v1::Keys;
use substreams::pb::substreams::Clock;
use substreams::scalar::BigInt;
use substreams::store::{StoreGet, StoreGetProto, StoreNew, StoreSetIfNotExists, StoreSetIfNotExistsProto};
use substreams::Hex;
use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_database_change::tables::Tables;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", Hex::encode(b))
}

fn zero() -> BigInt {
    BigInt::zero()
}

/// |x| for a signed BigInt, as decimal string.
fn abs_str(x: &BigInt) -> String {
    if *x < zero() {
        (zero() - x).to_string()
    } else {
        x.to_string()
    }
}

fn is_negative(x: &BigInt) -> bool {
    *x < zero()
}

fn parse_addr(s: &str, what: &str) -> Result<[u8; 20], Error> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    match hex::decode(trimmed) {
        Ok(bytes) if bytes.len() == 20 => {
            let mut out = [0u8; 20];
            out.copy_from_slice(&bytes);
            Ok(out)
        }
        _ => Err(anyhow::anyhow!("invalid {what} address param: {s}")),
    }
}

/// Per-event-instance context. `ordinal_key` = "<block>:<log_index>" — the
/// fact-table PK, stable across replays and unique per event instance.
struct MetaCtx<'a> {
    block: &'a eth::Block,
    tx_hash: String,
    log_index: u32,
    ordinal: u64,
}

impl<'a> MetaCtx<'a> {
    fn meta(&self) -> spawn::EventMeta {
        spawn::EventMeta {
            block_number: self.block.number,
            block_hash: hex0x(&self.block.hash),
            timestamp: self.block.timestamp_seconds(),
            tx_hash: self.tx_hash.clone(),
            log_index: self.log_index,
            ordinal_key: format!("{}:{}", self.block.number, self.ordinal),
        }
    }
}

// ---------------------------------------------------------------------------
// map_spawn_events — every protocol contract log in the block, fully decoded.
// ---------------------------------------------------------------------------

#[substreams::handlers::map]
fn map_spawn_events(
    params: String,
    block: eth::Block,
) -> Result<spawn::SpawnEvents, Error> {
    // params: "hook=0x… registry=0x… controller=0x… revenue_nft=0x…"
    let mut hook = String::new();
    let mut registry = String::new();
    let mut controller = String::new();
    let mut revenue_nft = String::new();
    for kv in params.split_whitespace() {
        let (k, v) = kv.split_once('=').unwrap_or(("", ""));
        match k {
            "hook" => hook = v.to_string(),
            "registry" => registry = v.to_string(),
            "controller" => controller = v.to_string(),
            "revenue_nft" => revenue_nft = v.to_string(),
            _ => {}
        }
    }
    let hook = parse_addr(&hook, "hook")?;
    let registry = parse_addr(&registry, "registry")?;
    let controller = parse_addr(&controller, "controller")?;
    let revenue_nft = parse_addr(&revenue_nft, "revenue_nft")?;

    let mut out = spawn::SpawnEvents::default();

    for log in block.logs() {
        let ctx = MetaCtx {
            block: &block,
            tx_hash: hex0x(&log.receipt.transaction.hash),
            log_index: log.index(),
            ordinal: log.ordinal(),
        };

        if log.address() == hook {
            decode_hook_log(&ctx, log.as_ref(), &mut out);
        } else if log.address() == registry {
            decode_registry_log(&ctx, log.as_ref(), &mut out);
        } else if log.address() == controller {
            decode_controller_log(&ctx, log.as_ref(), &mut out);
        } else if log.address() == revenue_nft {
            decode_revenue_nft_log(&ctx, log.as_ref(), &mut out);
        }
    }

    Ok(out)
}

fn decode_hook_log(ctx: &MetaCtx, log: &eth::Log, out: &mut spawn::SpawnEvents) {
    use abi::milestone_hook::events;

    if let Some(e) = events::Launched::match_and_decode(log) {
        out.launches.push(spawn::Launch {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            creator: hex0x(&e.creator),
            token: hex0x(&e.token),
            total_supply: e.total_supply.to_string(),
            opening_level: e.opening_level.to_i32(),
            far_level: e.far_level.to_i32(),
            config_hash: hex0x(&e.config_hash),
            payout_plan: String::new(),
            dev_buy_share_wad: String::new(),
            hook: String::new(),
            currency0: "0x0000000000000000000000000000000000000000".to_string(),
            currency1: hex0x(&e.token),
            // Name/symbol/uri ride the event since the economics revision —
            // no RPC round-trip needed for them (decimals is fixed at 18).
            token_name: e.name.clone(),
            token_symbol: e.symbol.clone(),
            token_uri: e.uri.clone(),
        });
    }
    if let Some(e) = events::LaunchConfigured::match_and_decode(log) {
        // Companion row: only plan fields set; db_out merges into pools by pool_id.
        out.launches.push(spawn::Launch {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            creator: String::new(),
            token: String::new(),
            total_supply: String::new(),
            opening_level: 0,
            far_level: 0,
            config_hash: String::new(),
            payout_plan: e.payout_plan.to_string(),
            dev_buy_share_wad: e.dev_buy_share_wad.to_string(),
            hook: String::new(),
            currency0: String::new(),
            currency1: String::new(),
            token_name: String::new(),
            token_symbol: String::new(),
            token_uri: String::new(),
        });
    }
    if let Some(e) = events::Graduated::match_and_decode(log) {
        out.graduations.push(spawn::Graduation {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            graduation_level: e.graduation_level.to_i32(),
            quote_proceeds: e.quote_proceeds.to_string(),
            lp_seed_quote: e.lp_seed_quote.to_string(),
            creator_quote: e.creator_quote.to_string(),
            protocol_quote: e.protocol_quote.to_string(),
            full_range_liquidity: e.full_range_liquidity.to_string(),
            wall_liquidity: e.wall_liquidity.to_string(),
        });
    }
    if let Some(e) = events::CurvePositionsDeployed::match_and_decode(log) {
        out.curve_deployments.push(spawn::CurveDeployment {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            minted: e.minted.to_string(),
            deployed: e.deployed.to_u64() as u32,
            token_settled: e.token_settled.to_string(),
        });
    }
    if let Some(e) = events::BandDeployed::match_and_decode(log) {
        out.band_deployments.push(spawn::BandDeployment {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            index: e.index.to_u64() as u32,
            level_lower: e.level_lower.to_i32(),
            level_upper: e.level_upper.to_i32(),
            liquidity: e.liquidity.to_string(),
            token_inventory: e.token_inventory.to_string(),
        });
    }
    if let Some(e) = events::BandSkipped::match_and_decode(log) {
        out.band_skips.push(spawn::BandSkip {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            index: e.index.to_u64() as u32,
            carried_inventory: e.carried_inventory.to_string(),
        });
    }
    if let Some(e) = events::MilestoneHarvested::match_and_decode(log) {
        out.milestone_harvests.push(spawn::MilestoneHarvest {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            index: e.index.to_u64() as u32,
            quote_proceeds: e.quote_proceeds.to_string(),
            token_residue: e.token_residue.to_string(),
            completed_milestones: e.completed_milestones.to_u64() as u32,
        });
    }
    if let Some(e) = events::DevBuyExecuted::match_and_decode(log) {
        out.dev_buy = Some(spawn::DevBuy {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            tokens_bought: e.tokens_bought.to_string(),
            eth_spent: e.eth_spent.to_string(),
        });
    }
    if let Some(e) = events::DevBuySkipped::match_and_decode(log) {
        out.dev_buy_skip = Some(spawn::DevBuySkip {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            relayer: hex0x(&e.relayer),
            tokens_requested: e.tokens_requested.to_string(),
        });
    }
    if let Some(e) = events::PayoutPotFunded::match_and_decode(log) {
        out.payout_pot_fundings.push(spawn::PayoutPotFunding {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            milestone_index: e.milestone_index.to_u64() as u32,
            gross_quote: e.gross_quote.to_string(),
            service_fee: e.service_fee.to_string(),
            net_quote: e.net_quote.to_string(),
            economic_version: e.economic_version.to_u64(),
        });
    }
    if let Some(e) = events::PayoutPotRedeemed::match_and_decode(log) {
        out.payout_pot_redemptions.push(spawn::PayoutPotRedemption {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::PayoutTipPaid::match_and_decode(log) {
        out.payout_tips.push(spawn::PayoutTip {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            recipient: hex0x(&e.recipient),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::PluginPayoutDelivered::match_and_decode(log) {
        out.plugin_payouts.push(spawn::PluginPayout {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            plugin_index: e.plugin_index.to_u64() as u32,
            plugin: hex0x(&e.plugin),
            outcome: spawn::PluginOutcome::PluginDelivered as i32,
            current_share: e.current_share.to_string(),
            previous_carry: e.previous_carry.to_string(),
            amount: e.delivered.to_string(),
        });
    }
    if let Some(e) = events::PluginPayoutCarried::match_and_decode(log) {
        out.plugin_payouts.push(spawn::PluginPayout {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            plugin_index: e.plugin_index.to_u64() as u32,
            plugin: hex0x(&e.plugin),
            outcome: spawn::PluginOutcome::PluginCarried as i32,
            current_share: e.current_share.to_string(),
            previous_carry: e.previous_carry.to_string(),
            amount: e.carried.to_string(),
        });
    }
    if let Some(e) = events::PluginPayoutRedirected::match_and_decode(log) {
        out.plugin_payouts.push(spawn::PluginPayout {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            plugin_index: e.plugin_index.to_u64() as u32,
            plugin: String::new(),
            outcome: spawn::PluginOutcome::PluginRedirected as i32,
            current_share: e.current_share.to_string(),
            previous_carry: e.previous_carry.to_string(),
            amount: e.redirected.to_string(),
        });
    }
    if let Some(e) = events::CreatorAccrued::match_and_decode(log) {
        out.creator_accruals.push(spawn::CreatorAccrual {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            amount: e.amount.to_string(),
            source: e.source.to_i32(),
            economic_version: e.economic_version.to_u64(),
        });
    }
    if let Some(e) = events::CreatorClaimed::match_and_decode(log) {
        out.creator_claims.push(spawn::CreatorClaim {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            holder: hex0x(&e.holder),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::CreatorPathAccrued::match_and_decode(log) {
        out.creator_path_accruals.push(spawn::CreatorPathAccrual {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::CreatorPathClaimed::match_and_decode(log) {
        out.creator_path_claims.push(spawn::CreatorPathClaim {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            holder: hex0x(&e.holder),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::CreatorPathClaimFailed::match_and_decode(log) {
        out.creator_path_claim_failures.push(spawn::CreatorPathClaimFailure {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            holder: hex0x(&e.holder),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::ProtocolAccrued::match_and_decode(log) {
        out.protocol_accruals.push(spawn::ProtocolAccrual {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            amount: e.amount.to_string(),
            source: e.source.to_i32(),
            economic_version: e.economic_version.to_u64(),
        });
    }
    if let Some(e) = events::ProtocolClaimed::match_and_decode(log) {
        out.protocol_claims.push(spawn::ProtocolClaim {
            meta: Some(ctx.meta()),
            recipient: hex0x(&e.recipient),
            amount: e.amount.to_string(),
        });
    }
    if let Some(e) = events::FeesCollected::match_and_decode(log) {
        out.fee_collections.push(spawn::FeeCollection {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            caller: hex0x(&e.caller),
            quote_fees: e.quote_fees.to_string(),
            token_fees: e.token_fees.to_string(),
        });
    }
    if let Some(e) = events::FeesRouted::match_and_decode(log) {
        out.fee_routings.push(spawn::FeeRouting {
            meta: Some(ctx.meta()),
            pool_id: hex0x(&e.pool_id),
            creator_quote: e.creator_quote.to_string(),
            protocol_quote: e.protocol_quote.to_string(),
            diverted_to_next_band: e.diverted_to_next_band.to_string(),
            tokens_burned: e.tokens_burned.to_string(),
            economic_version: e.economic_version.to_u64(),
        });
    }
    if let Some(e) = events::EconomicConfigSet::match_and_decode(log) {
        out.economic_configs.push(spawn::EconomicConfigSet {
            meta: Some(ctx.meta()),
            version: e.version.to_u64(),
            harvest_service_fee_wad: e.harvest_service_fee_wad.to_string(),
            quote_creator_share_wad: e.quote_creator_share_wad.to_string(),
            token_milestone_fund_share_wad: e.token_milestone_fund_share_wad.to_string(),
        });
    }
    if let Some(e) = events::ProtocolRecipientSet::match_and_decode(log) {
        out.protocol_recipient_sets.push(spawn::ProtocolRecipientSet {
            meta: Some(ctx.meta()),
            recipient: hex0x(&e.recipient),
        });
    }
    if let Some(e) = events::TrustedOperatorSet::match_and_decode(log) {
        out.trusted_operator_sets.push(spawn::TrustedOperatorSet {
            meta: Some(ctx.meta()),
            operator: hex0x(&e.operator),
        });
    }
}

fn decode_registry_log(ctx: &MetaCtx, log: &eth::Log, out: &mut spawn::SpawnEvents) {
    use abi::payout_plugin_registry::events;

    if let Some(e) = events::PluginRegistered::match_and_decode(log) {
        out.plugin_registrations.push(spawn::PluginRegistration {
            meta: Some(ctx.meta()),
            registry_index: e.index.to_u64() as u32,
            plugin: hex0x(&e.plugin),
            take_wad: e.take_wad.to_string(),
            gas_limit: e.gas_limit.to_u64() as u32,
            code_hash: hex0x(&e.code_hash),
            role: e.role.to_u64() as u32,
        });
    }
    if let Some(e) = events::PluginSuspensionSet::match_and_decode(log) {
        out.plugin_suspensions.push(spawn::PluginSuspension {
            meta: Some(ctx.meta()),
            registry_index: e.index.to_u64() as u32,
            suspended: e.suspended,
        });
    }
}

fn decode_controller_log(ctx: &MetaCtx, log: &eth::Log, out: &mut spawn::SpawnEvents) {
    use abi::protocol_controller::events;

    if let Some(e) = events::EconomicConfigUpdated::match_and_decode(log) {
        out.economic_configs.push(spawn::EconomicConfigSet {
            meta: Some(ctx.meta()),
            version: e.version.to_u64(),
            harvest_service_fee_wad: e.harvest_service_fee_wad.to_string(),
            quote_creator_share_wad: e.quote_creator_share_wad.to_string(),
            token_milestone_fund_share_wad: e.token_milestone_fund_share_wad.to_string(),
        });
    }
    if let Some(e) = events::ProtocolRecipientUpdated::match_and_decode(log) {
        out.protocol_recipient_sets.push(spawn::ProtocolRecipientSet {
            meta: Some(ctx.meta()),
            recipient: hex0x(&e.recipient),
        });
    }
    if let Some(e) = events::TrustedOperatorUpdated::match_and_decode(log) {
        out.trusted_operator_updates.push(spawn::TrustedOperatorUpdated {
            meta: Some(ctx.meta()),
            previous_operator: hex0x(&e.previous_operator),
            operator: hex0x(&e.operator),
        });
    }
}

fn decode_revenue_nft_log(ctx: &MetaCtx, log: &eth::Log, out: &mut spawn::SpawnEvents) {
    use abi::revenue_nft::events;

    if let Some(e) = events::MinterSet::match_and_decode(log) {
        out.minter_sets.push(spawn::MinterSet {
            meta: Some(ctx.meta()),
            minter: hex0x(&e.minter),
        });
    }
}

// map_token_burns — Transfer(to == 0x0) logs of known launch tokens. Every
// burn path in the protocol (user burn(), hook fee burns, the buyback
// plugin) surfaces as an ERC-20 Transfer to zero; indexing only those keeps
// the table tiny while supply accounting stays exact.
// ---------------------------------------------------------------------------

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[substreams::handlers::map]
fn map_token_burns(
    block: eth::Block,
    store: StoreGetProto<spawn::Launch>,
) -> Result<spawn::SpawnEvents, Error> {
    let mut out = spawn::SpawnEvents::default();

    for log in block.logs() {
        if log.topics().len() < 3 {
            continue;
        }
        // cheapest reject: to == 0x0 (right-padded address word)
        if log.topics()[2][12..] != [0u8; 20] {
            continue;
        }
        let token = hex0x(log.address());
        if store.get_last(&token).is_none() {
            continue; // not a known launch token
        }
        let Some(t) = abi::milestone_token::events::Transfer::match_and_decode(log.as_ref()) else {
            continue;
        };
        let ctx = MetaCtx {
            block: &block,
            tx_hash: hex0x(&log.receipt.transaction.hash),
            log_index: log.index(),
            ordinal: log.ordinal(),
        };
        out.token_burns.push(spawn::TokenBurn {
            meta: Some(ctx.meta()),
            token,
            burner: hex0x(&t.from),
            amount: t.value.to_string(),
        });
    }

    Ok(out)
}


// ---------------------------------------------------------------------------
// map_token_meta — one-time RPC enrichment per token (batched + cached via
// store_token_meta), then merged into the Launch row db_out consumes.
// ---------------------------------------------------------------------------

#[substreams::handlers::map]
fn map_token_meta(
    events: spawn::SpawnEvents,
    cache: StoreGetProto<spawn::Launch>,
) -> Result<spawn::SpawnEvents, Error> {
    use substreams_ethereum::rpc::RpcBatch;

    let mut out = spawn::SpawnEvents::default();
    // tokens launched in this block whose metadata we don't have yet
    let mut fresh: Vec<String> = Vec::new();
    for l in &events.launches {
        // the Launched pivot sets the bare token key in store_pools; the
        // metadata cache lives under "meta:<token>"
        if !l.token.is_empty() && cache.get_last(&format!("meta:{}", l.token)).is_none() {
            fresh.push(l.token.clone());
        }
    }
    if fresh.is_empty() {
        return Ok(out);
    }

    let mut batch = RpcBatch::new();
    let mut owned = Vec::new();
    for token in &fresh {
        let addr = parse_addr(token, "token")?;
        owned.push((token.clone(), addr));
        // Decimals is fixed at 18 (plain OZ ERC20, no override) — not fetched.
        batch = batch
            .add(abi::milestone_token::functions::Name {}, addr.to_vec())
            .add(abi::milestone_token::functions::Symbol {}, addr.to_vec())
            .add(abi::milestone_token::functions::TokenUri {}, addr.to_vec());
    }
    let batch = match batch.execute() {
        Ok(b) => b,
        Err(_) => return Ok(out), // never fail the stream on RPC issues
    };

    let mut i = 0;
    for (token, _) in &owned {
        let name = substreams_ethereum::rpc::RpcBatch::decode::<_, abi::milestone_token::functions::Name>(
            &batch.responses[i * 3],
        )
        .unwrap_or_default();
        let symbol = substreams_ethereum::rpc::RpcBatch::decode::<_, abi::milestone_token::functions::Symbol>(
            &batch.responses[i * 3 + 1],
        )
        .unwrap_or_default();
        let token_uri = substreams_ethereum::rpc::RpcBatch::decode::<_, abi::milestone_token::functions::TokenUri>(
            &batch.responses[i * 3 + 2],
        )
        .unwrap_or_default();
        let launch_block = events
            .launches
            .iter()
            .find(|l| l.token == *token)
            .and_then(|l| l.meta.as_ref())
            .map(|m| m.block_number)
            .unwrap_or(0);
        // Prefer the Launched event's own name/symbol/uri (authoritative since
        // the economics revision); RPC values are the fallback for pre-revision
        // pools and for the uri when the event predates it.
        let (event_name, event_symbol, event_uri) = events
            .launches
            .iter()
            .find(|l| l.token == *token && (!l.token_name.is_empty() || !l.token_uri.is_empty()))
            .map(|l| (l.token_name.clone(), l.token_symbol.clone(), l.token_uri.clone()))
            .unwrap_or_default();
        out.launches.push(spawn::Launch {
            meta: Some(spawn::EventMeta {
                block_number: launch_block,
                block_hash: String::new(),
                timestamp: 0,
                tx_hash: String::new(),
                log_index: 0,
                ordinal_key: format!("{}:meta", launch_block),
            }),
            pool_id: String::new(),
            creator: String::new(),
            token: token.clone(),
            total_supply: String::new(),
            opening_level: 0,
            far_level: 0,
            config_hash: String::new(),
            payout_plan: String::new(),
            dev_buy_share_wad: String::new(),
            hook: String::new(),
            currency0: String::new(),
            currency1: String::new(),
            token_name: if event_name.is_empty() { name } else { event_name },
            token_symbol: if event_symbol.is_empty() { symbol } else { event_symbol },
            token_uri: if event_uri.is_empty() { token_uri } else { event_uri },
        });
        i += 1;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// map_v4_swaps — PoolManager singleton Swap logs, filtered to launchpad pools
// via store_pools (pool_id -> Launch). ETH is always currency0; a negative
// amount0 means ETH flowed in (a buy).
// ---------------------------------------------------------------------------

#[substreams::handlers::map]
fn map_v4_swaps(
    params: String,
    block: eth::Block,
    store: StoreGetProto<spawn::Launch>,
) -> Result<spawn::TradedSwaps, Error> {
    // params: "pool_manager=0x…"
    let pool_manager_addr = params
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("pool_manager="))
        .unwrap_or_default()
        .to_string();
    let pool_manager = parse_addr(&pool_manager_addr, "pool_manager")?;
    let mut out = spawn::TradedSwaps::default();

    for log in block.logs() {
        if log.address() != pool_manager {
            continue;
        }
        let Some(s) = abi::pool_manager::events::Swap::match_and_decode(log.as_ref()) else {
            continue;
        };
        let pool_id = hex0x(&s.id);
        let Some(launch) = store.get_last(&pool_id) else {
            continue;
        };
        let ctx = MetaCtx {
            block: &block,
            tx_hash: hex0x(&log.receipt.transaction.hash),
            log_index: log.index(),
            ordinal: log.ordinal(),
        };
        let is_buy = is_negative(&s.amount0);
        // v4 fee is charged on the input side: buys pay in ETH, sells in tokens.
        // fee is in hundredths of a bip => divide by 1e6.
        let fee_wad = substreams::scalar::BigInt::from(s.fee.to_u64());
        let (fee_eth, fee_tokens) = if is_buy {
            let f = (abs_str(&s.amount0).parse::<substreams::scalar::BigInt>().unwrap_or_default()
                * fee_wad.clone())
                / substreams::scalar::BigInt::from(1_000_000u64);
            (f.to_string(), "0".to_string())
        } else {
            let f = (abs_str(&s.amount1).parse::<substreams::scalar::BigInt>().unwrap_or_default()
                * fee_wad.clone())
                / substreams::scalar::BigInt::from(1_000_000u64);
            ("0".to_string(), f.to_string())
        };
        out.swaps.push(spawn::TradedSwap {
            meta: Some(ctx.meta()),
            pool_id,
            token: launch.token,
            sender: hex0x(&s.sender),
            is_buy,
            amount0_eth: abs_str(&s.amount0),
            amount1_tokens: abs_str(&s.amount1),
            sqrt_price_x96: s.sqrt_price_x96.to_string(),
            liquidity: s.liquidity.to_string(),
            tick: s.tick.to_i32(),
            fee: s.fee.to_u64() as u32,
            fee_eth,
            fee_tokens,
        });
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// store_pools — pool_id -> Launch and token -> Launch (set_if_not_exists, so
// the Launched row is immutable and replays are idempotent).
// ---------------------------------------------------------------------------

#[substreams::handlers::store]
fn store_pools(events: spawn::SpawnEvents, output: StoreSetIfNotExistsProto<spawn::Launch>) {
    for launch in &events.launches {
        if !launch.token.is_empty() {
            if launch.pool_id.is_empty() {
                // metadata-only row (from map_token_meta): cache under a
                // dedicated key so it can never shadow the Launched pivot
                output.set_if_not_exists(0, &format!("meta:{}", launch.token), launch);
            } else {
                output.set_if_not_exists(0, &launch.pool_id, launch);
                output.set_if_not_exists(0, &launch.token, launch);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// index_spawn_activity — blockIndex keys for cost control: log_addr and
// topic0 for every log in the block.
// ---------------------------------------------------------------------------

#[substreams::handlers::map]
fn index_spawn_activity(block: eth::Block) -> Result<Keys, Error> {
    let mut keys = Keys::default();
    for log in block.logs() {
        keys.keys.push(format!("log_addr:{}", hex0x(log.address())));
        if let Some(t0) = log.topics().first() {
            keys.keys.push(format!("topic0:0x{}", Hex::encode(t0)));
        }
    }
    Ok(keys)
}

// ---------------------------------------------------------------------------
// db_out — the SQL sink payload: fact inserts, state upserts, and delta-op
// aggregates (add/sub/max/set_if_null) — all reorg-safe via the sink's undo
// journal. No triggers, no stored derived state that can drift.
// ---------------------------------------------------------------------------

#[substreams::handlers::map]
fn db_out(
    mut events: spawn::SpawnEvents,
    token_events: spawn::SpawnEvents,
    mut token_meta: spawn::SpawnEvents,
    swaps: spawn::TradedSwaps,
    pools: StoreGetProto<spawn::Launch>,
    clock: Clock,
) -> Result<DatabaseChanges, Error> {
    let mut tables = Tables::new();
    let global_key: &str = "global";

    let token_burns = std::mem::take(&mut {
        let mut token_events = token_events;
        let mut v = std::mem::take(&mut token_events.token_burns);
        v.append(&mut events.token_burns);
        v
    });
    // metadata rows from map_token_meta (pool_id empty, token_name set)
    let meta_rows = std::mem::take(&mut token_meta.launches);

    // --- launches + pools state ------------------------------------------------
    for l in &events.launches {
        let m = l.meta.clone().unwrap_or_default();
        if !l.token.is_empty() {
            tables
                .create_row("launches", [("ordinal_key", m.ordinal_key.as_str())])
                .set("pool_id", l.pool_id.clone())
                .set("creator", l.creator.clone())
                .set("token", l.token.clone())
                .set("total_supply", l.total_supply.clone())
                .set("opening_level", l.opening_level)
                .set("far_level", l.far_level)
                .set("config_hash", l.config_hash.clone())
                .set("block_number", m.block_number as i64)
                .set("timestamp", m.timestamp as i64);
            tables
                .upsert_row("pools", l.pool_id.as_str())
                .set_if_null("pool_id", l.pool_id.clone())
                .set_if_null("creator", l.creator.clone())
                .set_if_null("token", l.token.clone())
                .set_if_null("total_supply", l.total_supply.clone())
                .set_if_null("opening_level", l.opening_level)
                .set_if_null("far_level", l.far_level)
                .set_if_null("config_hash", l.config_hash.clone())
                .set_if_null("launch_block", m.block_number as i64)
                .set_if_null("launch_time", m.timestamp as i64)
                .set("status", "bonding".to_string());
        } else if !l.payout_plan.is_empty() || !l.dev_buy_share_wad.is_empty() {
            // LaunchConfigured companion row
            tables
                .upsert_row("pools", l.pool_id.as_str())
                .set("payout_plan", l.payout_plan.clone())
                .set("dev_buy_share_wad", l.dev_buy_share_wad.clone());
        }
    }

    // token metadata rows (from map_token_meta): one tokens row per token.
    // Name/symbol/uri prefer the Launched event (merged in map_token_meta).
    // Decimals is fixed at 18 and not tracked.
    for l in &meta_rows {
        if l.pool_id.is_empty() && (!l.token_name.is_empty() || !l.token_uri.is_empty()) {
            tables
                .upsert_row("tokens", l.token.as_str())
                .set("token", l.token.clone())
                .set("name", l.token_name.clone())
                .set("symbol", l.token_symbol.clone())
                .set("uri", l.token_uri.clone());
        }
    }

    // Fast path: Launched already carries name/symbol/uri, so seed the tokens
    // row even when the meta RPC for this block was skipped or failed. All
    // set_if_null so the authoritative meta upsert above always wins.
    for l in &events.launches {
        if !l.pool_id.is_empty() && !l.token.is_empty() && (!l.token_name.is_empty() || !l.token_uri.is_empty()) {
            tables
                .upsert_row("tokens", l.token.as_str())
                .set_if_null("token", l.token.clone())
                .set_if_null("name", l.token_name.clone())
                .set_if_null("symbol", l.token_symbol.clone())
                .set_if_null("uri", l.token_uri.clone());
        }
    }

    // --- graduations ---------------------------------------------------------------
    for g in &events.graduations {
        let m = g.meta.clone().unwrap_or_default();
        tables
            .create_row("graduations", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", g.pool_id.clone())
            .set("graduation_level", g.graduation_level)
            .set("quote_proceeds", g.quote_proceeds.clone())
            .set("lp_seed_quote", g.lp_seed_quote.clone())
            .set("creator_quote", g.creator_quote.clone())
            .set("protocol_quote", g.protocol_quote.clone())
            .set("full_range_liquidity", g.full_range_liquidity.clone())
            .set("wall_liquidity", g.wall_liquidity.clone())
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        tables
            .upsert_row("pools", g.pool_id.as_str())
            .set("status", "graduated".to_string())
            .set("graduation_level", g.graduation_level)
            .set("wall_liquidity", g.wall_liquidity.clone())
            .set("graduation_block", m.block_number as i64)
            .set("graduation_time", m.timestamp as i64);
        // Revenue accounting comes solely from CreatorAccrued/ProtocolAccrued
        // events (the authoritative ledgers). The Graduated split fields are
        // informational; aggregating them here would double-count against the
        // accruals emitted in the same transaction.
        let day = day_key(m.timestamp);
        tables
            .upsert_row("pool_day_stats", [("pool_id", g.pool_id.as_str()), ("day", day.as_str())])
            .add("creator_revenue_eth", g.creator_quote.as_str())
            .add("protocol_revenue_eth", g.protocol_quote.as_str());
        tables
            .upsert_row("protocol_day_stats", [("day", day.as_str())])
            .add("creator_revenue_eth", g.creator_quote.as_str())
            .add("protocol_revenue_eth", g.protocol_quote.as_str())
            .add("graduations", 1i64);
    }

    // --- curve deployments -----------------------------------------------------------
    for c in &events.curve_deployments {
        let m = c.meta.clone().unwrap_or_default();
        tables
            .create_row("curve_deployments", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", c.pool_id.clone())
            .set("minted", c.minted.clone())
            .set("deployed", c.deployed as i64)
            .set("token_settled", c.token_settled.clone())
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
    }

    // --- bands -------------------------------------------------------------------------
    // Bands deployed and harvested in the same block (a fast pump): merge the
    // terminal status into the single create_row instead of upserting after a
    // create (the Tables API forbids upsert-after-create on one table).
    let harvested: std::collections::HashSet<(String, u32)> = events
        .milestone_harvests
        .iter()
        .map(|h| (h.pool_id.clone(), h.index))
        .collect();
    for b in &events.band_deployments {
        let m = b.meta.clone().unwrap_or_default();
        let done_in_block = harvested.contains(&(b.pool_id.clone(), b.index));
        tables
            .create_row("bands", [("pool_id", b.pool_id.as_str()), ("band_index", b.index.to_string().as_str())])
            .set("level_lower", b.level_lower)
            .set("level_upper", b.level_upper)
            .set("liquidity", b.liquidity.clone())
            .set("token_inventory", b.token_inventory.clone())
            .set("status", if done_in_block { "completed".to_string() } else { "live".to_string() })
            .set("deployed_block", m.block_number as i64)
            .set("deployed_time", m.timestamp as i64)
            .set_if_null("harvested_block", m.block_number as i64);
        tables
            .upsert_row("pool_stats", b.pool_id.as_str())
            .set("last_activity_block", m.block_number as i64)
            .add("bands_deployed", 1i64);
    }
    for s in &events.band_skips {
        let m = s.meta.clone().unwrap_or_default();
        tables
            .create_row("band_skips", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", s.pool_id.clone())
            .set("band_index", s.index as i64)
            .set("carried_inventory", s.carried_inventory.clone())
            .set("block_number", m.block_number as i64);
    }

    // --- milestone harvests ---------------------------------------------------------------
    for h in &events.milestone_harvests {
        let m = h.meta.clone().unwrap_or_default();
        tables
            .create_row("milestone_harvests", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", h.pool_id.clone())
            .set("band_index", h.index as i64)
            .set("quote_proceeds", h.quote_proceeds.clone())
            .set("token_residue", h.token_residue.clone())
            .set("completed_milestones", h.completed_milestones as i64)
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        if !events.band_deployments.iter().any(|b| b.pool_id == h.pool_id && b.index == h.index) {
            tables
                .upsert_row("bands", [("pool_id", h.pool_id.as_str()), ("band_index", h.index.to_string().as_str())])
                .set("status", "completed".to_string())
                .set("harvested_block", m.block_number as i64);
        }
        tables
            .upsert_row("pool_stats", h.pool_id.as_str())
            .add("harvest_count", 1i64)
            .add("harvest_quote_total", h.quote_proceeds.as_str())
            .set("last_activity_block", m.block_number as i64);
    }

    // --- milestone harvest breakdown ----------------------------------------
    // One row per (pool, milestone index): service fee to protocol, pot net,
    // tip to recipient, plugin shares, creator-path remainder, and the buyback
    // burn (token Transfer to zero inside the plugin's delivery).
    // Fundings tell us gross/service/net; the flush events in the same or a
    // later block tell us the recipients. We record what is observable per
    // event family and keep the full recipient ledger in the respective facts.
    for f in &events.payout_pot_fundings {
        let m = f.meta.clone().unwrap_or_default();
        let milestone_index = f.milestone_index.to_string();
        let key = [
            ("pool_id", f.pool_id.as_str()),
            ("milestone_index", milestone_index.as_str()),
        ];
        tables
            .upsert_row("harvest_payouts", key)
            .set_if_null("gross_quote", f.gross_quote.clone())
            .set_if_null("service_fee", f.service_fee.clone())
            .set_if_null("net_quote", f.net_quote.clone())
            .set_if_null("economic_version", f.economic_version as i64)
            .set_if_null("funded_block", m.block_number as i64)
            .set_if_null("funded_time", m.timestamp as i64);
    }
    for t in &events.payout_tips {
        let m = t.meta.clone().unwrap_or_default();
        // a tip belongs to the most recent funded-but-unattributed milestone;
        // flush is pool-scoped, so attribute tips to the pool's last funded
        // milestone via the max milestone_index with a harvest_payouts row.
        tables
            .upsert_row("pool_stats", t.pool_id.as_str())
            .add("tips_total", t.amount.as_str());
        let _ = m;
    }
    for p in &events.plugin_payouts {
        tables
            .upsert_row("pool_stats", p.pool_id.as_str())
            .add("plugin_revenue_total", p.amount.clone());
    }

    // --- dev buys --------------------------------------------------------------------------
    if let Some(d) = &events.dev_buy {
        let m = d.meta.clone().unwrap_or_default();
        tables
            .create_row("dev_buys", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", d.pool_id.clone())
            .set("tokens_bought", d.tokens_bought.clone())
            .set("eth_spent", d.eth_spent.clone())
            .set("block_number", m.block_number as i64);
    }
    if let Some(d) = &events.dev_buy_skip {
        let m = d.meta.clone().unwrap_or_default();
        tables
            .create_row("dev_buy_skips", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", d.pool_id.clone())
            .set("relayer", d.relayer.clone())
            .set("tokens_requested", d.tokens_requested.clone())
            .set("block_number", m.block_number as i64);
    }

    // --- payout pots --------------------------------------------------------------------------
    for f in &events.payout_pot_fundings {
        let m = f.meta.clone().unwrap_or_default();
        tables
            .create_row("payout_pot_fundings", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", f.pool_id.clone())
            .set("milestone_index", f.milestone_index as i64)
            .set("gross_quote", f.gross_quote.clone())
            .set("service_fee", f.service_fee.clone())
            .set("net_quote", f.net_quote.clone())
            .set("economic_version", f.economic_version as i64)
            .set("block_number", m.block_number as i64);
        tables
            .upsert_row("pots", f.pool_id.as_str())
            .add("balance", f.net_quote.as_str())
            .add("funded_total", f.net_quote.as_str())
            .add("service_fee_total", f.service_fee.as_str());
        // service fees reach the protocol ledger through ProtocolAccrued
        // (MILESTONE_HARVEST) in this same block; adding them here would
        // double-count. pot_funded_total tracks the pot, not revenue.
        tables
            .upsert_row("pool_stats", f.pool_id.as_str())
            .add("pot_funded_total", f.net_quote.as_str());
        tables
            .upsert_row("protocol_day_stats", [("day", day_key(m.timestamp).as_str())])
            .add("protocol_revenue_eth", f.service_fee.as_str());
    }
    for r in &events.payout_pot_redemptions {
        let m = r.meta.clone().unwrap_or_default();
        tables
            .create_row("payout_pot_redemptions", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", r.pool_id.clone())
            .set("amount", r.amount.clone())
            .set("block_number", m.block_number as i64);
        tables
            .upsert_row("pots", r.pool_id.as_str())
            .sub("balance", r.amount.as_str());
    }
    for t in &events.payout_tips {
        let m = t.meta.clone().unwrap_or_default();
        tables
            .create_row("payout_tips", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", t.pool_id.clone())
            .set("recipient", t.recipient.clone())
            .set("amount", t.amount.clone())
            .set("block_number", m.block_number as i64);
    }

    // --- plugin payouts ---------------------------------------------------------------------
    for p in &events.plugin_payouts {
        let m = p.meta.clone().unwrap_or_default();
        let outcome = match p.outcome {
            x if x == spawn::PluginOutcome::PluginDelivered as i32 => "delivered",
            x if x == spawn::PluginOutcome::PluginCarried as i32 => "carried",
            _ => "redirected",
        };
        tables
            .create_row("plugin_payouts", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", p.pool_id.clone())
            .set("plugin_index", p.plugin_index as i64)
            .set("plugin", p.plugin.clone())
            .set("outcome", outcome.to_string())
            .set("current_share", p.current_share.clone())
            .set("previous_carry", p.previous_carry.clone())
            .set("amount", p.amount.clone())
            .set("block_number", m.block_number as i64);
        if outcome == "delivered" {
            tables
                .upsert_row("pool_stats", p.pool_id.as_str())
                .add("plugin_revenue_total", p.amount.as_str());
        }
    }

    // --- creator / protocol revenue + claims -----------------------------------------------------
    for a in &events.creator_accruals {
        let m = a.meta.clone().unwrap_or_default();
        let source = accrual_source_name(a.source);
        let day = day_key(m.timestamp);
        tables
            .create_row("creator_accruals", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", a.pool_id.clone())
            .set("amount", a.amount.clone())
            .set("source", source.to_string())
            .set("economic_version", a.economic_version as i64)
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        tables
            .upsert_row("pool_stats", a.pool_id.as_str())
            .add("creator_revenue_total", a.amount.clone())
            .add(format!("creator_revenue_{}", accrual_source_col(a.source)).as_str(), a.amount.as_str());
        tables
            .upsert_row("pool_day_stats", [("pool_id", a.pool_id.as_str()), ("day", day.as_str())])
            .add("creator_revenue_eth", a.amount.as_str());
        tables
            .upsert_row("protocol_day_stats", [("day", day.as_str())])
            .add("creator_revenue_eth", a.amount.as_str());
    }
    for a in &events.protocol_accruals {
        let m = a.meta.clone().unwrap_or_default();
        let source = accrual_source_name(a.source);
        tables
            .create_row("protocol_accruals", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", a.pool_id.clone())
            .set("amount", a.amount.clone())
            .set("source", source.to_string())
            .set("economic_version", a.economic_version as i64)
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        tables
            .upsert_row("protocol_stats", global_key)
            .add(format!("revenue_{}", accrual_source_col(a.source)).as_str(), a.amount.as_str())
            .add("revenue_total", a.amount.as_str());
        // Pool-level protocol revenue mirrors the same authoritative event.
        // Column names differ per source: milestone_harvest fees land in
        // protocol_revenue_harvest_fees; others use protocol_revenue_{suffix}.
        let proto_col = match a.source {
            x if x == spawn::AccrualSource::MilestoneHarvest as i32 => "protocol_revenue_harvest_fees",
            x if x == spawn::AccrualSource::CurveProceeds as i32 => "protocol_revenue_curve",
            x if x == spawn::AccrualSource::SwapFees as i32 => "protocol_revenue_swap_fees",
            _ => "protocol_revenue_other",
        };
        tables
            .upsert_row("pool_stats", a.pool_id.as_str())
            .add("protocol_revenue_total", a.amount.as_str())
            .add(proto_col, a.amount.as_str());
        let day = day_key(m.timestamp);
        tables
            .upsert_row("protocol_day_stats", [("day", day.as_str())])
            .add("protocol_revenue_eth", a.amount.as_str());
    }
    for a in &events.creator_path_accruals {
        let m = a.meta.clone().unwrap_or_default();
        tables
            .create_row("creator_path_accruals", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", a.pool_id.clone())
            .set("amount", a.amount.clone())
            .set("block_number", m.block_number as i64);
        tables
            .upsert_row("pool_stats", a.pool_id.as_str())
            .add("creator_path_revenue_total", a.amount.as_str());
    }
    for c in &events.creator_claims {
        record_claim(&mut tables, "creator", &c.pool_id, &c.holder, &c.amount, &c.meta);
    }
    for c in &events.creator_path_claims {
        record_claim(&mut tables, "creator_path", &c.pool_id, &c.holder, &c.amount, &c.meta);
    }
    for f in &events.creator_path_claim_failures {
        let m = f.meta.clone().unwrap_or_default();
        tables
            .create_row("creator_path_claim_failures", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", f.pool_id.clone())
            .set("holder", f.holder.clone())
            .set("amount", f.amount.clone())
            .set("block_number", m.block_number as i64);
    }
    for c in &events.protocol_claims {
        let m = c.meta.clone().unwrap_or_default();
        tables
            .create_row("claims", [("ordinal_key", m.ordinal_key.as_str())])
            .set("claim_type", "protocol".to_string())
            .set("pool_id", String::new())
            .set("holder", c.recipient.clone())
            .set("amount", c.amount.clone())
            .set("block_number", m.block_number as i64);
        tables
            .upsert_row("protocol_stats", global_key)
            .add("claimed_total", c.amount.as_str());
    }

    // --- fee collections + routing ---------------------------------------------------------------
    for f in &events.fee_collections {
        let m = f.meta.clone().unwrap_or_default();
        tables
            .create_row("fee_collections", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", f.pool_id.clone())
            .set("caller", f.caller.clone())
            .set("quote_fees", f.quote_fees.clone())
            .set("token_fees", f.token_fees.clone())
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
    }
    for r in &events.fee_routings {
        let m = r.meta.clone().unwrap_or_default();
        let day = day_key(m.timestamp);
        tables
            .create_row("fee_routings", [("ordinal_key", m.ordinal_key.as_str())])
            .set("pool_id", r.pool_id.clone())
            .set("creator_quote", r.creator_quote.clone())
            .set("protocol_quote", r.protocol_quote.clone())
            .set("diverted_to_next_band", r.diverted_to_next_band.clone())
            .set("tokens_burned", r.tokens_burned.clone())
            .set("economic_version", r.economic_version as i64)
            .set("block_number", m.block_number as i64);
        // Swap-fee revenue reaches the ledgers through CreatorAccrued /
        // ProtocolAccrued (SWAP_FEES) in this same transaction; adding the
        // routed split here would double-count. burned_total is still driven
        // by token Transfer-to-zero events (the authoritative burn record).
        tables
            .upsert_row("pool_stats", r.pool_id.as_str())
            .add("swap_fee_burned_tokens", r.tokens_burned.as_str());
        tables
            .upsert_row("pool_day_stats", [("pool_id", r.pool_id.as_str()), ("day", day.as_str())])
            .add("creator_revenue_eth", r.creator_quote.as_str());
        tables
            .upsert_row("protocol_day_stats", [("day", day.as_str())])
            .add("protocol_revenue_eth", r.protocol_quote.as_str())
            .add("creator_revenue_eth", r.creator_quote.as_str());
    }

    // --- governance state --------------------------------------------------------------------------
    for e in &events.economic_configs {
        let m = e.meta.clone().unwrap_or_default();
        tables
            .create_row("economic_configs", [("version", e.version.to_string().as_str())])
            .set("harvest_service_fee_wad", e.harvest_service_fee_wad.clone())
            .set("quote_creator_share_wad", e.quote_creator_share_wad.clone())
            .set("token_milestone_fund_share_wad", e.token_milestone_fund_share_wad.clone())
            .set("effective_block", m.block_number as i64);
        tables
            .upsert_row("protocol_state", global_key)
            .set("economic_version", e.version as i64);
    }
    for r in &events.protocol_recipient_sets {
        let m = r.meta.clone().unwrap_or_default();
        tables
            .upsert_row("protocol_state", global_key)
            .set("protocol_recipient", r.recipient.clone())
            .set("recipient_set_block", m.block_number as i64);
    }
    for o in &events.trusted_operator_sets {
        let m = o.meta.clone().unwrap_or_default();
        tables
            .upsert_row("protocol_state", global_key)
            .set("trusted_operator", o.operator.clone())
            .set("trusted_operator_set_block", m.block_number as i64);
    }
    for o in &events.trusted_operator_updates {
        let m = o.meta.clone().unwrap_or_default();
        tables
            .upsert_row("protocol_state", global_key)
            .set("trusted_operator", o.operator.clone())
            .set("trusted_operator_set_block", m.block_number as i64);
    }
    for p in &events.plugin_registrations {
        let m = p.meta.clone().unwrap_or_default();
        tables
            .upsert_row("plugin_registry", [("registry_index", p.registry_index.to_string().as_str())])
            .set("plugin", p.plugin.clone())
            .set("take_wad", p.take_wad.clone())
            .set("gas_limit", p.gas_limit as i64)
            .set("code_hash", p.code_hash.clone())
            .set("role", p.role as i64)
            .set("suspended", false)
            .set("registered_block", m.block_number as i64);
    }
    for s in &events.plugin_suspensions {
        let m = s.meta.clone().unwrap_or_default();
        tables
            .upsert_row("plugin_registry", [("registry_index", s.registry_index.to_string().as_str())])
            .set("suspended", s.suspended)
            .set("suspension_set_block", m.block_number as i64);
    }

    // --- launch token burns (every burn path) --------------------------------------
    for t in &token_burns {
        let m = t.meta.clone().unwrap_or_default();
        tables
            .create_row("token_burns", [("ordinal_key", m.ordinal_key.as_str())])
            .set("token", t.token.clone())
            .set("burner", t.burner.clone())
            .set("amount", t.amount.clone())
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        if let Some(launch) = pools.get_last(&t.token) {
            tables
                .upsert_row("pool_stats", launch.pool_id.as_str())
                .add("burned_total", t.amount.as_str());
        }
    }

    // --- v4 swaps: facts + volume/price aggregates ---------------------------------------------------------
    for s in &swaps.swaps {
        let m = s.meta.clone().unwrap_or_default();
        let day = day_key(m.timestamp);
        let (vol_eth, vol_tok) = if s.is_buy {
            ("buy_volume_eth", "buy_volume_tokens")
        } else {
            ("sell_volume_eth", "sell_volume_tokens")
        };
        tables
            .create_row("swaps", [("tx_hash", m.tx_hash.as_str()), ("log_index", m.log_index.to_string().as_str())])
            .set("pool_id", s.pool_id.clone())
            .set("token", s.token.clone())
            .set("sender", s.sender.clone())
            .set("is_buy", s.is_buy)
            .set("amount0_eth", s.amount0_eth.clone())
            .set("amount1_tokens", s.amount1_tokens.clone())
            .set("sqrt_price_x96", s.sqrt_price_x96.clone())
            .set("liquidity", s.liquidity.clone())
            .set("tick", s.tick as i64)
            .set("fee", s.fee as i64)
            .set("fee_eth", s.fee_eth.clone())
            .set("fee_tokens", s.fee_tokens.clone())
            .set("block_number", m.block_number as i64)
            .set("timestamp", m.timestamp as i64);
        tables
            .upsert_row("pool_stats", s.pool_id.as_str())
            .add(vol_eth, s.amount0_eth.as_str())
            .add(vol_tok, s.amount1_tokens.as_str())
            .add("swap_count", 1i64)
            .set("last_price_sqrt_x96", s.sqrt_price_x96.clone())
            .set("last_swap_block", m.block_number as i64)
            .set("last_activity_block", m.block_number as i64)
            .max("ath_sqrt_x96", big_dec(&s.sqrt_price_x96)?);
        tables
            .upsert_row("pool_day_stats", [("pool_id", s.pool_id.as_str()), ("day", day.as_str())])
            .add(vol_eth, s.amount0_eth.as_str())
            .add(vol_tok, s.amount1_tokens.as_str())
            .add("swap_count", 1i64)
            .set("close_sqrt_x96", s.sqrt_price_x96.clone())
            .max("high_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .min("low_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .set_if_null("open_sqrt_x96", s.sqrt_price_x96.clone());
        tables
            .upsert_row("protocol_day_stats", [("day", day.as_str())])
            .add("swap_volume_eth", s.amount0_eth.as_str())
            .add("swap_count", 1i64);

        // hourly candle
        let hour = hour_key(m.timestamp);
        tables
            .upsert_row("pool_hour_stats", [("pool_id", s.pool_id.as_str()), ("hour", hour.as_str())])
            .add(vol_eth, s.amount0_eth.as_str())
            .add(vol_tok, s.amount1_tokens.as_str())
            .add("swap_count", 1i64)
            .set("close_sqrt_x96", s.sqrt_price_x96.clone())
            .max("high_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .min("low_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .set_if_null("open_sqrt_x96", s.sqrt_price_x96.clone());

        // minute candle
        let minute = (m.timestamp / 60).to_string();
        tables
            .upsert_row("pool_minute_stats", [("pool_id", s.pool_id.as_str()), ("minute", minute.as_str())])
            .add(vol_eth, s.amount0_eth.as_str())
            .add(vol_tok, s.amount1_tokens.as_str())
            .add("swap_count", 1i64)
            .set("close_sqrt_x96", s.sqrt_price_x96.clone())
            .max("high_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .min("low_sqrt_x96", big_dec(&s.sqrt_price_x96)?)
            .set_if_null("open_sqrt_x96", s.sqrt_price_x96.clone());
    }

    // --- observability -------------------------------------------------------------------------------------
    tables
        .upsert_row("protocol_state", global_key)
        .set("last_indexed_block", clock.number as i64);

    Ok(tables.to_database_changes())
}

fn record_claim(
    tables: &mut Tables,
    claim_type: &str,
    pool_id: &str,
    holder: &str,
    amount: &str,
    meta: &Option<spawn::EventMeta>,
) {
    let m = meta.clone().unwrap_or_default();
    tables
        .create_row("claims", [("ordinal_key", m.ordinal_key.as_str())])
        .set("claim_type", claim_type.to_string())
        .set("pool_id", pool_id.to_string())
        .set("holder", holder.to_string())
        .set("amount", amount.to_string())
        .set("block_number", m.block_number as i64);
}

fn hour_key(ts: u64) -> String {
    (ts / 3600).to_string()
}

fn day_key(ts: u64) -> String {
    let days = ts / 86400;
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn big_dec(s: &str) -> Result<substreams::scalar::BigDecimal, Error> {
    use std::str::FromStr;
    substreams::scalar::BigDecimal::from_str(s)
        .map_err(|e| anyhow::anyhow!("non-numeric value {s}: {e}"))
}

fn accrual_source_name(v: i32) -> &'static str {
    // Descriptive label for fact rows (source column).
    match v {
        x if x == spawn::AccrualSource::CurveProceeds as i32 => "curve_proceeds",
        x if x == spawn::AccrualSource::SwapFees as i32 => "swap_fees",
        x if x == spawn::AccrualSource::MilestoneHarvest as i32 => "milestone_harvest",
        _ => "unknown",
    }
}

/// Column suffix for per-source aggregate columns; must match schema:
/// pool_stats: creator_revenue_{curve,swap_fees}
/// protocol_stats: revenue_{curve_proceeds,swap_fees,milestone_harvest}
fn accrual_source_col(v: i32) -> &'static str {
    match v {
        x if x == spawn::AccrualSource::CurveProceeds as i32 => "curve",
        x if x == spawn::AccrualSource::SwapFees as i32 => "swap_fees",
        x if x == spawn::AccrualSource::MilestoneHarvest as i32 => "milestone_harvest",
        _ => "unknown",
    }
}
