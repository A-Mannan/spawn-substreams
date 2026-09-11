fn main() {
    let abis = [
        ("MilestoneHook", "milestone_hook"),
        ("PayoutPluginRegistry", "payout_plugin_registry"),
        ("ProtocolController", "protocol_controller"),
        ("RevenueNFT", "revenue_nft"),
        ("MilestoneToken", "milestone_token"),
        ("PoolManager", "pool_manager"),
    ];
    for (contract, module) in abis {
        let abi_path = format!("abi/{contract}.json");
        substreams_ethereum::Abigen::new(contract, abi_path.as_str())
            .unwrap_or_else(|e| panic!("failed to load ABI {contract}: {e}"))
            .generate()
            .unwrap_or_else(|e| panic!("failed to generate bindings for {contract}: {e}"))
            .write_to_file(format!("src/abi/{module}.rs"))
            .unwrap_or_else(|e| panic!("failed to write bindings for {contract}: {e}"));
    }

    prost_build::compile_protos(&["proto/spawn.proto"], &["proto/"]).unwrap();
}
