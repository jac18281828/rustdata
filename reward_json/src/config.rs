use serde::Deserialize;

/// The configuration for the rewards claimer.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// The URL of the Ethereum RPC endpoint.
    #[serde(rename = "rpcUrl", default = "default_rpc_url")]
    pub rpc_url: String,
    /// The address of the rewards coordinator contract.
    #[serde(rename = "rewardsCoordinator", default = "default_rewards_coordinator")]
    pub rewards_coordinator: String,
    /// The earliest block to query for events.
    #[serde(rename = "earliestBlock", default = "default_earliest_block")]
    pub earliest_block: u64,
    /// The maximum number of blocks to query in a single request.
    #[serde(rename = "maxBlocks", default = "default_max_blocks")]
    pub max_blocks: u64,
    /// The sleep duration for event fetch in milliseconds.
    #[serde(rename = "sleepDuration", default = "default_sleep_duration")]
    pub sleep_duration_ms: u64,
}

fn default_rpc_url() -> String {
    "https://ethereum-holesky-rpc.publicnode.com".to_string()
}

fn default_rewards_coordinator() -> String {
    "0xAcc1fb458a1317E886dB376Fc8141540537E68fE".to_string()
}

fn default_earliest_block() -> u64 {
    0
}

fn default_max_blocks() -> u64 {
    100
}

fn default_sleep_duration() -> u64 {
    1000
}

pub fn load_config() -> eyre::Result<Config> {
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.json".to_string());
    let config = std::fs::read_to_string(config_path)?;
    let config: Config = serde_json::from_str(&config)?;
    tracing::info!("Loaded config: {:?}", config);
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserialization() {
        let config = r#"{
            "rpcUrl": "https://ethereum.org",
            "rewardsCoordinator": "0x1234567890123456789012345678901234567890",
            "earliestBlock": 123,
            "maxBlocks": 101,
            "sleepDuration": 777
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(config.rpc_url, "https://ethereum.org");
        assert_eq!(
            config.rewards_coordinator,
            "0x1234567890123456789012345678901234567890"
        );
        assert_eq!(config.earliest_block, 123);
        assert_eq!(config.max_blocks, 101);
        assert_eq!(config.sleep_duration_ms, 777);
    }

    #[test]
    fn test_default_rpc_url() {
        let config = r#"{
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(
            config.rpc_url,
            "https://ethereum-holesky-rpc.publicnode.com"
        );
    }

    #[test]
    fn test_default_rewards_coordinator() {
        let config = r#"{
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(
            config.rewards_coordinator,
            "0xAcc1fb458a1317E886dB376Fc8141540537E68fE"
        );
    }

    #[test]
    fn test_default_earliest_block() {
        let config = r#"{
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(config.earliest_block, 0);
    }

    #[test]
    fn test_default_max_blocks() {
        let config = r#"{
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(config.max_blocks, 100);
    }

    #[test]
    fn test_default_sleep_duration() {
        let config = r#"{
    }"#;
        let config: Config = serde_json::from_str(config).unwrap();
        assert_eq!(config.sleep_duration_ms, 1000);
    }
}
