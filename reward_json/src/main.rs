mod config;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    sol,
};
use std::fs::OpenOptions;
use std::io::Write;
use tokio::time::{sleep, Duration};
use tracing::{debug, info};

sol!(
    #[sol(rpc)]
    IRewardsCoordinator,
    "abi/IRewardsCoordinator.json"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let config = config::load_config()?;

    let provider = ProviderBuilder::new().on_http(config.rpc_url.parse()?);
    let contract_address = Address::parse_checksummed(config.rewards_coordinator, None)?;
    let latest_block = provider.clone().get_block_number().await?;
    let rewards_contract = IRewardsCoordinator::new(contract_address, provider);

    let mut outputfile = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("rewards_claimed.json")?;

    let mut claimed_events: Vec<eigen_types::RewardsClaimed> = Vec::new();
    for block in (config.earliest_block..latest_block).step_by(config.max_blocks as usize) {
        let begin_block = block;
        let end_block = core::cmp::min(block + config.max_blocks, latest_block);
        let events = rewards_contract
            .RewardsClaimed_filter()
            .from_block(begin_block)
            .to_block(end_block)
            .query()
            .await?;

        let mut event_count = 0;
        for (event, _) in events {
            let root: [u8; 32] = event.root.as_slice().try_into()?;
            let rewards_claimed = eigen_types::RewardsClaimed {
                root,
                earner: event.earner.to_string(),
                claimer: event.claimer.to_string(),
                recipient: event.recipient.to_string(),
                token: event.token.to_string(),
                claimed_amount: event.claimedAmount.to::<u128>(),
            };
            claimed_events.push(rewards_claimed);
            event_count += 1;
        }
        if event_count > 0 {
            info!("Block {}: {} events", block, event_count);
        } else {
            debug!("Block {}: no events", block);
        }
        sleep(Duration::from_millis(config.sleep_duration_ms)).await;
    }
    let json_file = serde_json::to_string_pretty(&claimed_events)?;
    outputfile.write_all(json_file.as_bytes())?;
    outputfile.flush()?;
    info!(
        "Wrote {} events to rewards_claimed.json",
        claimed_events.len()
    );

    Ok(())
}

fn init_logging() {
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(tracing::Level::DEBUG)
        .try_init()
        .expect("setting default subscriber failed");
}
