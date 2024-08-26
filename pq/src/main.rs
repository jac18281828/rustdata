mod metadata;
mod parquet;

use metadata::Metadata;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    // File paths
    let json_file_path = "rewards_claimed.json";
    let parquet_file_path = "rewards_coordinator.parquet";
    let metadata_file_path = "rewards_coordinator_metadata.json";

    // Read the JSON file
    let rewards_claimed = read_reward_json(json_file_path)?;

    let write_instant = std::time::Instant::now();
    // Write the Parquet file
    parquet::write_parquet_file(parquet_file_path, &rewards_claimed)?;
    println!("Write time: {:?} us", write_instant.elapsed().as_micros());

    // Create some metadata
    let metadata = Metadata {
        description: String::from("Parquet file"),
        row_count: rewards_claimed.len() as u64,
    };

    // Write the metadata sidecar
    metadata::write_metadata_sidecar(metadata_file_path, &metadata)?;
    println!("Parquet file and metadata sidecar written successfully.");

    let read_instant = std::time::Instant::now();
    let stat = parquet::read_reward_file_stat(parquet_file_path)?;
    println!(
        "Read stat time: {:?} us",
        read_instant.elapsed().as_micros()
    );
    println!("Claimed amount stat: {:?}", stat);

    Ok(())
}

fn init_logging() {
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(tracing::Level::DEBUG)
        .try_init()
        .expect("setting default subscriber failed");
}

fn read_reward_json(path: &str) -> eyre::Result<Vec<eigen_types::RewardsClaimed>> {
    let rewards_claimed = std::fs::read_to_string(path)?;
    let rewards_claimed: Vec<eigen_types::RewardsClaimed> = serde_json::from_str(&rewards_claimed)?;
    println!("{:?}", rewards_claimed.len());
    Ok(rewards_claimed)
}
