mod quack;

#[tokio::main]
async fn main() {
    init_logging();

    // File paths
    let json_file_path = "rewards_claimed.json";

    // Read the JSON file
    let rewards_claimed = read_reward_json(json_file_path).await.unwrap();

    // connect db
    let mut duck_client = quack::init().await.unwrap();
    quack::create_tables(&mut duck_client).await.unwrap();

    let write_instant = std::time::Instant::now();
    // Write the pg data
    quack::write_rewards_claimed(&mut duck_client, &rewards_claimed)
        .await
        .unwrap();
    println!("Write time: {:?} us", write_instant.elapsed().as_micros());

    let read_instant = std::time::Instant::now();
    let stat = quack::read_rewards_claimed_stat(&mut duck_client).await.unwrap();
    println!(
        "Read stat time: {:?} us",
        read_instant.elapsed().as_micros()
    );
    println!("Claimed amount stat: {:?}", stat);

    println!(
        "Table size in postgres: {:?}",
        quack::table_size(&mut duck_client).await.unwrap()
    );
}

fn init_logging() {
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .expect("setting default subscriber failed");
}

async fn read_reward_json(path: &str) -> eyre::Result<Vec<eigen_types::RewardsClaimed>> {
    let rewards_claimed = std::fs::read_to_string(path)?;
    let rewards_claimed: Vec<eigen_types::RewardsClaimed> = serde_json::from_str(&rewards_claimed)?;
    println!("{:?}", rewards_claimed.len());
    Ok(rewards_claimed)
}
