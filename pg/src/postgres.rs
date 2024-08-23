use tokio_postgres::{Client, Error, NoTls};
use tracing::warn;

use rust_decimal::prelude::*;
use rust_decimal::Decimal;

#[derive(Debug)]
pub struct ClaimedAmountStat {
    pub sum: Decimal,
    pub count: u64,
    pub claimed_mean: f64,
    pub claimed_max: Decimal,
    pub claimed_min: Decimal,
}

/// A wrapper around `tokio_postgres::Client` for working with PostgreSQL database.
pub struct PostgresClient {
    client: Client,
}

pub async fn init_postgres(connection_string: &str) -> eyre::Result<PostgresClient, Error> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
    tokio::spawn(async {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(PostgresClient { client })
}

fn array_to_hex_string(array: &[u8; 32]) -> String {
    let mut hex_string = String::new();
    for byte in array {
        hex_string.push_str(&format!("{:02x}", byte));
    }
    hex_string
}

pub async fn create_tables(postgres: &mut PostgresClient) -> Result<(), Error> {
    postgres
        .client
        .execute("DROP INDEX IF EXISTS rewards_claimed_id_index", &[])
        .await?;
    postgres
        .client
        .execute("DROP INDEX IF EXISTS rewards_claimed_claimed_index", &[])
        .await?;
    postgres
        .client
        .execute("DROP TABLE IF EXISTS rewards_claimed", &[])
        .await?;
    postgres
        .client
        .execute(
            "CREATE TABLE IF NOT EXISTS rewards_claimed (
                id SERIAL PRIMARY KEY,
                root CHAR(64),
                earner CHAR(42),
                claimer CHAR(42),
                recipient CHAR(42),
                token CHAR(42),
                claimed_amount NUMERIC
            )",
            &[],
        )
        .await?;
    postgres
        .client
        .execute(
            "CREATE INDEX IF NOT EXISTS rewards_claimed_id_index ON rewards_claimed (id, root)",
            &[],
        )
        .await?;
    postgres
        .client
        .execute(
            "CREATE INDEX IF NOT EXISTS rewards_claimed_claimed_index ON rewards_claimed (claimed_amount)",
            &[],
        )
        .await?;
    Ok(())
}

pub async fn write_rewards_claimed(
    postgres: &mut PostgresClient,
    rewards_claimed: Vec<eigen_types::RewardsClaimed>,
) -> Result<(), Error> {
    for reward in rewards_claimed {
        if reward.claimed_amount > 5000u128 * 10u128.pow(18) {
            warn!("Claimed amount too high: {:?}", reward.claimed_amount);
            continue;
        }
        let claimed_amount: Decimal = reward.claimed_amount.into();
        postgres
            .client
            .execute(
                "INSERT INTO rewards_claimed (root, earner, claimer, recipient, token, claimed_amount) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &array_to_hex_string(&reward.root),
                    &reward.earner,
                    &reward.claimer,
                    &reward.recipient,
                    &reward.token,
                    &claimed_amount,
                ],
            )
            .await?;
    }
    Ok(())
}

pub async fn read_rewards_claimed_stat(
    postgres: &mut PostgresClient,
) -> eyre::Result<ClaimedAmountStat> {
    let rows = postgres
        .client
        .query("SELECT SUM(claimed_amount) FROM rewards_claimed", &[])
        .await?;
    let sum = rows[0].get::<usize, Decimal>(0);
    let rows = postgres
        .client
        .query("SELECT COUNT(claimed_amount) FROM rewards_claimed", &[])
        .await?;
    let count = rows[0].get::<usize, i64>(0) as u64;
    let rows = postgres
        .client
        .query("SELECT MAX(claimed_amount) FROM rewards_claimed", &[])
        .await?;
    let claimed_max = rows[0].get::<usize, Decimal>(0);
    let rows = postgres
        .client
        .query("SELECT MIN(claimed_amount) FROM rewards_claimed", &[])
        .await?;
    let claimed_min = rows[0].get::<usize, Decimal>(0);
    let claimed_mean = sum.to_f64().unwrap() / count as f64;
    Ok(ClaimedAmountStat {
        sum,
        count,
        claimed_mean,
        claimed_max,
        claimed_min,
    })
}

pub async fn table_size(postgres: &mut PostgresClient) -> eyre::Result<u64> {
    let rows = postgres
        .client
        .query("SELECT pg_total_relation_size('rewards_claimed')", &[])
        .await?;
    let size: i64 = rows[0].get(0);
    Ok(size as u64)
}
