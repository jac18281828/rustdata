use tracing::warn;

use rusqlite::{Connection, Result, Error};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ClaimedAmountStat {
    pub sum: f64,
    pub count: u64,
    pub claimed_mean: f64,
    pub claimed_max: f64,
    pub claimed_min: f64,
}

/// A wrapper around `tokio_postgres::Client` for working with PostgreSQL database.
pub struct SqliteClient {
    client: Connection,
}

pub async fn init() -> eyre::Result<SqliteClient, Error> {
    let conn = Connection::open("rewards_claimed_sqlite.db")?;
    // let conn = Connection::open("rewards_claimed.db")?;
    // tokio::spawn(async {
    //     if let Err(e) = connection.await {
    //         eprintln!("connection error: {}", e);
    //     }
    // });
    Ok(SqliteClient { client: conn })
}

fn array_to_hex_string(array: &[u8; 32]) -> String {
    let mut hex_string = String::new();
    for byte in array {
        hex_string.push_str(&format!("{:02x}", byte));
    }
    hex_string
}

pub async fn create_tables(duck: &mut SqliteClient) -> Result<(), Error> {
    let _ = duck
        .client
        .execute("DROP INDEX IF EXISTS rewards_claimed_id_index", [])?;
    let _ = duck
        .client
        .execute("DROP INDEX IF EXISTS rewards_claimed_claimed_index", [])?;
    let _ = duck
        .client
        .execute("DROP TABLE IF EXISTS rewards_claimed", [])?;
    let _ = duck
        .client
        .execute(
            "CREATE TABLE IF NOT EXISTS rewards_claimed (
                id integer PRIMARY KEY,
                root CHAR(64),
                earner CHAR(42),
                claimer CHAR(42),
                recipient CHAR(42),
                token CHAR(42),
                claimed_amount NUMERIC
            )",
            [],
        )?;
    let _ =  duck
        .client
        .execute(
            "CREATE INDEX IF NOT EXISTS rewards_claimed_id_index ON rewards_claimed (id, root)",
            [],
        )?;
    let _ = duck
        .client
        .execute(
            "CREATE INDEX IF NOT EXISTS rewards_claimed_claimed_index ON rewards_claimed (claimed_amount)",
            [],
        )?;
    Ok(())
}

pub async fn write_rewards_claimed(
    quack: &mut SqliteClient,
    rewards_claimed: &Vec<eigen_types::RewardsClaimed>,
) -> Result<(), Error> {
    for reward in rewards_claimed {
        if reward.claimed_amount > 5000u128 * 10u128.pow(18) {
            // warn!("Claimed amount too high: {:?}", reward.claimed_amount);
            continue;
        }
        let claimed_amount: u128 = reward.claimed_amount.into();
        let _ = quack
            .client
            .execute(
                "INSERT INTO rewards_claimed (root, earner, claimer, recipient, token, claimed_amount) VALUES (?, ?, ?, ?, ?, ?)",
                &[
                    &array_to_hex_string(&reward.root),
                    &reward.earner,
                    &reward.claimer,
                    &reward.recipient,
                    &reward.token,
                    &claimed_amount.to_string(),
                ],
            );
    }
    Ok(())
}

pub async fn read_rewards_claimed_stat(quack: &mut SqliteClient) -> eyre::Result<ClaimedAmountStat> {
    let sum: f64 = quack
        .client
        .query_row("SELECT SUM(claimed_amount) FROM rewards_claimed", [], |row| row.get(0))?;

    let count: u64 = quack
        .client
        .query_row("SELECT COUNT(claimed_amount) FROM rewards_claimed", [], |row| row.get(0))?;

    let claimed_max: f64 = quack
        .client
        .query_row("SELECT MAX(claimed_amount) FROM rewards_claimed", [], |row| row.get(0))?;

    let claimed_min: f64 = quack
        .client
        .query_row("SELECT MIN(claimed_amount) FROM rewards_claimed", [], |row| row.get(0))?;

    let claimed_mean = sum / count as f64;

    Ok(ClaimedAmountStat {
        sum,
        count,
        claimed_mean,
        claimed_max,
        claimed_min,
    })
}

pub async fn table_size(quack: &mut SqliteClient) -> eyre::Result<u64> {
    let size: u64 = quack
        .client
        .query_row("select estimated_size from duckdb_tables()", [], |row| row.get(0))?;
    Ok(size)
}
