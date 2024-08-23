# rustdata

## Disclaimer

This is an unscientific comparision of Parquet and Postgress for a simple stat calculation.

The dataset is 2500 rows of Ethereum Event data.  Eigenlayer IRewardsCoordinator.RewardsClaimed.  The data is stored to the database and then serialized back with the purpose of computing some common statistics, mean, min, max, etc.


## Postgres

#### 1. Start Postgres database

```bash
docker run --name my-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=edb -p 5432:5432 -d postgres
```

#### 2. Upload Postgres data

```bash
cargo run --release --bin pg
```

Write time to Postgres:

1037531 us

Read time for stats

2539 us


## Parquet

#### 1. run the client

```bash
cargo run --release --bin pq
```

Write time to Parquet

2687 us

Read time for stats

339 us

##### Storage size

583k


## Comparison Chart

| Storage                                | Write Time (us) | Read Time (us) | Storage Size (kB) |
| -------------------------------------- | --------------- | -------------- | ----------------- |
| [Parquet](https://parquet.apache.org/) | 2687            | 339            | 583               |
| [Postgres](https://www.postgresql.org) | 1037531         | 2539           | 1024              |
