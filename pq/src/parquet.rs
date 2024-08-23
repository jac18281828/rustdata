use std::{fs, path::Path, sync::Arc};

use alloy::sol_types::SolValue;
use eyre::Ok;
use parquet::{
    column::reader::ColumnReader, data_type::FixedLenByteArray, data_type::FixedLenByteArrayType,
    file::reader::FileReader, file::reader::SerializedFileReader,
    file::writer::SerializedFileWriter, schema::parser::parse_message_type,
};
use tracing::debug;

#[derive(Debug)]
pub struct ClaimedAmountStat {
    pub sum: u128,
    pub count: u64,
    pub claimed_mean: f64,
    pub claimed_max: u128,
    pub claimed_min: u128,
}

fn convert_to_fixed_bytes(data: Vec<u8>) -> eyre::Result<FixedLenByteArray> {
    let fixed_bytes = FixedLenByteArray::from(data);
    Ok(fixed_bytes)
}

pub fn write_parquet_file(
    path: &str,
    rewards_claimed: Vec<eigen_types::RewardsClaimed>,
) -> eyre::Result<u64> {
    let path = Path::new(path);

    let dx_data = vec![1, 2, 3];

    let message_type = "
      message schema {
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) root;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) earner;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) claimer;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) recipient;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) token;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) claimedAmount;
      }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // roots
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.root.clone().to_vec()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // earners
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.earner.clone().into()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // claimer
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.claimer.clone().into()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // claimer
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.recipient.clone().into()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // claimer
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.token.clone().into()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // claimer
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &rewards_claimed
                    .iter()
                    .map(|r| convert_to_fixed_bytes(r.claimed_amount.abi_encode()).unwrap())
                    .collect::<Vec<FixedLenByteArray>>(),
                None,
                None,
            )
            .unwrap();
        col_writer.close().unwrap();
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();

    let row_count = dx_data.len();

    println!("Wrote {} rows", row_count);
    Ok(row_count as u64)
}

#[allow(dead_code)] // this function is an example for testing purposes
pub fn read_reward_file(path: &str) -> eyre::Result<()> {
    let path = Path::new(path);
    // Reading data using column reader API.
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    for i in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i).unwrap();
        let row_group_metadata = metadata.row_group(i);
        println!(
            "Row group: {} has {} rows",
            i,
            row_group_metadata.num_rows()
        );
        println!(
            "Row group: {} has {} columns",
            i,
            row_group_metadata.num_columns()
        );
        for j in 0..row_group_metadata.num_columns() {
            let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
            match column_reader {
                // You can also use `get_typed_column_reader` method to extract typed reader.
                ColumnReader::FixedLenByteArrayColumnReader(ref mut dx_reader) => {
                    let mut values = vec![];
                    let mut def_levels = vec![];
                    let mut rep_levels = vec![];

                    let (records_read, values_read, levels_read) = dx_reader
                        .read_records(
                            10000, // maximum records to read
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values,
                        )
                        .unwrap();
                    println!("Read {} records", records_read);
                    println!("Values read: {:?}", values_read);
                    println!("Def levels read: {:?}", levels_read);
                    println!("Values are: {:?}", values);
                }
                _ => {
                    println!("Column type not supported");
                }
            }
        }
    }
    Ok(())
}

pub fn read_reward_file_stat(path: &str) -> eyre::Result<ClaimedAmountStat> {
    let path = Path::new(path);
    // Reading data using column reader API.
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let mut claimed_stat = ClaimedAmountStat {
        sum: 0,
        count: 0,
        claimed_mean: 0.0,
        claimed_max: 0,
        claimed_min: u128::MAX,
    };
    for i in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i).unwrap();
        let row_group_metadata = metadata.row_group(i);
        let claimed_amount_column = row_group_metadata.num_columns() - 1;
        let mut column_reader = row_group_reader
            .get_column_reader(claimed_amount_column)
            .unwrap();
        match column_reader {
            // You can also use `get_typed_column_reader` method to extract typed reader.
            ColumnReader::FixedLenByteArrayColumnReader(ref mut dx_reader) => {
                let mut values = vec![];
                let mut def_levels = vec![];
                let mut rep_levels = vec![];

                let (records_read, values_read, levels_read) = dx_reader
                    .read_records(
                        10000, // maximum records to read
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut values,
                    )
                    .unwrap();
                println!("Read {} records", records_read);
                println!("Values read: {:?}", values_read);
                println!("Def levels read: {:?}", levels_read);
                for value in values {
                    if value.data().len() != 32 {
                        debug!("Invalid fixed byte data length: {}", value.data().len());
                        continue;
                    }
                    // last 16 bytes
                    let data_bytes = value.data()[16..].try_into().unwrap();
                    let claimed_amount =
                        u128::from_le_bytes(data_bytes) / 1_000_000_000_000_000_000;
                    claimed_stat.sum += claimed_amount;
                    claimed_stat.count += 1;
                    claimed_stat.claimed_mean = claimed_stat.sum as f64 / claimed_stat.count as f64;
                    claimed_stat.claimed_max = claimed_amount.max(claimed_stat.claimed_max);
                    claimed_stat.claimed_min = claimed_amount.min(claimed_stat.claimed_min);
                }
            }
            _ => {
                println!("Column type not supported");
            }
        }
    }
    Ok(claimed_stat)
}
