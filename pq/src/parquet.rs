use std::{fs, path::Path, sync::Arc};

use anyhow::Ok;
use parquet::{
    column::reader::ColumnReader, data_type::FixedLenByteArray, data_type::FixedLenByteArrayType,
    data_type::Int32Type, file::reader::FileReader, file::reader::SerializedFileReader,
    file::writer::SerializedFileWriter, schema::parser::parse_message_type,
};

fn convert_to_fixed_bytes(data: Vec<u8>) -> anyhow::Result<FixedLenByteArray> {
    let fixed_bytes = FixedLenByteArray::from(data);
    Ok(fixed_bytes)
}

pub fn write_parquet_file(path: &str) -> anyhow::Result<u64> {
    let path = Path::new(path);

    let dx_data = vec![1, 2, 3];
    let binary256_data = vec!["a".repeat(32), "b".repeat(32), "c".repeat(32)];
    let binary256_data: Vec<FixedLenByteArray> = binary256_data
        .iter()
        .map(|x| x.as_bytes().to_vec())
        .map(|x| convert_to_fixed_bytes(x).unwrap())
        .collect();

    let message_type = "
      message schema {
        REQUIRED INT32 b;
        REQUIRED FIXED_LEN_BYTE_ARRAY (32) c;
      }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer
            .typed::<Int32Type>()
            .write_batch(&dx_data, None, None)
            .unwrap();
        col_writer.close().unwrap();
    }
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer
            .typed::<FixedLenByteArrayType>()
            .write_batch(&binary256_data, None, None)
            .unwrap();
        col_writer.close().unwrap();
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    let row_count = dx_data.len();

    println!("Wrote {} rows", row_count);
    Ok(row_count as u64)
}

pub fn read_parquet_file(path: &str) -> anyhow::Result<()> {
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
                ColumnReader::Int32ColumnReader(ref mut dx_reader) => {
                    let mut values = vec![];
                    let mut def_levels = vec![];
                    let mut rep_levels = vec![];

                    let (records_read, values_read, levels_read) = dx_reader
                        .read_records(
                            8, // maximum records to read
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
                ColumnReader::FixedLenByteArrayColumnReader(ref mut byte_reader) => {
                    let mut values = vec![];
                    let mut def_levels = vec![];
                    let mut rep_levels = vec![];
                    let (records_read, values_read, levels_read) = byte_reader
                        .read_records(
                            8, // maximum records to read
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
                _ => {}
            }
        }
    }
    Ok(())
}
