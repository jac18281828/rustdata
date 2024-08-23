mod metadata;
mod parquet;

use metadata::Metadata;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File paths
    let parquet_file_path = "data.parquet";
    let metadata_file_path = "data_metadata.json";

    // Write the Parquet file
    let row_count = parquet::write_parquet_file(parquet_file_path)?;

    // Create some metadata
    let metadata = Metadata {
        description: String::from("Parquet file"),
        row_count: row_count,
    };

    // Write the metadata sidecar
    metadata::write_metadata_sidecar(metadata_file_path, &metadata)?;
    println!("Parquet file and metadata sidecar written successfully.");

    parquet::read_parquet_file(parquet_file_path)?;

    Ok(())
}
