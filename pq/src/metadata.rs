use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub description: String,
    pub row_count: u64,
}

pub fn write_metadata_sidecar(
    file_path: &str,
    metadata: &Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(file_path)?;
    serde_json::to_writer_pretty(file, &metadata)?;
    Ok(())
}
