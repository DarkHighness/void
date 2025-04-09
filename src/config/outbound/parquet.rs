use crate::{
    config::{template::Template, Verify},
    core::tag::{OutboundTagId, TagId},
};
use parquet::basic::{BrotliLevel, GzipLevel};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Parquet Compression options
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZO compression
    Lzo,
    /// Brotli compression
    Brotli,
    /// No compression
    None,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::Snappy
    }
}

impl From<Compression> for parquet::basic::Compression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Snappy => parquet::basic::Compression::SNAPPY,
            Compression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
            Compression::Lzo => parquet::basic::Compression::LZO,
            Compression::Brotli => parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            Compression::None => parquet::basic::Compression::UNCOMPRESSED,
        }
    }
}

/// Configuration for Parquet outbound
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetOutboundConfig {
    /// Tag for this outbound
    #[serde(default = "default_parquet_tag")]
    pub tag: OutboundTagId,

    /// Input sources to read records from
    pub inbounds: Vec<TagId>,

    /// Path to the output Parquet file
    pub path: Template<PathBuf>,

    /// Maximum number of records to batch before writing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Compression codec to use
    #[serde(default)]
    pub compression: Compression,

    #[serde(default)]
    pub disabled: bool,
}

fn default_parquet_tag() -> OutboundTagId {
    OutboundTagId::new("parquet")
}

fn default_batch_size() -> usize {
    1000
}

impl ParquetOutboundConfig {
    /// Returns the scale factor for the channel
    pub fn channel_scale_factor(&self) -> usize {
        8
    }
}

impl Verify for ParquetOutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.path.to_string_lossy().is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "path"));
        }

        if self.inbounds.is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "inbounds"));
        }

        Ok(())
    }
}
