use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use log::{error, info};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::config::outbound::parquet::ParquetOutboundConfig;
use crate::core::types::conv::parquet::{records_to_arrow_schema, records_to_record_batch, Error};
use crate::core::{
    actor::Actor,
    manager::{ChannelGraph, TaggedReceiver},
    tag::{HasTag, TagId},
    types::Record,
};
use crate::utils::recv::recv_batch;

use super::base::Outbound;

pub struct ParquetOutbound {
    tag: TagId,
    path: String,
    batch_size: usize,
    compression: Compression,
    append: bool,
    inbounds: Vec<TaggedReceiver>,
    schema: Option<SchemaRef>,
    records_buffer: Vec<Record>,
}

impl HasTag for ParquetOutbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

impl ParquetOutbound {
    pub fn try_create_from(
        cfg: ParquetOutboundConfig,
        channels: &mut ChannelGraph,
    ) -> super::Result<Self> {
        let tag = cfg.tag.into();
        let inbounds = cfg
            .inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &tag))
            .collect::<Vec<_>>();

        // Convert path to String for easier manipulation
        let path = cfg.path.to_string_lossy().to_string();

        // Use direct conversion from enum
        let compression = cfg.compression.into();

        Ok(ParquetOutbound {
            tag,
            path,
            batch_size: cfg.batch_size,
            compression,
            append: cfg.append,
            inbounds,
            schema: None,
            records_buffer: Vec::with_capacity(cfg.batch_size),
        })
    }

    async fn flush_records(&mut self) -> super::Result<()> {
        if self.records_buffer.is_empty() {
            return Ok(());
        }

        // Create schema from the first record if not created yet
        if self.schema.is_none() && !self.records_buffer.is_empty() {
            let schema = records_to_arrow_schema(&self.records_buffer)?;
            self.schema = Some(schema);
        }

        let schema = match &self.schema {
            Some(s) => s.clone(),
            None => return Ok(()),
        };

        // Convert records to Arrow RecordBatch
        let record_batch = records_to_record_batch(&self.records_buffer, schema.clone())?;

        // Ensure directory exists
        if let Some(parent) = Path::new(&self.path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Check if file exists and we're appending
        let file_exists = tokio::fs::metadata(&self.path).await.is_ok();

        if file_exists && self.append {
            // Append to existing file - this requires reading the schema first
            // For now, we'll just error out as true append support would require more complex code
            panic!("Appending to existing Parquet files is not supported yet");
        }

        // Open file and write the batch
        let file = File::create(&self.path).await?;

        let file = file.into_std().await;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        info!(
            "{}: Wrote {} records to {}",
            self.tag,
            self.records_buffer.len(),
            self.path
        );
        self.records_buffer.clear();

        Ok(())
    }
}

#[async_trait]
impl Actor for ParquetOutbound {
    type Error = super::Error;

    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()> {
        let tag = self.tag.clone();
        let batch_size = self.batch_size;

        let records = match recv_batch(
            &tag,
            self.inbounds(),
            Some(std::time::Duration::from_millis(100)),
            batch_size,
            ctx.clone(),
        )
        .await
        {
            Ok(records) => records,
            Err(crate::utils::recv::Error::Timeout) => {
                // On timeout, flush any buffered records
                if !self.records_buffer.is_empty() {
                    self.flush_records().await?;
                }
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if records.is_empty() {
            return Ok(());
        }

        // Add records to buffer
        self.records_buffer.extend(records);

        // If we've reached our batch size, flush the records
        if self.records_buffer.len() >= self.batch_size {
            self.flush_records().await?;
        }

        Ok(())
    }
}

impl Outbound for ParquetOutbound {
    fn inbounds(&mut self) -> &mut [TaggedReceiver] {
        &mut self.inbounds
    }
}
