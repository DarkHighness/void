use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use log::{error, info};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;

use crate::config::outbound::parquet::ParquetOutboundConfig;
use crate::core::types::conv::parquet::ParquetWriter;
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
    inbounds: Vec<TaggedReceiver>,
    schema: Option<SchemaRef>,
    records_buffer: Vec<Record>,
    writer: Option<ParquetWriter>,
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
            inbounds,
            schema: None,
            records_buffer: Vec::with_capacity(cfg.batch_size),
            writer: None,
        })
    }

    async fn flush_records(&mut self) -> super::Result<()> {
        if self.records_buffer.is_empty() {
            return Ok(());
        }

        // Initialize schema and writer if needed
        if self.writer.is_none() {
            // Get or create schema based on first record
            let schema = if let Some(ref schema) = self.schema {
                schema.clone()
            } else {
                let schema =
                    crate::core::types::conv::parquet::record_to_schema(&self.records_buffer[0])?;
                self.schema = Some(schema.clone());
                schema
            };

            // Setup writer properties with compression
            let props_builder = WriterProperties::builder().set_compression(self.compression);
            let props = props_builder.build();

            // Create a new writer
            self.writer = Some(ParquetWriter::with_properties(
                &self.path,
                schema,
                Some(props),
            )?);
        }

        // Write records using our writer
        if let Some(writer) = &mut self.writer {
            writer.write_records(&self.records_buffer)?;

            info!(
                "Wrote {} records to {}",
                self.records_buffer.len(),
                self.path
            );

            // Clear buffer after successful write
            self.records_buffer.clear();
        }

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

// Implement Drop to ensure writer is closed properly
impl Drop for ParquetOutbound {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            // Try to close the writer
            if let Err(e) = writer.close() {
                error!("Error closing parquet writer: {}", e);
            }
        }
    }
}
