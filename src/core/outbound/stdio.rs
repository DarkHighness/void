use async_trait::async_trait;
use futures::stream::StreamExt;
use std::io::Write;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{
        global::{time_tracing_path, use_time_tracing},
        outbound::stdio::{Io, StdioOutboundConfig},
    },
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        tag::{HasTag, TagId},
    },
    utils::recv::recv_batch,
};

use super::base::Outbound;

use crate::core::types::{STAGE_OUTBOUND_PROCESSED, STAGE_OUTBOUND_RECEIVED};
use crate::utils::record_timing::{mark_pipeline_stage, summarize_record_timings};

pub struct StdioOutbound {
    tag: TagId,

    io: Io,
    inbounds: Vec<TaggedReceiver>,
}

impl HasTag for StdioOutbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

impl StdioOutbound {
    pub fn try_create_from(
        cfg: StdioOutboundConfig,
        channels: &mut ChannelGraph,
    ) -> super::Result<Self> {
        let tag = cfg.tag.into();
        let inbounds = cfg
            .inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &tag))
            .collect::<Vec<_>>();

        Ok(StdioOutbound {
            tag,
            io: cfg.io,
            inbounds,
        })
    }
}

#[async_trait]
impl Actor for StdioOutbound {
    type Error = super::Error;

    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()> {
        let tag = self.tag.clone();

        let records = match recv_batch(
            &tag,
            self.inbounds(),
            Some(std::time::Duration::from_millis(100)),
            16,
            ctx,
        )
        .await
        {
            Ok(mut records) => {
                // Mark outbound receiving time
                mark_pipeline_stage(&mut records, STAGE_OUTBOUND_RECEIVED);
                records
            }
            Err(crate::utils::recv::Error::Timeout) => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        for mut record in records {
            // Mark outbound processing completion time
            record.mark_timestamp(STAGE_OUTBOUND_PROCESSED);

            if use_time_tracing() {
                let file = std::fs::File::options()
                    .append(true)
                    .create(true)
                    .open(time_tracing_path())
                    .unwrap();
                let mut writer = std::io::BufWriter::new(file);
                writeln!(writer, "{}", summarize_record_timings(&record)).unwrap();
            }

            // Output record content
            match self.io {
                Io::Stdout => {
                    println!("{}", record);
                }
                Io::Stderr => {
                    eprintln!("{}", record);
                }
            }
        }

        Ok(())
    }
}

impl Outbound for StdioOutbound {
    fn inbounds(&mut self) -> &mut [TaggedReceiver] {
        &mut self.inbounds
    }
}
