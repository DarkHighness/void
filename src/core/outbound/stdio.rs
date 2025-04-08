use async_trait::async_trait;
use log::error;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::{
    config::outbound::stdio::{Io, StdioOutboundConfig},
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        tag::{HasTag, TagId},
    },
    utils::recv::recv_batch,
};

use super::base::Outbound;

pub struct StdioOutbound {
    tag: TagId,

    io: tokio::io::BufWriter<Box<dyn AsyncWrite + Send + Unpin>>,
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

        let io: tokio::io::BufWriter<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> = match cfg.io {
            Io::Stdout => tokio::io::BufWriter::new(Box::new(tokio::io::stdout())),
            Io::Stderr => tokio::io::BufWriter::new(Box::new(tokio::io::stderr())),
        };

        Ok(StdioOutbound { tag, io, inbounds })
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
            Ok(records) => records,
            Err(crate::utils::recv::Error::Timeout) => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        for record in &records {
            let s = record.to_string();
            if let Err(e) = self.io.write_all(s.as_bytes()).await {
                error!("{}: failed to write record: {:?}", tag, e);
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
