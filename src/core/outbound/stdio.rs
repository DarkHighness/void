use async_trait::async_trait;
use futures::stream::StreamExt;
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
        let records = match recv_batch(
            self.inbounds(),
            std::time::Duration::from_millis(100),
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

        for record in records {
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
