use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::{
    config::outbound::stdio::{Io, StdioOutboundConfig},
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        tag::{HasTag, TagId},
    },
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
        let mut streams = self
            .inbounds
            .iter_mut()
            .map(|rx| rx.recv())
            .collect::<FuturesUnordered<_>>();

        loop {
            tokio::select! {
                Some(record) = streams.next() => {
                    match record {
                        Ok(record) => {
                            match self.io {
                                Io::Stdout => {
                                    println!("{}", record);
                                }
                                Io::Stderr => {
                                    eprintln!("{}", record);
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Error receiving record: {}", e);
                        }
                    }
                }
                _ = ctx.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Outbound for StdioOutbound {}
