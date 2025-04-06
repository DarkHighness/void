use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::sync::broadcast::error::RecvError;
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
        let tag = self.tag.clone();

        let futs = self.inbounds.iter_mut().map(|inbound| {
            let io = self.io.clone();
            let ctx = ctx.clone();
            let tag = tag.clone();
            let inbound_tag = inbound.tag().clone();
            let fut = async move {
                loop {
                    if ctx.is_cancelled() {
                        break;
                    }

                    let msg = match inbound.recv().await {
                        Ok(msg) => msg,
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => {
                            return Err(super::Error::InboundClosed(tag, inbound_tag));
                        }
                    };

                    match io {
                        Io::Stdout => {
                            println!("{}", msg);
                        }
                        Io::Stderr => {
                            eprintln!("{}", msg);
                        }
                    }
                }

                Ok(())
            };

            fut
        });

        futures::future::try_join_all(futs).await?;

        Ok(())
    }
}

impl Outbound for StdioOutbound {}
