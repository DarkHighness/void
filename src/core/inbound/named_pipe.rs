use std::path::PathBuf;

use async_trait::async_trait;
use log::info;
use tokio::{net::UnixListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{
        inbound::{named_pipe::NamedPipeConfig, unix::UnixSocketConfig},
        ProtocolConfig,
    },
    core::{
        actor::Actor,
        inbound::instance::ReaderBasedInstance,
        manager::{ChannelGraph, TaggedSender},
        tag::{HasTag, TagId},
    },
};

use super::base::Inbound;
use super::error::Result;

pub(crate) struct NamedPipeInbound {
    tag: TagId,
    path: PathBuf,

    ctx: CancellationToken,

    handle: Option<JoinHandle<()>>,

    outbound: TaggedSender,
    protocol: ProtocolConfig,
}

impl NamedPipeInbound {
    pub fn try_create_from(
        cfg: NamedPipeConfig,
        protocol_cfg: ProtocolConfig,
        channel_graph: &mut ChannelGraph,
    ) -> Result<Self> {
        let path = cfg.path;

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tag = cfg.tag.into();
        let outbound = channel_graph.sender(&tag);

        let inbound = NamedPipeInbound {
            tag,
            path,
            handle: None,
            ctx: CancellationToken::new(),
            outbound,
            protocol: protocol_cfg,
        };

        info!(
            "inbound \"{}\" listening on {:?}",
            inbound.tag, inbound.path
        );

        Ok(inbound)
    }
}

impl Drop for NamedPipeInbound {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            log::error!("Failed to remove named pipe file: {:?}", e);
        }

        self.ctx.cancel();
    }
}

impl HasTag for NamedPipeInbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for NamedPipeInbound {
    type Error = super::Error;
    async fn poll(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> miette::Result<(), super::Error> {
        if self.handle.is_none() {
            // make fifo pipe
            nix::unistd::mkfifo(
                &self.path,
                nix::sys::stat::Mode::S_IRWXU
                    | nix::sys::stat::Mode::S_IRWXG
                    | nix::sys::stat::Mode::S_IRWXO,
            )?;

            let receiver = tokio::net::unix::pipe::OpenOptions::new().open_receiver(&self.path)?;

            let reader = ReaderBasedInstance::try_create_from(
                self.tag.clone(),
                self.path.display().to_string(),
                receiver,
                self.protocol.clone(),
                self.outbound.clone(),
                ctx.clone(),
            )?;

            self.handle = Some(reader);
        }

        tokio::task::yield_now().await;

        Ok(())
    }
}

impl Inbound for NamedPipeInbound {}
