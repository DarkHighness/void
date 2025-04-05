pub(crate) mod connection;

use std::path::PathBuf;

use async_trait::async_trait;
use connection::UnixConnection;
use log::{error, info};
use tokio::{
    net::UnixListener,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{inbound::unix::UnixSocketConfig, ProtocolConfig},
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedSender},
        tag::{HasTag, TagId},
        types::Record,
    },
};

use super::base::Inbound;
use super::error::Result;

// pub const UNIX_SOCKET_CONNECTION_BUFFER_SIZE: usize = 64;

pub(crate) struct UnixSocketInbound {
    tag: TagId,
    path: PathBuf,

    listener: UnixListener,
    ctx: CancellationToken,

    handles: Vec<JoinHandle<()>>,

    outbound: TaggedSender,
    protocol: ProtocolConfig,
}

impl UnixSocketInbound {
    pub fn try_create_from(
        cfg: UnixSocketConfig,
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

        let socket = UnixListener::bind(&path)?;
        let outbound = channel_graph.sender(&tag);

        let inbound = UnixSocketInbound {
            tag,
            path,
            listener: socket,
            ctx: CancellationToken::new(),
            handles: Vec::new(),
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

impl Drop for UnixSocketInbound {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            log::error!("Failed to remove socket file: {:?}", e);
        }

        self.ctx.cancel();
    }
}

impl HasTag for UnixSocketInbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for UnixSocketInbound {
    type Error = super::Error;
    async fn poll(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), super::Error> {
        let new_connection = self.listener.accept();

        tokio::select! {
            _ = ctx.cancelled() => return Ok(()),
            Ok((stream, addr)) = new_connection => {
                info!("inbound \"{}\" accept new connection \"{:?}\" ", self.tag, addr);
                let conn = UnixConnection::try_create_from(
                    self.tag.clone(),
                    stream,
                    self.protocol.clone(),
                    self.outbound.clone(),
                    self.ctx.clone(),
                )?;
                let handle = conn.spawn();
                self.handles.push(handle);
                info!("inbound \"{}\" spawn a new connection \"{:?}\" ", self.tag, addr);
            }
        }

        Ok(())
    }
}

impl Inbound for UnixSocketInbound {}
