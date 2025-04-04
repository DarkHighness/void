pub(crate) mod connection;

use std::path::PathBuf;

use async_trait::async_trait;
use connection::UnixConnection;
use log::{debug, info};
use tokio::{net::UnixListener, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{inbound::unix::UnixSocketConfig, ProtocolConfig},
    core::{
        tag::{HasTag, InboundTagId, TagId},
        types::Record,
    },
};

use super::base::Inbound;
use super::error::Result;

pub(crate) struct UnixSocketInbound {
    tag: InboundTagId,
    path: PathBuf,

    listener: UnixListener,
    ctx: CancellationToken,

    tx: mpsc::Sender<Record>,
    rx: mpsc::Receiver<Record>,
    handles: Vec<JoinHandle<()>>,

    protocol: ProtocolConfig,
}

impl UnixSocketInbound {
    pub fn try_create_from(cfg: UnixSocketConfig, protocol_cfg: ProtocolConfig) -> Result<Self> {
        let path = cfg.path;

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let socket = UnixListener::bind(&path)?;

        let (tx, rx) = mpsc::channel(64);

        let inbound = UnixSocketInbound {
            tag: cfg.tag,
            path,
            listener: socket,
            ctx: CancellationToken::new(),
            tx,
            rx,
            handles: Vec::new(),
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
    fn tag(&self) -> TagId {
        (&self.tag).into()
    }
}

#[async_trait]
impl Inbound for UnixSocketInbound {
    async fn poll_async(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), super::Error> {
        let new_connection = self.listener.accept();

        tokio::select! {
            _ = ctx.cancelled() => return Ok(()),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                debug!("inbound \"{}\" not ready", self.tag);
                return Ok(());
            }
            Ok((stream, addr)) = new_connection => {
                info!("inbound \"{}\" accept new connection \"{:?}\" ", self.tag, addr);
                let conn = UnixConnection::try_create_from(
                    stream,
                    self.protocol.clone(),
                    self.tx.clone(),
                    self.ctx.clone(),
                )?;
                let handle = conn.spawn();
                self.handles.push(handle);
                info!("inbound \"{}\" spawn new reader \"{:?}\" ", self.tag, addr);
            }
            data = self.rx.recv() => match data {
                Some(data) => {
                    info!("{} {:?}", self.tag, data)
                }
                None => return Ok(()),
            }
        }

        Ok(())
    }
}
