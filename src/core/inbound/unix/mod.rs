pub(crate) mod connection;

use std::path::PathBuf;

use async_trait::async_trait;
use connection::UnixConnection;
use log::{debug, info};
use tokio::{net::UnixListener, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::inbound::{parser::ParserConfig, ScanMode},
    core::{
        component::Component,
        tag::{HasTag, TagId},
    },
};

use super::error::Result;
use super::{base::Inbound, parser::Parser};

pub(crate) struct UnixSocketInbound {
    tag: TagId,

    path: PathBuf,
    mode: ScanMode,

    parser: Box<dyn Parser>,

    listener: UnixListener,

    connection_handles: Vec<JoinHandle<()>>,

    ctx: CancellationToken,
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<String>,
}

impl UnixSocketInbound {
    pub fn try_create_from_config(
        tag: TagId,
        mode: ScanMode,
        path: PathBuf,
        parser_cfg: ParserConfig,
    ) -> Result<Self> {
        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let socket = UnixListener::bind(&path)?;
        let parser = super::parser::try_create_from_config(parser_cfg)?;
        let (tx, rx) = mpsc::channel(1024);

        let inbound = UnixSocketInbound {
            tag,
            mode,
            path,
            listener: socket,
            connection_handles: Vec::new(),
            ctx: CancellationToken::new(),
            tx,
            rx,
            parser,
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
        self.tag.clone()
    }
}

#[async_trait]
impl Component for UnixSocketInbound {
    type T = ();
    type Error = super::error::Error;

    async fn poll_async(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<Self::T, Self::Error> {
        let new_connection = self.listener.accept();

        tokio::select! {
            _ = ctx.cancelled() => return Ok(()),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                debug!("inbound \"{}\" not ready", self.tag);
                return Ok(());
            }
            Ok((stream, addr)) = new_connection => {
                info!("inbound \"{}\" accept new connection \"{:?}\" ", self.tag, addr);
                let handle = UnixConnection::spawn(
                    stream,
                    addr,
                    self.mode.clone(),
                    self.tx.clone(),
                    self.ctx.clone(),
                );
                self.connection_handles.push(handle);
            }
            data = self.rx.recv() => match data {
                Some(data) => {
                    let record = self.parser.parse(data)?;
                    info!("{}: {:?}", self.tag, record)
                }
                None => return Ok(()),
            }
        }

        Ok(())
    }
}

impl Inbound for UnixSocketInbound {}
