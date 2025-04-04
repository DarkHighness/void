use log::{error, warn};
use tokio::{
    net::{unix::SocketAddr, UnixStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ProtocolConfig,
    core::{
        protocol::{self, ProtocolParser},
        types::Record,
    },
};
pub struct UnixConnection {
    remote_addr: SocketAddr,

    parser: Box<dyn ProtocolParser>,
    tx: mpsc::Sender<Record>,

    ctx: CancellationToken,
}

impl UnixConnection {
    pub fn try_create_from(
        stream: UnixStream,
        protocol_cfg: ProtocolConfig,
        tx: mpsc::Sender<Record>,
        ctx: CancellationToken,
    ) -> super::Result<Self> {
        let remote_addr = stream.peer_addr()?;
        let parser = protocol::try_create_parser_from(stream, protocol_cfg)?;

        Ok(UnixConnection {
            remote_addr,
            parser,
            tx,
            ctx,
        })
    }

    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let next_record = self.parser.read_next();
                let cancelled = self.ctx.cancelled();
                let record = tokio::select! {
                    // UnixInbound has been dropped
                    _ = cancelled => break,
                    record = next_record => match record {
                        Ok(record) => record,
                        Err(err) => {
                            if err.is_eof() {
                                warn!("UnixInbound connection {:?} has been closed", self.remote_addr);
                            } else {
                                error!("UnixInbound connection {:?} error: {}", self.remote_addr, err);
                            }
                            break;
                        }
                    }
                };

                let cancelled = self.ctx.cancelled();

                tokio::select! {
                    // UnixInbound has been dropped
                    _ = cancelled => break,
                    error = self.tx.send(record) => if let Err(_) = error {
                        break;
                    }
                }
            }
        })
    }
}
