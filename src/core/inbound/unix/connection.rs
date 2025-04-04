use log::error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::{unix::SocketAddr, UnixStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{inbound::ScanMode, ProtocolConfig},
    core::{
        protocol::{self, Protocol},
        types::Record,
    },
};
pub struct UnixConnection {
    protocol: Box<dyn Protocol>,
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
        let protocol = protocol::try_create_from(stream, protocol_cfg)?;

        Ok(UnixConnection { protocol, tx, ctx })
    }

    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let next_record = self.protocol.read_next();
                let cancelled = self.ctx.cancelled();

                let record = tokio::select! {
                    // UnixInbound has been dropped
                    _ = cancelled => break,
                    record = next_record => match record {
                        Ok(record) => record,
                        Err(err) => {
                            if !err.is_eof() {
                                error!("failed to read record: {}", err);
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
