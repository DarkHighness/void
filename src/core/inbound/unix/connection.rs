use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::{unix::SocketAddr, UnixStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::config::inbound::ScanMode;
pub struct UnixConnection {
    reader: tokio::sync::Mutex<BufReader<UnixStream>>,

    addr: SocketAddr,
    mode: ScanMode,

    tx: mpsc::Sender<String>,
    ctx: CancellationToken,
}

impl UnixConnection {
    fn new(
        stream: UnixStream,
        addr: SocketAddr,
        mode: ScanMode,
        tx: mpsc::Sender<String>,
        ctx: CancellationToken,
    ) -> Self {
        UnixConnection {
            reader: tokio::sync::Mutex::new(BufReader::new(stream)),
            addr,
            mode,
            tx,
            ctx,
        }
    }

    async fn read_next(&self, buf: &mut String) -> std::io::Result<usize> {
        let mut reader = self.reader.lock().await;

        match self.mode {
            ScanMode::Line => reader.read_line(buf).await,
            ScanMode::Full => reader.read_to_string(buf).await,
        }
    }

    pub fn spawn(
        stream: UnixStream,
        addr: SocketAddr,
        mode: ScanMode,
        tx: mpsc::Sender<String>,
        ctx: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let conn = UnixConnection::new(stream, addr, mode, tx, ctx);
        tokio::spawn(async move {
            loop {
                let mut buf = String::new();
                let next = conn.read_next(&mut buf);
                let cancelled = conn.ctx.cancelled();

                tokio::select! {
                    // UnixInbound has been dropped
                    _ = cancelled => {
                        break;
                    }
                    readed = next => {
                        match readed {
                            Ok(0) => {
                                log::info!("Connection closed: {:?}", conn.addr);
                                break;
                            }
                            Err(e) => {
                                log::error!("Failed to read data: {:?} {}", conn.addr, e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }

                let cancelled = conn.ctx.cancelled();

                tokio::select! {
                    // UnixInbound has been dropped
                    _ = cancelled => {
                        break;
                    }
                    error = conn.tx.send(buf) => if let Err(_) = error {
                        break;
                    }
                }
            }
        })
    }
}
