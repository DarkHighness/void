use log::{error, warn};
use tokio::{io::AsyncRead, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ProtocolConfig,
    core::{
        manager::TaggedSender,
        protocol::{self, ProtocolParser},
        tag::TagId,
    },
};

pub struct ReaderBasedInstance {
    tag: TagId,
    id: String,

    parser: Box<dyn ProtocolParser>,
    sender: TaggedSender,

    ctx: CancellationToken,
}

impl ReaderBasedInstance {
    pub fn try_create_from<R: AsyncRead + Send + Unpin + 'static>(
        tag: TagId,
        id: String,
        reader: R,
        protocol: ProtocolConfig,
        sender: TaggedSender,
        ctx: CancellationToken,
    ) -> super::Result<JoinHandle<()>> {
        let parser = protocol::try_create_from(reader, protocol)?;

        let instance = ReaderBasedInstance {
            tag,
            id,
            parser,
            sender,
            ctx,
        };

        let handle = instance.spawn();

        Ok(handle)
    }

    fn spawn(self) -> tokio::task::JoinHandle<()> {
        let name = format!("{}({})", self.tag, self.id);

        tokio::task::Builder::new()
            .name(&name.clone())
            .spawn(async move {
                let mut parser = self.parser;

                loop {
                    let next_record = parser.read_next();
                    let cancelled = self.ctx.cancelled();
                    let record = tokio::select! {
                        // Instance has been dropped
                        _ = cancelled => break,
                        record = next_record => match record {
                            Ok(record) => record,
                            Err(err) => match err.is_eof(){
                                true => {
                                    warn!("{} has been closed", &name);
                                    break;
                                }
                                false => {
                                    error!("Error reading from {}, err: {}", &name, err);
                                    break;
                                }
                            }
                        }
                    };

                    if let Err(err) = self.sender.send(record) {
                        error!("{} failed to send, err: {}", &name, err);
                        break;
                    }
                }
            })
            .expect("Failed to spawn task")
    }
}
