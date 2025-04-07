// use log::{debug, error, warn};
// use tokio::net::{unix::SocketAddr, UnixStream};
// use tokio_util::sync::CancellationToken;

// use crate::{
//     config::ProtocolConfig,
//     core::{
//         manager::TaggedSender,
//         protocol::{self, ProtocolParser},
//         tag::TagId,
//         types::Attribute,
//     },
// };
// pub struct UnixConnection {
//     tag: TagId,
//     remote_addr: SocketAddr,

//     parser: Box<dyn ProtocolParser>,
//     sender: TaggedSender,

//     ctx: CancellationToken,
// }

// impl UnixConnection {
//     pub fn try_create_from(
//         tag: TagId,
//         stream: UnixStream,
//         protocol_cfg: ProtocolConfig,
//         tx: TaggedSender,
//         ctx: CancellationToken,
//     ) -> super::Result<Self> {
//         let remote_addr = stream.peer_addr()?;
//         let parser = protocol::try_create_from(stream, protocol_cfg)?;

//         Ok(UnixConnection {
//             tag,
//             remote_addr,
//             parser,
//             sender: tx,
//             ctx,
//         })
//     }

//     pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
//         let name = format!("{}{:?}", self.tag, self.remote_addr);

//         tokio::task::Builder::new().name(&name).spawn(async move {
//             loop {
//                 let next_record = self.parser.read_next();
//                 let cancelled = self.ctx.cancelled();
//                 let mut record = tokio::select! {
//                     // UnixInbound has been dropped
//                     _ = cancelled => break,
//                     record = next_record => match record {
//                         Ok(record) => record,
//                         Err(err) => match err.is_eof(){
//                             true => {
//                                 warn!("unix connection of {}({:?}) has been closed", self.tag, self.remote_addr);
//                                 break;
//                             }
//                             false => {
//                                 error!("unix connection of {}({:?}) encountered an error: {}", self.tag, self.remote_addr, err);
//                                 continue;
//                             }
//                         }
//                     }
//                 };

//                 record.set_attribute(Attribute::Inbound, (&self.tag).into());

//                 match self.sender.send(record) {
//                     Ok(n) => {
//                         debug!(
//                             "unix connection of {}({:?}) send a record to {} receivers",
//                             self.tag, self.remote_addr, n
//                         );
//                     },
//                     Err(_) => {
//                         warn!(
//                             "unix connection of {}({:?}) send a record failed, channel closed",
//                             self.tag, self.remote_addr
//                         );
//                         break;
//                     }
//                 }
//             }
//         }).expect("Failed to spawn unix connection")
//     }
// }
