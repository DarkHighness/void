use crate::config::{inbound::InboundConfig, ProtocolConfig};

mod base;
mod error;
mod instance;
mod unix;

pub use base::Inbound;
pub use error::{Error, Result};

use super::manager::ChannelGraph;

pub fn try_create_from(
    inbound_config: InboundConfig,
    protocol_config: ProtocolConfig,
    channel_graph: &mut ChannelGraph,
) -> Result<Box<dyn base::Inbound>> {
    let inbound = match inbound_config {
        InboundConfig::UnixSocket(cfg) => Box::new(unix::UnixSocketInbound::try_create_from(
            cfg,
            protocol_config,
            channel_graph,
        )?),
    };

    Ok(inbound)
}
