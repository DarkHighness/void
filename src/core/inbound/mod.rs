use crate::config::{inbound::InboundConfig, ProtocolConfig};

mod base;
mod error;
mod unix;

pub use base::Inbound;
pub use error::{Error, Result};

pub fn try_create_from(
    inbound_config: InboundConfig,
    protocol_config: ProtocolConfig,
) -> Result<Box<dyn base::Inbound>> {
    let inbound = match inbound_config {
        InboundConfig::UnixSocket(cfg) => Box::new(unix::UnixSocketInbound::try_create_from(
            cfg,
            protocol_config,
        )?),
        _ => unimplemented!(),
    };

    Ok(inbound)
}
