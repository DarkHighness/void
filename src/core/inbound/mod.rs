use crate::config::inbound::InboundConfig;

pub mod base;
pub mod error;
mod parser;
mod unix;

pub use base::Inbound;
pub use error::Error;
use error::Result;

pub fn try_create_from_config(inbound_config: InboundConfig) -> Result<Box<dyn base::Inbound>> {
    let inbound = match inbound_config {
        InboundConfig::UnixSocket(cfg) => {
            Box::new(unix::UnixSocketInbound::try_create_from_config(cfg)?)
        }
        _ => unimplemented!(),
    };

    Ok(inbound)
}
