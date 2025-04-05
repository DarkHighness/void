mod base;
mod error;
pub mod prometheus;
pub mod stdio;

pub use base::Outbound;
pub use error::{Error, Result};

use crate::config::OutboundConfig;

use super::manager::ChannelGraph;

pub fn try_create_from(
    cfg: OutboundConfig,
    channels: &mut ChannelGraph,
) -> Result<Box<dyn Outbound>> {
    match cfg {
        OutboundConfig::Stdio(cfg) => Ok(Box::new(stdio::StdioOutbound::try_create_from(
            cfg, channels,
        )?)),
        OutboundConfig::Prometheus(cfg) => todo!(),
    }
}
