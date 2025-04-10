pub mod named_pipe;
pub mod unix;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

pub use super::Result;
use super::Verify;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InboundConfig {
    #[serde(rename = "unix_socket")]
    UnixSocket(unix::UnixSocketConfig),
    #[serde(rename = "named_pipe")]
    NamedPipe(named_pipe::NamedPipeConfig),
}

impl InboundConfig {
    pub fn protocol(&self) -> TagId {
        match self {
            InboundConfig::UnixSocket(cfg) => From::from(&cfg.protocol),
            InboundConfig::NamedPipe(cfg) => From::from(&cfg.protocol),
        }
    }

    pub fn disabled(&self) -> bool {
        match self {
            InboundConfig::UnixSocket(cfg) => cfg.disabled,
            InboundConfig::NamedPipe(cfg) => cfg.disabled,
        }
    }
}

impl Display for InboundConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InboundConfig::UnixSocket(cfg) => write!(f, "{}", cfg),
            InboundConfig::NamedPipe(cfg) => write!(f, "{}", cfg),
        }
    }
}

impl HasTag for InboundConfig {
    fn tag(&self) -> &TagId {
        match self {
            InboundConfig::UnixSocket(cfg) => &cfg.tag,
            InboundConfig::NamedPipe(cfg) => &cfg.tag,
        }
    }
}

impl Verify for InboundConfig {
    fn verify(&mut self) -> Result<()> {
        match self {
            InboundConfig::UnixSocket(cfg) => {
                cfg.verify()?;
                Ok(())
            }
            InboundConfig::NamedPipe(cfg) => {
                cfg.verify()?;
                Ok(())
            }
        }
    }
}
