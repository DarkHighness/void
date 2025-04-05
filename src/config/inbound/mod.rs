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
}

impl InboundConfig {
    pub fn protocol(&self) -> TagId {
        match self {
            InboundConfig::UnixSocket(cfg) => From::from(&cfg.protocol),
        }
    }
}

impl Display for InboundConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InboundConfig::UnixSocket(cfg) => write!(f, "{}", cfg),
        }
    }
}

impl HasTag for InboundConfig {
    fn tag(&self) -> &TagId {
        match self {
            InboundConfig::UnixSocket(cfg) => &cfg.tag,
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
        }
    }
}
