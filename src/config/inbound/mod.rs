pub mod unix;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    Line,
    Full,
}

impl Default for ScanMode {
    fn default() -> Self {
        ScanMode::Line
    }
}

impl Display for ScanMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanMode::Line => write!(f, "line"),
            ScanMode::Full => write!(f, "full"),
        }
    }
}

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
    fn tag(&self) -> TagId {
        match self {
            InboundConfig::UnixSocket(cfg) => From::from(&cfg.tag),
        }
    }
}

impl Verify for InboundConfig {
    fn verify(&mut self) -> super::error::Result<()> {
        match self {
            InboundConfig::UnixSocket(cfg) => {
                cfg.verify()?;
                Ok(())
            }
        }
    }
}
