use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

pub use super::{Error, Result};

pub mod auth;
pub mod prometheus;
pub mod stdio;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum OutboundConfig {
    #[serde(rename = "stdio")]
    Stdio(stdio::StdioOutboundConfig),
    Prometheus(prometheus::PrometheusConfig),
}

impl HasTag for OutboundConfig {
    fn tag(&self) -> &TagId {
        match self {
            OutboundConfig::Stdio(cfg) => &cfg.tag,
            OutboundConfig::Prometheus(cfg) => &cfg.tag,
        }
    }
}

impl OutboundConfig {
    pub fn disabled(&self) -> bool {
        match self {
            OutboundConfig::Stdio(cfg) => cfg.disabled,
            OutboundConfig::Prometheus(cfg) => cfg.disabled,
        }
    }
}

impl Verify for OutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        match self {
            OutboundConfig::Stdio(cfg) => cfg.verify(),
            OutboundConfig::Prometheus(cfg) => cfg.verify(),
        }
    }
}
