use serde::{Deserialize, Serialize};

use crate::core::tag::HasTag;

use super::Verify;

pub use super::{Error, Result};

pub mod prometheus;
pub mod stdout;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum OutboundConfig {
    #[serde(rename = "stdout")]
    Stdout(stdout::StdoutOutboundConfig),
    Prometheus(prometheus::PrometheusConfig),
}

impl Verify for OutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        Ok(())
    }
}

impl HasTag for OutboundConfig {
    fn tag(&self) -> crate::core::tag::TagId {
        match self {
            OutboundConfig::Stdout(cfg) => From::from(&cfg.tag),
            OutboundConfig::Prometheus(cfg) => From::from(&cfg.tag),
        }
    }
}

impl OutboundConfig {
    pub fn inbounds(&self) -> Vec<crate::core::tag::TagId> {
        match self {
            OutboundConfig::Stdout(cfg) => cfg.inbounds(),
            OutboundConfig::Prometheus(cfg) => cfg.inbounds(),
        }
    }
}
