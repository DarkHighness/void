use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

pub use super::{Error, Result};

pub mod auth;
pub mod parquet;
pub mod prometheus;
pub mod stdio;

use self::{
    parquet::ParquetOutboundConfig, prometheus::PrometheusOutboundConfig,
    stdio::StdioOutboundConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum OutboundConfig {
    #[serde(rename = "stdio")]
    Stdio(StdioOutboundConfig),
    Prometheus(PrometheusOutboundConfig),
    Parquet(ParquetOutboundConfig),
}

impl HasTag for OutboundConfig {
    fn tag(&self) -> &TagId {
        match self {
            OutboundConfig::Stdio(cfg) => &cfg.tag,
            OutboundConfig::Prometheus(cfg) => &cfg.tag,
            OutboundConfig::Parquet(cfg) => &cfg.tag,
        }
    }
}

impl OutboundConfig {
    pub fn disabled(&self) -> bool {
        match self {
            OutboundConfig::Stdio(cfg) => cfg.disabled,
            OutboundConfig::Prometheus(cfg) => cfg.disabled,
            OutboundConfig::Parquet(cfg) => cfg.disabled,
        }
    }

    pub fn channel_scale_factor(&self) -> usize {
        match self {
            OutboundConfig::Stdio(cfg) => cfg.channel_scale_factor(),
            OutboundConfig::Prometheus(cfg) => cfg.channel_scale_factor(),
            OutboundConfig::Parquet(cfg) => cfg.channel_scale_factor(),
        }
    }
}

impl Verify for OutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        match self {
            OutboundConfig::Stdio(cfg) => cfg.verify(),
            OutboundConfig::Prometheus(cfg) => cfg.verify(),
            OutboundConfig::Parquet(cfg) => cfg.verify(),
        }
    }
}
