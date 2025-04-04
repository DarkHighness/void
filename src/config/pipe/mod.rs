use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

pub mod timeseries;
pub use super::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum PipeConfig {
    #[serde(rename = "timeseries")]
    Timeseries(timeseries::TimeseriesPipeConfig),
}

impl Verify for PipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        match self {
            PipeConfig::Timeseries(config) => config.verify(),
        }
    }
}

impl HasTag for PipeConfig {
    fn tag(&self) -> crate::core::tag::TagId {
        match self {
            PipeConfig::Timeseries(cfg) => From::from(&cfg.tag),
        }
    }
}

impl PipeConfig {
    pub fn inbounds(&self) -> Vec<TagId> {
        match self {
            PipeConfig::Timeseries(cfg) => cfg.inbounds(),
        }
    }
}
