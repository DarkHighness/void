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
    #[serde(rename = "timeseries_annotate")]
    TimeseriesAnnotate(timeseries::TimeseriesAnnotatePipeConfig),
}

impl Verify for PipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        match self {
            PipeConfig::Timeseries(config) => config.verify(),
            PipeConfig::TimeseriesAnnotate(config) => config.verify(),
        }
    }
}

impl HasTag for PipeConfig {
    fn tag(&self) -> &TagId {
        match self {
            PipeConfig::Timeseries(cfg) => &cfg.tag,
            PipeConfig::TimeseriesAnnotate(cfg) => &cfg.tag,
        }
    }
}

impl PipeConfig {
    pub fn disabled(&self) -> bool {
        match self {
            PipeConfig::Timeseries(cfg) => cfg.disabled,
            PipeConfig::TimeseriesAnnotate(cfg) => cfg.disabled,
        }
    }

    pub fn channel_scale_factor(&self) -> usize {
        match self {
            PipeConfig::Timeseries(cfg) => cfg.channel_scale_factor(),
            PipeConfig::TimeseriesAnnotate(cfg) => cfg.channel_scale_factor(),
        }
    }
}
