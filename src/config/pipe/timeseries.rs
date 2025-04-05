use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::{
        tag::{PipeTagId, TagId},
        types::Symbol,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesPipeConfig {
    #[serde(default = "default_timeseries_tag")]
    pub tag: PipeTagId,
    pub inbounds: Vec<TagId>,

    #[serde(default)]
    pub labels: Vec<Symbol>,
    pub values: Vec<Symbol>,

    // If the timestamp field is not set, the current time will be used.
    #[serde(default)]
    pub timestamp: Option<Symbol>,

    #[serde(default)]
    pub extra_labels: HashMap<Symbol, String>,
}

impl Verify for TimeseriesPipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("inbounds is empty".into()));
        }

        if self.values.is_empty() {
            return Err(super::Error::InvalidConfig("values is empty".into()));
        }

        if self.labels.is_empty() {
            return Err(super::Error::InvalidConfig("labels is empty".into()));
        }

        Ok(())
    }
}

impl TimeseriesPipeConfig {}

fn default_timeseries_tag() -> PipeTagId {
    PipeTagId::new("timeseries")
}
