use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::{
        tag::{PipeTagId, ScopedTagId, TagId},
        types::Symbol,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesPipeConfig {
    #[serde(default = "default_timeseries_tag")]
    pub tag: PipeTagId,
    pub data_inbounds: Vec<ScopedTagId>,

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
        if self.data_inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("data_inbounds is empty".into()));
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

impl TimeseriesPipeConfig {
    pub fn inbounds(&self) -> Vec<TagId> {
        self.data_inbounds.iter().cloned().map(Into::into).collect()
    }
}

fn default_timeseries_tag() -> PipeTagId {
    PipeTagId::new("timeseries")
}
