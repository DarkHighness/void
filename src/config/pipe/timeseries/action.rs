use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{PipeTagId, TagId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesActionPipeConfig {
    #[serde(default = "default_timeseries_action_tag")]
    pub tag: PipeTagId,
    pub data_inbounds: Vec<TagId>,
    pub control_inbounds: Vec<TagId>,

    #[serde(default)]
    pub disabled: bool,
}

impl Verify for TimeseriesActionPipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.data_inbounds.is_empty() {
            return Err(super::Error::InvalidConfig(
                "TimeseriesActionPipeConfig must have at least one data inbound".to_string(),
            ));
        }
        if self.control_inbounds.is_empty() {
            return Err(super::Error::InvalidConfig(
                "TimeseriesActionPipeConfig must have at least one control inbound".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_timeseries_action_tag() -> PipeTagId {
    PipeTagId::new("timeseries_action")
}
