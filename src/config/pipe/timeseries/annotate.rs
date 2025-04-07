use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{PipeTagId, TagId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesAnnotatePipeConfig {
    #[serde(default = "default_timeseries_annotate_tag")]
    pub tag: PipeTagId,
    pub data_inbounds: Vec<TagId>,
    pub control_inbounds: Vec<TagId>,

    #[serde(default)]
    pub disabled: bool,

    #[serde(default = "default_timeseries_annotate_pipe_interval")]
    pub interval: Duration,

    #[serde(default = "default_timeseries_annotate_pipe_buffer_size")]
    pub buffer_size: usize,
}

impl Verify for TimeseriesAnnotatePipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.data_inbounds.is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "inbounds"));
        }

        if self.control_inbounds.is_empty() {
            return Err(super::Error::EmptyField(
                (&self.tag).into(),
                "control_inbounds",
            ));
        }
        Ok(())
    }
}

fn default_timeseries_annotate_tag() -> PipeTagId {
    PipeTagId::new("timeseries_annotate")
}

fn default_timeseries_annotate_pipe_interval() -> Duration {
    Duration::from_millis(5)
}

fn default_timeseries_annotate_pipe_buffer_size() -> usize {
    32
}
