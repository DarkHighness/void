mod base;
mod error;
mod timeseries;

pub use base::Pipe;
pub use error::{Error, Result};

use crate::config::pipe::PipeConfig;

use super::manager::ChannelGraph;

pub fn try_create_from(cfg: PipeConfig, channels: &ChannelGraph) -> Result<Box<dyn Pipe>> {
    let pipe: Box<dyn Pipe> = match cfg {
        PipeConfig::Timeseries(cfg) => {
            Box::new(timeseries::TimeseriesPipe::try_create_from(cfg, channels)?)
        }
    };

    Ok(pipe)
}
