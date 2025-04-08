use log::warn;
use serde::{Deserialize, Serialize};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_channel_buffer_size")]
    pub inbound_channel_buffer_size: usize,
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    #[serde(default)]
    pub time_tracing: bool,
}

fn default_channel_buffer_size() -> usize {
    128
}

pub static GLOBAL_CONFIG: once_cell::sync::OnceCell<GlobalConfig> =
    once_cell::sync::OnceCell::new();

pub fn inbound_channel_buffer_size() -> usize {
    GLOBAL_CONFIG
        .get()
        .map_or(default_channel_buffer_size(), |config| {
            config.inbound_channel_buffer_size
        })
}

pub fn channel_buffer_size() -> usize {
    GLOBAL_CONFIG
        .get()
        .map_or(default_channel_buffer_size(), |config| {
            config.channel_buffer_size
        })
}

pub fn use_time_tracing() -> bool {
    GLOBAL_CONFIG
        .get()
        .map_or(false, |config| config.time_tracing)
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            inbound_channel_buffer_size: default_channel_buffer_size(),
            channel_buffer_size: default_channel_buffer_size(),
            time_tracing: false,
        }
    }
}

impl Verify for GlobalConfig {
    fn verify(&mut self) -> super::Result<()> {
        warn!("Global Settings: ");
        warn!("  - channel_buffer_size: {}", self.channel_buffer_size);
        warn!("  - time_tracing: {}", self.time_tracing);

        Ok(())
    }
}
