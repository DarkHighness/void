use log::warn;
use serde::{Deserialize, Serialize};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    #[serde(default = "default_serial_mode")]
    pub use_serial_mode: bool,
}

fn default_channel_buffer_size() -> usize {
    16
}

fn default_serial_mode() -> bool {
    true
}

pub static GLOBAL_CONFIG: once_cell::sync::OnceCell<GlobalConfig> =
    once_cell::sync::OnceCell::new();

pub fn channel_buffer_size() -> usize {
    GLOBAL_CONFIG
        .get()
        .map_or(default_channel_buffer_size(), |config| {
            config.channel_buffer_size
        })
}

pub fn use_serial_mode() -> bool {
    GLOBAL_CONFIG
        .get()
        .map_or(default_serial_mode(), |config| config.use_serial_mode)
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: default_channel_buffer_size(),
            use_serial_mode: default_serial_mode(),
        }
    }
}

impl Verify for GlobalConfig {
    fn verify(&mut self) -> super::Result<()> {
        warn!("Global Settings: ");
        warn!("  - channel_buffer_size: {}", self.channel_buffer_size);
        warn!("  - use_serial_mode: {}", self.use_serial_mode);

        Ok(())
    }
}
