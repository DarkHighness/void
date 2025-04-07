use log::warn;
use serde::{Deserialize, Serialize};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    #[serde(default = "default_serial_mode")]
    pub use_serial_mode: bool,
    #[serde(default = "default_rayon_thread_count")]
    pub rayon_thread_count: usize,
}

fn default_channel_buffer_size() -> usize {
    128
}

fn default_serial_mode() -> bool {
    true
}

fn default_rayon_thread_count() -> usize {
    std::cmp::min(num_cpus::get(), 4)
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

pub fn rayon_thread_count() -> usize {
    GLOBAL_CONFIG
        .get()
        .map_or(default_rayon_thread_count(), |config| {
            config.rayon_thread_count
        })
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: default_channel_buffer_size(),
            use_serial_mode: default_serial_mode(),
            rayon_thread_count: default_rayon_thread_count(),
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
