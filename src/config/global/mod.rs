use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
}

fn default_channel_buffer_size() -> usize {
    256
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

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: default_channel_buffer_size(),
        }
    }
}
