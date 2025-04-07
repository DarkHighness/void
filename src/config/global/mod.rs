use std::path::{Path, PathBuf};

use log::warn;
use serde::{Deserialize, Serialize};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    #[serde(default = "default_serial_mode")]
    pub use_serial_mode: bool,
    #[serde(default)]
    pub time_tracing: Option<PathBuf>,
}

fn default_channel_buffer_size() -> usize {
    128
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

pub fn use_time_tracing() -> bool {
    GLOBAL_CONFIG
        .get()
        .map_or(false, |config| config.time_tracing.is_some())
}

pub fn time_tracing_path() -> &'static Path {
    assert!(
        use_time_tracing(),
        "Time tracing is not enabled, but time_tracing_path() was called"
    );

    GLOBAL_CONFIG
        .get()
        .map_or_else(
            || panic!("Time tracing is not enabled"),
            |config| config.time_tracing.as_ref().unwrap(),
        )
        .as_path()
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: default_channel_buffer_size(),
            use_serial_mode: default_serial_mode(),
            time_tracing: None,
        }
    }
}

impl Verify for GlobalConfig {
    fn verify(&mut self) -> super::Result<()> {
        warn!("Global Settings: ");
        warn!("  - channel_buffer_size: {}", self.channel_buffer_size);
        warn!("  - use_serial_mode: {}", self.use_serial_mode);
        warn!(
            "  - time_tracing: {}",
            self.time_tracing
                .as_ref()
                .map_or("None".to_string(), |path| path
                    .to_string_lossy()
                    .to_string())
        );

        // Remove time_tracing path if it exists
        if let Some(path) = &self.time_tracing {
            if path.exists() {
                std::fs::remove_file(path).map_err(|e| {
                    super::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to remove time tracing file: {}", e),
                    ))
                })?;
            }
        }

        Ok(())
    }
}
