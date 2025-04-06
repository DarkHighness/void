use serde::{Deserialize, Serialize};

use crate::config::env::Env;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum AuthConfig {
    /// No authentication
    None,
    /// Basic authentication
    Basic {
        /// Username
        username: Env<String>,
        /// Password
        password: Env<String>,
    },
    /// Bearer token authentication
    Bearer {
        /// Token
        token: Env<String>,
    },
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig::None
    }
}
