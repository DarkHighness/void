use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum AuthConfig {
    /// No authentication
    None,
    /// Basic authentication
    Basic {
        /// Username
        username: String,
        /// Password
        password: String,
    },
    /// Bearer token authentication
    Bearer {
        /// Token
        token: String,
    },
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig::None
    }
}
