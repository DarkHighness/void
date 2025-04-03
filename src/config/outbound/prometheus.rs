use serde::{Deserialize, Serialize};

use crate::config::env::Env;

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    pub address: Env<String>,

    pub auth_name: Option<Env<String>>,
    pub auth_password: Option<Env<String>>,
    pub auth_token: Option<Env<String>>,
}
