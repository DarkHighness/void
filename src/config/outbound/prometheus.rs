use serde::{Deserialize, Serialize};

use crate::{config::env::Env, core::tag::OutboundTagId};

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default = "default_prometheus_tag")]
    pub tag: OutboundTagId,
    pub address: Env<String>,

    pub auth_name: Option<Env<String>>,
    pub auth_password: Option<Env<String>>,
    pub auth_token: Option<Env<String>>,
}

impl PrometheusConfig {
    pub fn inbounds(&self) -> Vec<crate::core::tag::TagId> {
        vec![]
    }
}

fn default_prometheus_tag() -> OutboundTagId {
    OutboundTagId::new("prometheus")
}
