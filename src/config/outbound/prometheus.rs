use serde::{Deserialize, Serialize};

use crate::{
    config::{env::Env, Verify},
    core::tag::OutboundTagId,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default = "default_prometheus_tag")]
    pub tag: OutboundTagId,
    pub address: Env<String>,

    pub auth_name: Option<Env<String>>,
    pub auth_password: Option<Env<String>>,
    pub auth_token: Option<Env<String>>,
}

impl PrometheusConfig {}

impl Verify for PrometheusConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.address.is_empty() {
            return Err(super::Error::InvalidConfig("address is empty".into()));
        }

        Ok(())
    }
}

fn default_prometheus_tag() -> OutboundTagId {
    OutboundTagId::new("prometheus")
}
