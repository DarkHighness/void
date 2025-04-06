use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::{env::Env, Verify},
    core::tag::{OutboundTagId, TagId},
};

use super::auth::AuthConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default = "default_prometheus_tag")]
    pub tag: OutboundTagId,
    pub address: Env<String>,

    #[serde(deserialize_with = "parse_duration")]
    pub interval: std::time::Duration,

    #[serde(default)]
    pub auth: AuthConfig,

    pub inbounds: Vec<TagId>,

    #[serde(default)]
    pub disabled: bool,
}

impl PrometheusConfig {}

impl Verify for PrometheusConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.address.is_empty() {
            return Err(super::Error::InvalidConfig("address is empty".into()));
        }

        if self.inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("inbounds is empty".into()));
        }

        Ok(())
    }
}

fn default_prometheus_tag() -> OutboundTagId {
    OutboundTagId::new("prometheus")
}

fn parse_duration<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        Some(ref s) => match go_parse_duration::parse_duration(&s) {
            Ok(duration) => Ok(Duration::from_nanos(duration as u64)),
            Err(_) => Err(serde::de::Error::custom(format!(
                "failed to parse duration: {}",
                s
            ))),
        },
        None => Ok(std::time::Duration::from_secs(1)),
    }
}
