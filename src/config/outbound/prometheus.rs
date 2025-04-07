use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::{env::Env, Verify},
    core::tag::{OutboundTagId, TagId},
};

use super::auth::AuthConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusOutboundConfig {
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

    #[serde(default = "default_prometheus_outbound_buffer_size")]
    pub buffer_size: usize,
}

impl PrometheusOutboundConfig {}

impl Verify for PrometheusOutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.address.is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "address"));
        }

        if self.inbounds.is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "inbounds"));
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
        None => Ok(default_prometheus_outbound_window_interval()),
    }
}

fn default_prometheus_outbound_window_interval() -> std::time::Duration {
    std::time::Duration::from_secs(5)
}

// We use a large buffer size and a long window interval to avoid
// out of order time series data.
// Which is sometimes acceptable, or you should enable serial mode in config
fn default_prometheus_outbound_buffer_size() -> usize {
    8192 * 8 * 16
}
