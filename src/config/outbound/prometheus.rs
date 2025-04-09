use serde::{Deserialize, Serialize};

use crate::{
    config::{env::Env, Verify},
    core::tag::{OutboundTagId, TagId},
};

use super::auth::AuthConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusOutboundConfig {
    #[serde(default = "default_prometheus_tag")]
    pub tag: OutboundTagId,
    pub address: Env<String>,

    #[serde(default)]
    pub auth: AuthConfig,

    pub inbounds: Vec<TagId>,

    #[serde(default)]
    pub disabled: bool,

    #[serde(default = "default_prometheus_outbound_recv_timeout")]
    #[serde(deserialize_with = "crate::utils::parse_duration")]
    pub recv_timeout: std::time::Duration,

    #[serde(default = "default_prometheus_outbound_recv_buffer_size")]
    pub recv_buffer_size: usize,
}

impl PrometheusOutboundConfig {
    pub fn channel_scale_factor(&self) -> usize {
        8
    }
}

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

fn default_prometheus_outbound_recv_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(5)
}

// We use a large buffer size and a long window interval to avoid
// out of order time series data.
// Which is sometimes acceptable, or you should enable serial mode in config
fn default_prometheus_outbound_recv_buffer_size() -> usize {
    64 * 8192
}
