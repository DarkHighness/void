use serde::{Deserialize, Serialize};

pub mod prometheus;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum OutboundConfig {
    Prometheus(prometheus::PrometheusConfig),
}
