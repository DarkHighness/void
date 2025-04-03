use serde::{Deserialize, Serialize};

use crate::core::tag::TagId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeConfig {
    #[serde(default)]
    pub inbounds: Vec<TagId>,
    #[serde(default)]
    pub outbounds: Vec<TagId>,
    #[serde(default)]
    pub stages: Vec<TagId>,
}
