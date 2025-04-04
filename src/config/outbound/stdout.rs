use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{OutboundTagId, ScopedTagId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdoutOutboundConfig {
    #[serde(default = "default_stdout_tag")]
    pub tag: OutboundTagId,
    pub r#inbounds: Vec<ScopedTagId>,
}

impl Verify for StdoutOutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.r#inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("inbounds is empty".into()));
        }

        Ok(())
    }
}

impl StdoutOutboundConfig {
    pub fn inbounds(&self) -> Vec<crate::core::tag::TagId> {
        self.r#inbounds.iter().cloned().map(Into::into).collect()
    }
}

fn default_stdout_tag() -> OutboundTagId {
    OutboundTagId::new("stdout")
}
