use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{OutboundTagId, ScopedTagId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Io {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdioOutboundConfig {
    #[serde(default = "default_stdio_tag")]
    pub tag: OutboundTagId,
    pub r#inbounds: Vec<ScopedTagId>,
    #[serde(default = "default_io")]
    pub io: Io,
}

impl Verify for StdioOutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.r#inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("inbounds is empty".into()));
        }

        Ok(())
    }
}

impl StdioOutboundConfig {
    pub fn inbounds(&self) -> Vec<crate::core::tag::TagId> {
        self.r#inbounds.iter().cloned().map(Into::into).collect()
    }
}

fn default_stdio_tag() -> OutboundTagId {
    OutboundTagId::new("stdio")
}

fn default_io() -> Io {
    Io::Stdout
}
