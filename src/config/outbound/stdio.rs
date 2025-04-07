use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{OutboundTagId, TagId},
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
    pub r#inbounds: Vec<TagId>,
    #[serde(default = "default_io")]
    pub io: Io,

    #[serde(default)]
    pub disabled: bool,
}

impl Verify for StdioOutboundConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.r#inbounds.is_empty() {
            return Err(super::Error::EmptyField((&self.tag).into(), "inbounds"));
        }

        Ok(())
    }
}

impl StdioOutboundConfig {}

fn default_stdio_tag() -> OutboundTagId {
    OutboundTagId::new("stdio")
}

fn default_io() -> Io {
    Io::Stdout
}
