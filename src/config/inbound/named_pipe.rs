use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{InboundTagId, ProtocolTagId},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct NamedPipeConfig {
    #[serde(default = "default_named_pipe_tag")]
    pub tag: InboundTagId,
    pub path: PathBuf,
    pub protocol: ProtocolTagId,
    #[serde(default)]
    pub disabled: bool,
}

impl Display for NamedPipeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NamedPipeConfig {{ tag: {}, path: {}}}",
            self.tag.as_ref(),
            self.path.display(),
        )
    }
}

impl Verify for NamedPipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        Ok(())
    }
}

fn default_named_pipe_tag() -> InboundTagId {
    InboundTagId::new("named_pipe")
}
