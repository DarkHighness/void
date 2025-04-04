use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{InboundTagId, ProtocolTagId, ScopedTagId, TagId},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct UnixSocketConfig {
    #[serde(default = "default_unix_socket_tag")]
    pub tag: InboundTagId,
    pub path: PathBuf,
    pub protocol: ProtocolTagId,
}

impl Display for UnixSocketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnixSocketConfig {{ tag: {}, path: {}}}",
            self.tag,
            self.path.display(),
        )
    }
}

impl Verify for UnixSocketConfig {
    fn verify(&mut self) -> super::Result<()> {
        Ok(())
    }
}

fn default_unix_socket_tag() -> InboundTagId {
    InboundTagId::new("unix_socket")
}
