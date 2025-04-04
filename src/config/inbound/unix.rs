use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::tag::{HasTag, InboundTagId, ProtocolTagId, TagId},
};

use super::ScanMode;

#[derive(Debug, Serialize, Deserialize)]
pub struct UnixSocketConfig {
    pub tag: InboundTagId,
    #[serde(default)]
    pub mode: ScanMode,
    pub path: PathBuf,
    pub protocol: ProtocolTagId,
}

impl Display for UnixSocketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnixSocketConfig {{ tag: {}, mode: {:?}, path: {}}}",
            self.tag,
            self.mode,
            self.path.display(),
        )
    }
}

impl Verify for UnixSocketConfig {
    fn verify(&mut self) -> crate::config::error::Result<()> {
        Ok(())
    }
}
