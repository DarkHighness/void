use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{config::Verify, core::tag::TagId};

use super::{parser, ScanMode};

#[derive(Debug, Serialize, Deserialize)]
pub struct UnixSocketConfig {
    pub tag: TagId,
    #[serde(default)]
    pub mode: ScanMode,
    pub parser: parser::ParserConfig,

    pub path: PathBuf,
}

impl Display for UnixSocketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnixSocketConfig {{ tag: {}, mode: {:?}, path: {}, parser: {} }}",
            self.tag,
            self.mode,
            self.path.display(),
            self.parser
        )
    }
}

impl Verify for UnixSocketConfig {
    fn verify(&mut self) -> crate::config::error::Result<()> {
        self.parser.verify()?;

        Ok(())
    }
}
