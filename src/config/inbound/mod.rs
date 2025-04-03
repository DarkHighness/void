pub mod parser;

use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    Line,
    Full,
}

impl Default for ScanMode {
    fn default() -> Self {
        ScanMode::Line
    }
}

impl Display for ScanMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanMode::Line => write!(f, "line"),
            ScanMode::Full => write!(f, "full"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InboundConfig {
    #[serde(rename = "unix_socket")]
    UnixSocket {
        tag: TagId,
        #[serde(default)]
        mode: ScanMode,
        path: PathBuf,

        parser: parser::ParserConfig,
    },
}

impl Display for InboundConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InboundConfig::UnixSocket {
                tag,
                mode,
                path,
                parser,
            } => {
                write!(
                    f,
                    "UnixSocket {{ tag: {}, mode: {}, path: {}, parser: {} }}",
                    tag,
                    mode,
                    path.display(),
                    parser
                )
            }
        }
    }
}

impl HasTag for InboundConfig {
    fn tag(&self) -> TagId {
        match self {
            InboundConfig::UnixSocket { tag, .. } => tag.clone(),
        }
    }
}

impl Verify for InboundConfig {
    fn verify(&mut self) -> super::error::Result<()> {
        match self {
            InboundConfig::UnixSocket {
                tag: _,
                path: _,
                mode: _,
                parser,
            } => parser.verify(),
        }
    }
}
