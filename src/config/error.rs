use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Duplicate tags found: {0:?}")]
    DuplicateTags(Vec<TagId>),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Invalid config")]
    InvalidConfig(String),
    #[error("Invalid config file format: {0}")]
    InvalidConfigFileFormat(String),
    #[error(transparent)]
    InvalidJsonConfig(#[from] serde_json::Error),
    #[error(transparent)]
    InvalidTomlConfig(#[from] toml::de::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
