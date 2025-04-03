use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::watch::error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    CSV(#[from] csv::Error),
    #[error(transparent)]
    Void(#[from] crate::core::types::Error),
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
}

pub type Result<T> = std::result::Result<T, Error>;
