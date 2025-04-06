use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error(transparent)]
    Type(#[from] crate::core::types::Error),
    #[error(transparent)]
    Reqwuest(#[from] reqwest::Error),
    #[error(transparent)]
    Snap(#[from] snap::Error),
    #[error(transparent)]
    Recv(#[from] crate::utils::recv::Error),
    #[error("Request error: {0}")]
    RequestError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
