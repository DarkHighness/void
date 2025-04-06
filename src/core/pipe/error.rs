use miette::Diagnostic;
use thiserror::Error;


#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error(transparent)]
    TypeError(#[from] crate::core::types::Error),
    #[error(transparent)]
    Recv(#[from] crate::utils::recv::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
