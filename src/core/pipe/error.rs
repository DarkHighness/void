use miette::Diagnostic;
use thiserror::Error;

use crate::core::types::Record;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error("Invalid action: {0}")]
    InvalidAction(String),
    #[error("Field not found: {0}")]
    FieldNotFound(&'static str),
    #[error(transparent)]
    TypeError(#[from] crate::core::types::Error),
    #[error(transparent)]
    Recv(#[from] crate::utils::recv::Error),
    #[error(transparent)]
    Send(#[from] tokio::sync::broadcast::error::SendError<Record>),
}

pub type Result<T> = std::result::Result<T, Error>;
