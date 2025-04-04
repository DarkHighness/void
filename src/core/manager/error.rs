use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inbound error: {0}")]
    Inbound(#[from] super::inbound::Error),
    #[error("Protocol not found: {0}")]
    ProtocolNotFound(TagId),
    #[error("Cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;
