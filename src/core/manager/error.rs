use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inbound error: {0}")]
    Inbound(#[from] crate::core::inbound::Error),
    #[error("Protocol not found: {0}")]
    ProtocolNotFound(TagId),
    #[error("Pipe error: {0}")]
    Pipe(#[from] crate::core::pipe::Error),
    #[error("Outbound error: {0}")]
    Outbound(#[from] crate::core::outbound::Error),
    #[error("Unknown tag: {0}")]
    UnknownTag(TagId),
    #[error("Unknown tag {0} required by {1}")]
    UnknownTagRequired(TagId, TagId),
    #[error("Cancelled")]
    Cancelled,
    #[error("Duplicate tag: {0}")]
    DuplicateTag(TagId),
}

pub type Result<T> = std::result::Result<T, Error>;
