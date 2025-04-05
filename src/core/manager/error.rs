use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Protocol not found: {0}")]
    ProtocolNotFound(TagId),
    #[error("Unknown tag: {0}")]
    UnknownTag(TagId),
    #[error("Unknown tag {0} required by {1}")]
    UnknownTagRequired(TagId, TagId),
    #[error("Duplicate tag: {0}")]
    DuplicateTag(TagId),
    #[error(transparent)]
    Actor(#[from] crate::core::actor::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;
