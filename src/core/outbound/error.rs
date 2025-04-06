use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inbound {1} of {0} has been closed")]
    InboundClosed(TagId, TagId),
}

pub type Result<T> = std::result::Result<T, Error>;
