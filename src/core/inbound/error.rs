use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] crate::core::protocol::Error),
    #[error("Channel closed: {0}")]
    ChannelClosed(TagId),
}

pub type Result<T> = std::result::Result<T, Error>;
