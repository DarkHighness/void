use miette::Diagnostic;
use thiserror::Error;

use crate::core::{tag::TagId, types::Symbol};

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error("{0} failed to receive from inbound: {0}")]
    InboundRecvError(String),
    #[error("{0} failed to send to outbound: {0}")]
    OutboundSendError(String),
    #[error("Inbound channel {0} has been lagged for {1} records")]
    ChannelLagged(usize, u64),
    #[error("All inbound channel of {0} has been closed")]
    AllInboundsClosed(TagId),
    #[error("Field not found: {0}")]
    FieldNotFound(Symbol),
    #[error(transparent)]
    TypeError(#[from] crate::core::types::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
