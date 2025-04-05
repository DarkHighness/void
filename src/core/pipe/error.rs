use miette::Diagnostic;
use thiserror::Error;

use crate::core::types::{Record, Symbol};

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error("Failed to receive from channel: {0}")]
    ChannelRecvError(String),
    #[error("Failed to send to channel: {0}")]
    ChannelSendError(String),
    #[error("Field not found: {0}")]
    FieldNotFound(Symbol),
}

pub type Result<T> = std::result::Result<T, Error>;
