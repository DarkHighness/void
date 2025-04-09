use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inbound error: {0}")]
    Inbound(#[from] crate::core::inbound::Error),
    #[error("Outbound error: {0}")]
    Outbound(#[from] crate::core::outbound::Error),
    #[error("Pipe error: {0}")]
    Pipe(#[from] crate::core::pipe::Error),
}

pub type Result<T> = miette::Result<T, Error>;
