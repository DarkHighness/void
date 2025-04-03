use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inbound Error: {0}")]
    Inbound(#[from] super::inbound::error::Error),
    #[error("Cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;
