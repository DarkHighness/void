use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Failed to parse: {0}")]
    Parser(#[from] crate::core::inbound::parser::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
