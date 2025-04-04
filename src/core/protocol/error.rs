use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("EOF")]
    EOF,
    #[error("Mismatched format: {0}")]
    MismatchedFormat(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn is_eof(&self) -> bool {
        matches!(self, Error::EOF)
    }
}
