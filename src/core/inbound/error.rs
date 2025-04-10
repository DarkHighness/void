use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    #[diagnostic(transparent)]
    Protocol(#[from] crate::core::protocol::Error),
    #[error(transparent)]
    Nix(#[from] nix::Error),
}

pub type Result<T> = miette::Result<T, Error>;
