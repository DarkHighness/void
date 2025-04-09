use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Type(#[from] crate::core::types::Error),
    #[error(transparent)]
    #[diagnostic(transparent)]
    Conv(#[from] crate::core::types::conv::prometheus::Error),
    #[error(transparent)]
    Reqwuest(#[from] reqwest::Error),
    #[error(transparent)]
    Snap(#[from] snap::Error),
    #[error(transparent)]
    #[diagnostic(transparent)]
    Recv(#[from] crate::utils::recv::Error),
}

pub type Result<T> = miette::Result<T, Error>;
