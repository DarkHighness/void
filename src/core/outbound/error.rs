use miette::Diagnostic;
use thiserror::Error;

use crate::core::tag::TagId;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Prometheus(#[from] super::prometheus::Error),
    #[error(transparent)]
    Recv(#[from] crate::utils::recv::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    #[diagnostic(transparent)]
    ParquetConv(#[from] crate::core::types::conv::parquet::Error),
}

pub type Result<T> = miette::Result<T, Error>;
