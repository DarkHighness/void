use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;
