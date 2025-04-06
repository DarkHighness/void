use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid number format: {0}, expected: number or number unit")]
    InvalidNumberFormat(String),
    #[error("Invalid bool format: {0}, expected: true, false, yes, no, on, off, active, inactive, not active")]
    InvalidBoolFormat(String),
    #[error("Unknown datetime format {0}")]
    UnknownDatetimeFormat(String),
    #[error("Non-unique timestamp zone mapping: {0}")]
    NonUniqueTimestampZoneMapping(i64),
    #[error("Invalid value type: {0}")]
    InvalidValueType(String),
    #[error("Unexpected value type, expect {0}, got {1}")]
    UnexpectedType(&'static str, &'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
