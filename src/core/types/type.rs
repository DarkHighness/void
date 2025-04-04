use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use chrono::TimeZone;
use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    String,
    Int,
    Float,
    Bool,
    #[serde(rename = "datetime")]
    DateTime,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::Bool => write!(f, "bool"),
            DataType::DateTime => write!(f, "datetime"),
        }
    }
}
