use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Primitive {
    Null,
    String,
    Int,
    Float,
    Bool,
    #[serde(rename = "datetime")]
    DateTime,
}

pub const NULL_TYPE: &'static str = "Null";
pub const STRING_TYPE: &'static str = "String";
pub const INT_TYPE: &'static str = "Int";
pub const FLOAT_TYPE: &'static str = "Float";
pub const BOOL_TYPE: &'static str = "Bool";
pub const DATETIME_TYPE: &'static str = "Datetime";

impl Primitive {
    pub fn as_str(&self) -> &'static str {
        match self {
            Primitive::Null => NULL_TYPE,
            Primitive::String => STRING_TYPE,
            Primitive::Int => INT_TYPE,
            Primitive::Float => FLOAT_TYPE,
            Primitive::Bool => BOOL_TYPE,
            Primitive::DateTime => DATETIME_TYPE,
        }
    }
}

impl Display for Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Primitive::Null => write!(f, "null"),
            Primitive::String => write!(f, "string"),
            Primitive::Int => write!(f, "int"),
            Primitive::Float => write!(f, "float"),
            Primitive::Bool => write!(f, "bool"),
            Primitive::DateTime => write!(f, "datetime"),
        }
    }
}
