pub mod csv;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::config::Verify;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ParserConfig {
    #[serde(rename = "csv")]
    CSV(csv::CSVParserConfig),
}

impl Display for ParserConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserConfig::CSV(config) => write!(f, "CSVParserConfig {{ {} }}", config),
        }
    }
}

impl Verify for ParserConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        match self {
            ParserConfig::CSV(config) => config.verify(),
        }
    }
}
