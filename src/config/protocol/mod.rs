pub mod csv;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::{protocol::ProtocolParser, tag::HasTag};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ProtocolConfig {
    #[serde(rename = "csv")]
    CSV(csv::CSVProtocolConfig),
}

impl Display for ProtocolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolConfig::CSV(config) => write!(f, "CSVParserConfig {{ {} }}", config),
        }
    }
}

impl Verify for ProtocolConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        match self {
            ProtocolConfig::CSV(config) => config.verify(),
        }
    }
}

impl HasTag for ProtocolConfig {
    fn tag(&self) -> crate::core::tag::TagId {
        match self {
            ProtocolConfig::CSV(config) => From::from(&config.tag),
        }
    }
}
