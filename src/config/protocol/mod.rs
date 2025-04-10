pub mod csv;
pub mod graphite;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::tag::{HasTag, TagId};

use super::Verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum ProtocolConfig {
    #[serde(rename = "csv")]
    CSV(csv::CSVProtocolConfig),
    #[serde(rename = "graphite")]
    Graphite(graphite::GraphiteProtocolConfig),
}

impl Display for ProtocolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolConfig::CSV(config) => write!(f, "CSVParserConfig {{ {} }}", config),
            ProtocolConfig::Graphite(config) => write!(f, "GraphiteParserConfig {{ {} }}", config),
        }
    }
}

impl Verify for ProtocolConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        match self {
            ProtocolConfig::CSV(config) => config.verify(),
            ProtocolConfig::Graphite(config) => config.verify(),
        }
    }
}

impl HasTag for ProtocolConfig {
    fn tag(&self) -> &TagId {
        match self {
            ProtocolConfig::CSV(config) => &config.tag,
            ProtocolConfig::Graphite(config) => &config.tag,
        }
    }
}
