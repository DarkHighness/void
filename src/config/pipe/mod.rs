// pub mod csv;

// use std::fmt::Display;

// use serde::{Deserialize, Serialize};

// use super::Verify;

// #[derive(Debug, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// #[serde(tag = "type")]
// pub enum PipeConfig {
//     #[serde(rename = "csv")]
//     CSV(csv::CSVPipeConfig),
// }

// impl Display for PipeConfig {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             PipeConfig::CSV(config) => write!(f, "CSVParserConfig {{ {} }}", config),
//         }
//     }
// }

// impl Verify for PipeConfig {
//     fn verify(&mut self) -> crate::config::Result<()> {
//         match self {
//             PipeConfig::CSV(config) => config.verify(),
//         }
//     }
// }
