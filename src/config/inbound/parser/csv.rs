use std::{collections::HashSet, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{config::Verify, core::types::DataType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CSVColumn {
    pub name: String,
    pub r#type: DataType,
    #[serde(default)]
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CSVParserConfig {
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    pub columns: Vec<CSVColumn>,
}

impl Display for CSVColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "['{}':'{}']", self.name, self.r#type)
    }
}

impl Display for CSVParserConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|col| col.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "CSVParserConfig {{ delimiter: '{}', columns: {} }}",
            self.delimiter, columns
        )
    }
}

impl Verify for CSVParserConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        for column in &self.columns {
            if column.name.is_empty() {
                return Err(crate::config::Error::InvalidConfig(
                    "CSV column name cannot be empty".to_string(),
                ));
            }
        }

        let is_all_zero = self.columns.iter().all(|c| c.index == 0);
        if is_all_zero {
            self.columns.iter_mut().enumerate().for_each(|(i, col)| {
                col.index = i;
            });
        } else {
            // 检查是否有重复的 Index
            let index_set = self.columns.iter().map(|c| c.index).collect::<HashSet<_>>();
            if index_set.len() != self.columns.len() {
                return Err(crate::config::Error::InvalidConfig(
                    "CSV column index cannot be duplicated".to_string(),
                ));
            }
        }

        Ok(())
    }
}

fn default_delimiter() -> char {
    ','
}
