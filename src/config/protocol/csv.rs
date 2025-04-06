use std::{collections::HashSet, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::{
        tag::ProtocolTagId,
        types::{DataType, Symbol},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CSVField {
    pub name: Symbol,
    pub r#type: DataType,
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CSVProtocolConfig {
    #[serde(default = "default_csv_tag")]
    pub tag: ProtocolTagId,

    #[serde(default)]
    pub has_header: bool,

    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    pub fields: Vec<CSVField>,
    #[serde(default)]
    pub num_fields: usize,
}

impl Display for CSVField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "['{}':'{}']", self.name, self.r#type)
    }
}

impl Display for CSVProtocolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields = self
            .fields
            .iter()
            .map(|col| col.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "CSVParserConfig {{ delimiter: '{}', has_header: {}, fields: {}, num_fields: {} }}",
            self.delimiter, self.has_header, fields, self.num_fields
        )
    }
}

impl Verify for CSVProtocolConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        // fill in the index if all are zero
        let is_all_zero = self.fields.iter().all(|c| c.index == 0);
        if is_all_zero {
            self.fields.iter_mut().enumerate().for_each(|(i, col)| {
                col.index = i;
            });
        } else {
            // 检查是否有重复的 Index
            let index_set = self.fields.iter().map(|c| c.index).collect::<HashSet<_>>();
            if index_set.len() != self.fields.len() {
                return Err(crate::config::Error::InvalidConfig(
                    "CSV field index cannot be duplicated".to_string(),
                ));
            }
        }

        // fill in num_fields if it is zero
        if self.num_fields == 0 {
            let max_index = self.fields.iter().map(|e| e.index).max();
            match max_index {
                Some(max_index) => self.num_fields = max_index + 1,
                None => {
                    return Err(crate::config::Error::InvalidConfig(
                        "CSV field count cannot be zero".to_string(),
                    ));
                }
            }
        }

        if self.fields.len() > self.num_fields {
            return Err(crate::config::Error::InvalidConfig(format!(
                "CSV field count: {} cannot be greater than num_fields: {}",
                self.fields.len(),
                self.num_fields
            )));
        }

        for field in &self.fields {
            if field.name.is_empty() {
                return Err(crate::config::Error::InvalidConfig(
                    "CSV field name cannot be empty".to_string(),
                ));
            }
        }

        // Optional fields should be at the end of the list
        let mut optional = false;
        for field in &self.fields {
            if field.optional {
                optional = true;
            } else if optional {
                return Err(crate::config::Error::InvalidConfig(
                    "CSV optional fields should be at the end".to_string(),
                ));
            }
        }

        Ok(())
    }
}

fn default_delimiter() -> char {
    ','
}

fn default_csv_tag() -> ProtocolTagId {
    ProtocolTagId::new("csv")
}
