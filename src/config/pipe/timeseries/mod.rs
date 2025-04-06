pub mod action;

pub use super::{Error, Result};
pub use action::TimeseriesActionPipeConfig;
use log::warn;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::{
        tag::{PipeTagId, TagId},
        types::Symbol,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    Counter,
    Gauge,
}

impl Default for MetricType {
    fn default() -> Self {
        MetricType::Gauge
    }
}

impl AsRef<str> for MetricType {
    fn as_ref(&self) -> &str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
        }
    }
}

impl ToString for MetricType {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl Into<Symbol> for MetricType {
    fn into(self) -> Symbol {
        Symbol::from(self.as_ref())
    }
}

impl From<&str> for MetricType {
    fn from(s: &str) -> Self {
        match s {
            "counter" => MetricType::Counter,
            "gauge" => MetricType::Gauge,
            _ => MetricType::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValueField {
    pub name: Symbol,
    pub r#type: MetricType,
}

impl<'de> Deserialize<'de> for ValueField {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        let mut parts = str.splitn(2, ':');
        match (parts.next(), parts.next()) {
            (Some(r#type), Some(name)) => {
                let r#type = MetricType::from(r#type);
                let name = Symbol::from(name);

                Ok(ValueField { name, r#type })
            }
            (Some(name), None) => Ok(ValueField {
                name: Symbol::from(name),
                r#type: MetricType::default(),
            }),
            _ => Err(serde::de::Error::custom("invalid value field format")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesPipeConfig {
    #[serde(default = "default_timeseries_tag")]
    pub tag: PipeTagId,
    pub inbounds: Vec<TagId>,

    #[serde(default)]
    pub labels: Vec<Symbol>,

    // If the values field is not set, all the fields except the labels and timestamp will be treated as values.
    #[serde(default)]
    pub values: Option<Vec<ValueField>>,

    // If the timestamp field is not set, the current time will be used.
    #[serde(default)]
    pub timestamp: Option<Symbol>,

    #[serde(default)]
    pub extra_labels: HashMap<Symbol, String>,

    #[serde(default)]
    pub disabled: bool,
}

impl Verify for TimeseriesPipeConfig {
    fn verify(&mut self) -> super::Result<()> {
        if self.inbounds.is_empty() {
            return Err(super::Error::InvalidConfig("inbounds is empty".into()));
        }

        if self.labels.is_empty() {
            return Err(super::Error::InvalidConfig("labels is empty".into()));
        }

        match self.values {
            Some(ref values) => {
                if values.is_empty() {
                    return Err(super::Error::InvalidConfig("values is empty".into()));
                }
            }
            None => {
                warn!("values is not set, all fields except the labels and timestamp will be treated as values");
            }
        }

        Ok(())
    }
}

impl TimeseriesPipeConfig {}

fn default_timeseries_tag() -> PipeTagId {
    PipeTagId::new("timeseries")
}
