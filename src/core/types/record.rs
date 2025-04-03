use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordType {
    Gauge,
    Counter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub name: String,
    pub r#type: RecordType,
    pub labels: HashMap<String, String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub value: f64,
}
