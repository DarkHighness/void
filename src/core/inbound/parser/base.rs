use std::{collections::HashMap, sync::Arc};

use crate::core::types::Value;

use super::Result;

pub trait Parser: Send + Sync {
    fn parse(&self, data: String) -> Result<Vec<ParsedRecord>>;
}

pub type ParsedRecord = HashMap<Arc<str>, Value>;
