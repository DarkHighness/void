use std::{collections::HashMap, fmt::Display};

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{
    config::Verify,
    core::{
        tag::{HasTag, ProtocolTagId, TagId},
        types::{Primitive, Symbol},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphiteProtocolConfig {
    pub tag: ProtocolTagId,
    pub attributes: Option<HashMap<String, Primitive>>,
}

impl Verify for GraphiteProtocolConfig {
    fn verify(&mut self) -> crate::config::Result<()> {
        Ok(())
    }
}

impl HasTag for GraphiteProtocolConfig {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

impl Display for GraphiteProtocolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GraphiteParserConfig {{ tag: {} }}", self.tag())
    }
}
