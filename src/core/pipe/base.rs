use crate::core::{
    tag::{HasTag, TagId},
    types::Record,
};

use super::Result;

pub trait Pipe: HasTag + Send + Sync {
    fn pipe(&self, data: String) -> Result<Vec<Record>>;
}
