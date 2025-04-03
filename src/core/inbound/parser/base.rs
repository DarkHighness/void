use crate::core::types::Record;

use super::Result;

pub trait Parser: Send + Sync {
    fn parse(&self, data: String) -> Result<Vec<Record>>;
}
