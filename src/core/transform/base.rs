use crate::core::types::Record;

pub trait Transform: Send + Sync {
    fn transform(&mut self, record: Record) -> Record;
}
