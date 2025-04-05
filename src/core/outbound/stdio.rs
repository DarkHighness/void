use crate::core::tag::{HasTag, TagId};

pub struct StdioOutbound {
    tag: TagId,
}

impl HasTag for StdioOutbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}
