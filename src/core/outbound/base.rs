use async_trait::async_trait;

use crate::core::{actor::Actor, manager::TaggedReceiver};

#[async_trait]
pub trait Outbound: Actor<Error = super::Error> {
    fn inbounds(&mut self) -> &mut [TaggedReceiver];
}
