use async_trait::async_trait;

use crate::core::{
    actor::Actor,
    manager::{TaggedReceiver, TaggedSender},
};

#[async_trait]
pub trait Pipe: Actor<Error = super::Error> {
    fn inbounds(&mut self) -> &mut [TaggedReceiver];
    fn outbound(&mut self) -> &mut TaggedSender;
}
