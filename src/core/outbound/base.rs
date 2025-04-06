use async_trait::async_trait;

use crate::core::{actor::Actor, manager::TaggedReceiver, types::Record};

#[async_trait]
pub trait Outbound: Actor<Error = super::Error> {
    fn inbounds(&mut self) -> &mut [TaggedReceiver];

    async fn recv(&mut self, timeout: std::time::Duration) -> super::Result<Record> {
        todo!()
    }

    async fn recv_batch(&mut self, timeout: std::time::Duration) -> super::Result<Vec<Record>> {
        todo!()
    }
}
