use async_trait::async_trait;

use crate::core::tag::HasTag;

#[async_trait]
pub trait Inbound: HasTag + Send {
    async fn poll_async(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> Result<(), super::Error>;

    fn poll(&mut self, ctx: tokio_util::sync::CancellationToken) -> Result<(), super::Error> {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(self.poll_async(ctx))
    }
}
