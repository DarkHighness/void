use async_trait::async_trait;

#[async_trait]
pub trait Component: Send + Sync {
    type T;
    type Error;

    async fn poll_async(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> Result<Self::T, Self::Error>;

    fn poll(&mut self, ctx: tokio_util::sync::CancellationToken) -> Result<Self::T, Self::Error> {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(self.poll_async(ctx))
    }
}
