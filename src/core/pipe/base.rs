
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::core::tag::HasTag;

#[async_trait]
pub trait Pipe: HasTag + Send + Sync {
    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()>;
}
