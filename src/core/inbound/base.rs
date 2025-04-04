use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::core::tag::HasTag;

#[async_trait]
pub trait Inbound: HasTag + Send {
    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()>;
}
