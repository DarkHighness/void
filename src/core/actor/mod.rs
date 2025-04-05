use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::tag::HasTag;

#[async_trait]
pub trait Actor: HasTag + Send {
    type Error;
    async fn poll(&mut self, ctx: CancellationToken) -> std::result::Result<(), Self::Error>;
}
