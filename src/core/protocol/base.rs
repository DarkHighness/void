use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::types::Record;

#[async_trait]
pub trait ProtocolParser: Send {
    async fn read_next(&mut self) -> super::Result<Record>;
}
