use std::thread::JoinHandle;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::core::{
    tag::{HasTag, TagId},
    types::Record,
};

#[async_trait]
pub trait Pipe: HasTag + Send + Sync {
    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()>;
}
