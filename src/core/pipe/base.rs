use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, error, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::{actor::Actor, types::Record};

#[async_trait]
pub trait Pipe: Actor<Error = super::Error> {}
