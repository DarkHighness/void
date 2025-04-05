use async_trait::async_trait;
use log::{debug, error};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::{actor::Actor, types::Record};

#[async_trait]

pub trait Inbound: Actor<Error = super::Error> {}
