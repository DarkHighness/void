use async_trait::async_trait;

use crate::core::actor::Actor;

#[async_trait]

pub trait Inbound: Actor<Error = super::Error> {}
