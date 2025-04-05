use async_trait::async_trait;

use crate::core::actor::Actor;

#[async_trait]
pub trait Outbound: Actor<Error = super::Error> {}
