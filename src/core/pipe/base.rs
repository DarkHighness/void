use async_trait::async_trait;

use crate::core::actor::Actor;

#[async_trait]
pub trait Pipe: Actor<Error = super::Error> {}
