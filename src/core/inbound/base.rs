
use crate::core::actor::Actor;

pub trait Inbound: Actor<Error = super::Error> {}
