use crate::core::{component::Component, tag::HasTag};

pub trait Inbound: HasTag + Component<T = (), Error = super::error::Error> + Send {}
