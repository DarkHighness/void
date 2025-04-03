use crate::core::tag::HasTag;

pub trait Outbound: HasTag + Send {}
