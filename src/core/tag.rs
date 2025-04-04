use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagId {
    scope: &'static str,
    name: Arc<str>,
}

pub trait HasTag {
    fn tag(&self) -> TagId;
}

pub trait ScopedTagId {}

pub const INBOUND_TAG_SCOPE: &str = "inbound";
pub const OUTBOUND_TAG_SCOPE: &str = "outbound";
pub const PROTOCOL_TAG_SCOPE: &str = "protocol";

pub struct InboundTagId(TagId);
pub struct OutboundTagId(TagId);
pub struct ProtocolTagId(TagId);

impl ScopedTagId for InboundTagId {}
impl ScopedTagId for OutboundTagId {}
impl ScopedTagId for ProtocolTagId {}

macro_rules! impl_serde_for_scoped_tag_id {
    ($tag_id:ty, $scope:ident) => {
        impl<'de> Deserialize<'de> for $tag_id {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let name = String::deserialize(deserializer)?;
                Ok(Self(TagId {
                    scope: $scope,
                    name: Arc::from(name),
                }))
            }
        }

        impl Serialize for $tag_id {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0.name)
            }
        }

        impl From<$tag_id> for TagId {
            fn from(tag_id: $tag_id) -> Self {
                tag_id.0
            }
        }

        impl From<&$tag_id> for TagId {
            fn from(tag_id: &$tag_id) -> Self {
                tag_id.0.clone()
            }
        }

        impl Clone for $tag_id {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }

        impl std::fmt::Display for $tag_id {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0.name)
            }
        }

        impl std::fmt::Debug for $tag_id {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}({})", stringify!($tag_id), self.0.name)
            }
        }
    };
}

impl_serde_for_scoped_tag_id!(InboundTagId, INBOUND_TAG_SCOPE);
impl_serde_for_scoped_tag_id!(OutboundTagId, OUTBOUND_TAG_SCOPE);
impl_serde_for_scoped_tag_id!(ProtocolTagId, PROTOCOL_TAG_SCOPE);

impl std::fmt::Display for TagId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

pub fn find_duplicate_tags<T>(tags: &[T]) -> Option<Vec<TagId>>
where
    T: HasTag,
{
    let mut unique_tags = HashSet::new();
    let mut duplicates = HashSet::new();

    for tag in tags {
        if !unique_tags.insert(tag.tag()) {
            duplicates.insert(tag.tag());
        }
    }

    if duplicates.is_empty() {
        return None;
    }

    Some(duplicates.into_iter().collect())
}
