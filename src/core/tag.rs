use serde::{Deserialize, Serialize};
use std::{fmt::Display, ops::Deref};

use std::collections::HashSet;

use super::types::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagId {
    // These two pointers are static, so we can implement Copy trait for them.
    scope: &'static str,
    name: &'static str,
}

pub trait HasTag {
    fn tag(&self) -> &TagId;
}

impl Display for TagId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.scope, self.name)
    }
}

impl From<&TagId> for Value {
    fn from(tag_id: &TagId) -> Self {
        Value::String(tag_id.to_string().into())
    }
}

pub const INBOUND_TAG_SCOPE: &str = "inbound";
pub const OUTBOUND_TAG_SCOPE: &str = "outbound";
pub const PROTOCOL_TAG_SCOPE: &str = "protocol";
pub const PIPE_TAG_SCOPE: &str = "pipe";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundTagId(TagId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundTagId(TagId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolTagId(TagId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeTagId(TagId);

macro_rules! impl_serde_for_scoped_tag_id {
    ($tag_id:ty, $scope:ident) => {
        impl $tag_id {
            pub fn new(name: &str) -> Self {
                let name = name.to_string();
                let name = name.leak();

                Self(TagId {
                    scope: $scope,
                    name,
                })
            }
        }

        impl<'de> Deserialize<'de> for $tag_id {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let name = String::deserialize(deserializer)?;
                let name = name.leak();
                Ok(Self(TagId {
                    scope: $scope,
                    name,
                }))
            }
        }

        impl Serialize for $tag_id {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let str = format!("{}:{}", self.0.scope, self.0.name);
                serializer.serialize_str(&str)
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

        impl AsRef<TagId> for $tag_id {
            fn as_ref(&self) -> &TagId {
                &self.0
            }
        }

        impl Deref for $tag_id {
            type Target = TagId;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<TagId> for $tag_id {
            fn from(tag_id: TagId) -> Self {
                Self(tag_id)
            }
        }
    };
}

impl_serde_for_scoped_tag_id!(InboundTagId, INBOUND_TAG_SCOPE);
impl_serde_for_scoped_tag_id!(OutboundTagId, OUTBOUND_TAG_SCOPE);
impl_serde_for_scoped_tag_id!(ProtocolTagId, PROTOCOL_TAG_SCOPE);
impl_serde_for_scoped_tag_id!(PipeTagId, PIPE_TAG_SCOPE);

impl Serialize for TagId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = format!("{}:{}", self.scope, self.name);
        serializer.serialize_str(&str)
    }
}

impl<'de> Deserialize<'de> for TagId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        let str = str.leak();
        let parts: Vec<&str> = str.split(':').collect();
        if parts.len() != 2 {
            return Err(serde::de::Error::custom("Invalid ScopedTagId format"));
        }

        let scope = parts[0].to_lowercase();
        let scope_str = scope.as_str();
        let name: &str = parts[1];
        if scope.is_empty() || name.is_empty() {
            return Err(serde::de::Error::custom("Invalid ScopedTagId format"));
        }

        let scope = match scope_str {
            INBOUND_TAG_SCOPE => INBOUND_TAG_SCOPE,
            OUTBOUND_TAG_SCOPE => OUTBOUND_TAG_SCOPE,
            PROTOCOL_TAG_SCOPE => PROTOCOL_TAG_SCOPE,
            PIPE_TAG_SCOPE => PIPE_TAG_SCOPE,
            _ => return Err(serde::de::Error::custom("Invalid ScopedTagId scope")),
        };

        Ok(TagId { scope, name })
    }
}

impl TagId {
    pub fn new(scope: &'static str, name: &'static str) -> Self {
        Self { scope, name }
    }

    pub fn scope(&self) -> &'static str {
        self.scope
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn is_inbound(&self) -> bool {
        self.scope == INBOUND_TAG_SCOPE
    }

    pub fn is_outbound(&self) -> bool {
        self.scope == OUTBOUND_TAG_SCOPE
    }

    pub fn is_protocol(&self) -> bool {
        self.scope == PROTOCOL_TAG_SCOPE
    }

    pub fn is_pipe(&self) -> bool {
        self.scope == PIPE_TAG_SCOPE
    }
}

pub fn find_duplicate_tags<T>(tags: &[T]) -> Option<Vec<&TagId>>
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
