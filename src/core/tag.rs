use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagId(Arc<str>);

pub trait HasTag {
    fn tag(&self) -> TagId;
}

impl Serialize for TagId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TagId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(TagId(Arc::from(s)))
    }
}

impl std::fmt::Display for TagId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
