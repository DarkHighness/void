use std::{collections::HashMap, fmt::Display, sync::Arc};

use crate::{
    config::global::use_time_tracing,
    core::{tag::TagId, types::resolve},
    utils::tracing::{Direction, TracingContext},
};

use super::{Symbol, Value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Attribute {
    Inbound,
    Type,
}

impl Display for Attribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Attribute::Inbound => write!(f, "__inbound__"),
            Attribute::Type => write!(f, "__type__"),
        }
    }
}

pub type AttributeMap = HashMap<Attribute, Value>;
pub type SymbolMap = HashMap<Symbol, Value>;

#[derive(Debug, Clone)]
pub struct Record {
    values: SymbolMap,
    attributes: AttributeMap,

    tracing_ctx: Arc<TracingContext>,
}

impl Record {
    pub fn empty() -> Self {
        Self {
            values: SymbolMap::new(),
            attributes: AttributeMap::new(),
            tracing_ctx: TracingContext::new_root(),
        }
    }

    pub fn new_root() -> Self {
        Self {
            values: SymbolMap::new(),
            attributes: AttributeMap::new(),
            tracing_ctx: TracingContext::new_root(),
        }
    }

    pub fn new(ctx: Arc<TracingContext>) -> Self {
        let ctx = TracingContext::inherit(ctx);
        Self {
            values: SymbolMap::new(),
            attributes: AttributeMap::new(),
            tracing_ctx: ctx,
        }
    }

    pub fn new_with_attrs(attrs: AttributeMap, ctx: Arc<TracingContext>) -> Self {
        let ctx = TracingContext::inherit(ctx);
        Self {
            values: SymbolMap::new(),
            attributes: attrs,
            tracing_ctx: ctx,
        }
    }

    pub fn new_with_values(values: SymbolMap, ctx: Arc<TracingContext>) -> Self {
        let ctx = TracingContext::inherit(ctx);
        Self {
            values,
            attributes: AttributeMap::new(),
            tracing_ctx: ctx,
        }
    }

    pub fn set(&mut self, key: Symbol, value: Value) {
        self.values.insert(key, value);
    }

    pub fn get(&self, key: &Symbol) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn get_mut(&mut self, key: &Symbol) -> Option<&mut Value> {
        self.values.get_mut(key)
    }

    pub fn set_attribute_overwrite(&mut self, key: Attribute, value: Value, overwrite: bool) {
        if overwrite {
            self.attributes.insert(key, value);
        } else {
            self.attributes.entry(key).or_insert(value);
        }
    }

    pub fn set_attribute(&mut self, key: Attribute, value: Value) {
        self.set_attribute_overwrite(key, value, false);
    }

    pub fn get_attribute(&self, key: &Attribute) -> Option<&Value> {
        self.attributes.get(key)
    }

    pub fn get_attribute_mut(&mut self, key: &Attribute) -> Option<&mut Value> {
        self.attributes.get_mut(key)
    }

    pub fn get_type(&self) -> Option<&Value> {
        self.get_attribute(&Attribute::Type)
    }

    pub fn take(self) -> SymbolMap {
        self.values
    }

    pub fn attributes(&self) -> &AttributeMap {
        &self.attributes
    }

    pub fn tracing_context(&self) -> &Arc<TracingContext> {
        &self.tracing_ctx
    }

    pub fn mark_timestamp(&self, tag: &TagId, direction: Direction) {
        if use_time_tracing() {
            self.tracing_ctx.add_timepoint(tag, direction);
        }
    }

    pub fn mark_record_release(&self) {
        if use_time_tracing() {
            self.tracing_ctx.record();
        }
    }
}

// impl From<HashMap<Symbol, Value>> for Record {
//     fn from(values: HashMap<Symbol, Value>) -> Self {
//         Self::new_with_values(values)
//     }
// }

// impl FromIterator<(Symbol, Value)> for Record {
//     fn from_iter<T: IntoIterator<Item = (Symbol, Value)>>(iter: T) -> Self {
//         let mut values = HashMap::new();
//         for (key, value) in iter {
//             values.insert(key, value);
//         }
//         Self::new_with_values(values)
//     }
// }

impl std::ops::Index<&Symbol> for Record {
    type Output = Value;

    fn index(&self, index: &Symbol) -> &Self::Output {
        self.values.get(index).unwrap()
    }
}

impl std::ops::Deref for Record {
    type Target = HashMap<Symbol, Value>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.values.keys().cloned().collect::<Vec<_>>();
        keys.sort_by(|a, b| resolve(a).cmp(&resolve(b)));

        let fields = keys.into_iter().map(|key| {
            let value = self.values.get(&key).unwrap();
            format!("\"{}\": {}", resolve(&key), value)
        });

        let attrs = self.attributes.keys().cloned().collect::<Vec<_>>();
        let attrs = attrs.into_iter().map(|key| {
            let value = self.attributes.get(&key).unwrap();
            format!("\"{}\": {}", key, value)
        });

        let r#type = self
            .get_type()
            .map(|t| t.to_string())
            .unwrap_or("Record".to_string());

        let s = fields
            .chain(attrs)
            .fold(format!("{} {{", r#type), |acc, field| {
                format!("{}\n  {},", acc, field)
            });

        let s = format!("{}\n}}", s);
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Value;

    #[test]
    fn test_empty_record() {
        let record = Record::empty();
        assert_eq!(record.values.len(), 0);
        assert_eq!(record.attributes.len(), 0);
    }

    #[test]
    fn test_record_set_get() {
        let mut record = Record::empty();
        let key = Symbol::from("test_key");
        let value = Value::String(Symbol::from("test_value"));

        record.set(key.clone(), value.clone());

        assert_eq!(record.get(&key), Some(&value));
    }

    #[test]
    fn test_record_attributes() {
        let mut record = Record::empty();
        let value = Value::String(Symbol::from("test_type"));

        record.set_attribute(Attribute::Type, value.clone());

        assert_eq!(record.get_attribute(&Attribute::Type), Some(&value));
        assert_eq!(record.get_type(), Some(&value));
    }

    #[test]
    fn test_record_attribute_overwrite() {
        let mut record = Record::empty();
        let value1 = Value::String(Symbol::from("value1"));
        let value2 = Value::String(Symbol::from("value2"));

        record.set_attribute(Attribute::Type, value1.clone());
        record.set_attribute_overwrite(Attribute::Type, value2.clone(), false);
        assert_eq!(record.get_attribute(&Attribute::Type), Some(&value1));

        record.set_attribute_overwrite(Attribute::Type, value2.clone(), true);
        assert_eq!(record.get_attribute(&Attribute::Type), Some(&value2));
    }

    #[test]
    fn test_from_hashmap() {
        let mut values = HashMap::new();
        let key = Symbol::from("test_key");
        let value = Value::String(Symbol::from("test_value"));
        values.insert(key.clone(), value.clone());

        let record = Record::new_with_values(values, TracingContext::new_root());

        assert_eq!(record.get(&key), Some(&value));
    }

    #[test]
    fn test_index_operator() {
        let mut record = Record::empty();
        let key = Symbol::from("test_key");
        let value = Value::String(Symbol::from("test_value"));

        record.set(key.clone(), value.clone());

        assert_eq!(record[&key], value);
    }

    #[test]
    fn test_deref() {
        let mut record = Record::empty();
        let key = Symbol::from("test_key");
        let value = Value::String(Symbol::from("test_value"));

        record.set(key.clone(), value.clone());

        assert_eq!(record.len(), 1);
        assert!(record.contains_key(&key));
    }

    #[test]
    fn test_display() {
        let mut record = Record::empty();
        let key = Symbol::from("name");
        let value = Value::String(Symbol::from("test"));

        record.set(key, value);
        record.set_attribute(Attribute::Type, Value::String(Symbol::from("Person")));

        let displayed = record.to_string();
        assert!(displayed.contains("Person {"));
        assert!(displayed.contains("\"name\": test"));
        assert!(displayed.contains("\"__type__\": Person"));
    }

    #[test]
    fn test_attribute_display() {
        assert_eq!(Attribute::Type.to_string(), "__type__");
        assert_eq!(Attribute::Inbound.to_string(), "__inbound__");
    }
}
