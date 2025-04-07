use std::{
    collections::HashMap,
    fmt::Display,
    time::{Duration, Instant},
};

use crate::core::types::resolve;

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

pub const STAGE_INBOUND_RECEIVED: &str = "inbound_received";
pub const STAGE_PIPE_RECEIVED: &str = "pipe_received";
pub const STAGE_PIPE_PROCESSED: &str = "pipe_processed";
pub const STAGE_OUTBOUND_RECEIVED: &str = "outbound_received";
pub const STAGE_OUTBOUND_PROCESSED: &str = "outbound_processed";

#[derive(Debug, Clone)]
pub struct Record {
    values: HashMap<Symbol, Value>,
    attributes: HashMap<Attribute, Value>,
    creation_time: Instant,
    stage_duration: HashMap<String, Duration>,
}

impl Record {
    pub fn empty() -> Self {
        Self {
            values: HashMap::new(),
            attributes: HashMap::new(),
            creation_time: Instant::now(),
            stage_duration: HashMap::new(),
        }
    }

    pub fn new(values: HashMap<Symbol, Value>) -> Self {
        Self {
            values,
            attributes: HashMap::new(),
            creation_time: Instant::now(),
            stage_duration: HashMap::new(),
        }
    }

    pub fn new_with_attributes(
        values: HashMap<Symbol, Value>,
        attributes: HashMap<Attribute, Value>,
    ) -> Self {
        Self {
            values,
            attributes,
            creation_time: Instant::now(),
            stage_duration: HashMap::new(),
        }
    }

    pub fn mark_timestamp(&mut self, stage_name: &str) {
        let elapsed = self.creation_time.elapsed();
        self.stage_duration.insert(stage_name.to_string(), elapsed);
    }

    pub fn get_timestamp(&self, stage_name: &str) -> Option<&Duration> {
        self.stage_duration.get(stage_name)
    }

    pub fn get_duration_between(&self, start_stage: &str, end_stage: &str) -> Option<Duration> {
        let start = self.stage_duration.get(start_stage)?;
        let end = self.stage_duration.get(end_stage)?;

        if end > start {
            Some(*end - *start)
        } else {
            None
        }
    }

    pub fn inherit_timestamps(&mut self, other: &Self) {
        self.creation_time = other.creation_time;
        self.stage_duration = other.stage_duration.clone();
    }

    pub fn set_stage_duration_map(&mut self, stage_duration: HashMap<String, Duration>) {
        self.stage_duration = stage_duration;
    }

    pub fn set_creation_time(&mut self, creation_time: Instant) {
        self.creation_time = creation_time;
    }

    pub fn get_stage_duration(&self) -> &HashMap<String, Duration> {
        &self.stage_duration
    }

    pub fn total_elapsed(&self) -> Duration {
        self.creation_time.elapsed()
    }

    pub fn creation_time(&self) -> &Instant {
        &self.creation_time
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

    pub fn take(self) -> HashMap<Symbol, Value> {
        self.values
    }
}

impl From<HashMap<Symbol, Value>> for Record {
    fn from(values: HashMap<Symbol, Value>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<(Symbol, Value)> for Record {
    fn from_iter<T: IntoIterator<Item = (Symbol, Value)>>(iter: T) -> Self {
        let mut values = HashMap::new();
        for (key, value) in iter {
            values.insert(key, value);
        }
        Self::new(values)
    }
}

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

        // 添加时间戳信息
        let timestamps = self
            .stage_duration
            .iter()
            .map(|(stage, duration)| format!("{} time: {}s", stage, duration.as_secs_f64()));

        let r#type = self
            .get_type()
            .map(|t| t.to_string())
            .unwrap_or("Record".to_string());

        let s = fields
            .chain(attrs)
            .chain(timestamps)
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

        let record = Record::from(values);

        assert_eq!(record.get(&key), Some(&value));
    }

    #[test]
    fn test_from_iterator() {
        let key = Symbol::from("test_key");
        let value = Value::String(Symbol::from("test_value"));
        let pairs = vec![(key.clone(), value.clone())];

        let record = Record::from_iter(pairs);

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
