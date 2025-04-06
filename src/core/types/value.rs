use std::{collections::HashMap, fmt::Display, str::FromStr};

use chrono::{Offset, TimeZone};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

use super::{DataType, Symbol};

pub const VALUE_TYPE_NULL: &str = "null";
pub const VALUE_TYPE_STRING: &str = "string";
pub const VALUE_TYPE_INT: &str = "int";
pub const VALUE_TYPE_FLOAT: &str = "float";
pub const VALUE_TYPE_BOOL: &str = "bool";
pub const VALUE_TYPE_DATETIME: &str = "datetime";
pub const VALUE_TYPE_MAP: &str = "map";
pub const VALUE_TYPE_ARRAY: &str = "array";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    String(super::string::Symbol),
    Int(Number<i64>),
    Float(Number<f64>),
    Bool(bool),
    DateTime(chrono::DateTime<chrono::Utc>),
    Map(HashMap<Value, Value>),
    Array(Vec<Value>),
}

pub trait Num {}
impl Num for i64 {}
impl Num for f64 {}

#[derive(Debug, Clone, PartialEq)]
pub struct Number<T> {
    pub value: T,
    pub unit: Option<String>,
}

impl<T> Number<T>
where
    T: Num,
{
    pub fn new(value: T) -> Self {
        Self { value, unit: None }
    }

    pub fn new_with_unit(value: T, unit: String) -> Self {
        Self {
            value,
            unit: Some(unit),
        }
    }
}

impl<T> ToString for Number<T>
where
    T: Num + Display,
{
    fn to_string(&self) -> String {
        if let Some(unit) = &self.unit {
            format!("{} {}", self.value, unit)
        } else {
            self.value.to_string()
        }
    }
}

impl Hash for Number<i64> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        if let Some(unit) = &self.unit {
            unit.hash(state);
        }
    }
}

impl Hash for Number<f64> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.to_bits().hash(state);
        if let Some(unit) = &self.unit {
            unit.hash(state);
        }
    }
}

impl Eq for Number<i64> {}
impl Eq for Number<f64> {}

impl PartialOrd for Number<i64> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl PartialOrd for Number<f64> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Ord for Number<i64> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl Ord for Number<f64> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value
            .partial_cmp(&other.value)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl From<Number<i64>> for Number<f64> {
    fn from(number: Number<i64>) -> Self {
        Number {
            value: number.value as f64,
            unit: number.unit,
        }
    }
}

impl From<Number<f64>> for Number<i64> {
    fn from(number: Number<f64>) -> Self {
        Number {
            value: number.value as i64,
            unit: number.unit,
        }
    }
}

impl AsRef<i64> for Number<i64> {
    fn as_ref(&self) -> &i64 {
        &self.value
    }
}

impl AsRef<f64> for Number<f64> {
    fn as_ref(&self) -> &f64 {
        &self.value
    }
}

/*
1. If the unit is not present, it will be serialized as a number.
2. If the unit is present, it will be serialized as a string with the unit appended.
*/
impl Serialize for Number<f64> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(unit) = &self.unit {
            serializer.serialize_str(&format!("{} {}", self.value, unit))
        } else {
            serializer.serialize_f64(self.value)
        }
    }
}

impl Serialize for Number<i64> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(unit) = &self.unit {
            serializer.serialize_str(&format!("{} {}", self.value, unit))
        } else {
            serializer.serialize_i64(self.value)
        }
    }
}

/*
1. If it is a string, it should be treated as a number with the unit appended.
2. If it is a number, it should be treated as a number without the unit.
*/

impl<'de> Deserialize<'de> for Number<i64> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let value = parse_number_value::<i64>(&value).map_err(serde::de::Error::custom)?;

        match value {
            Value::Int(number) => Ok(number),
            _ => Err(serde::de::Error::custom("expected a number")),
        }
    }
}

impl<'de> Deserialize<'de> for Number<f64> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let value = parse_number_value::<f64>(&value).map_err(serde::de::Error::custom)?;

        match value {
            Value::Float(number) => Ok(number),
            _ => Err(serde::de::Error::custom("expected a number")),
        }
    }
}

impl From<i64> for Value {
    fn from(number: i64) -> Self {
        Value::Int(Number::new(number))
    }
}

impl From<f64> for Value {
    fn from(number: f64) -> Self {
        Value::Float(Number::new(number))
    }
}

impl From<Number<i64>> for Value {
    fn from(number: Number<i64>) -> Self {
        Value::Int(number)
    }
}

impl From<Number<f64>> for Value {
    fn from(number: Number<f64>) -> Self {
        Value::Float(number)
    }
}

impl From<String> for Value {
    fn from(string: String) -> Self {
        Value::String(super::string::intern(string))
    }
}

impl From<bool> for Value {
    fn from(boolean: bool) -> Self {
        Value::Bool(boolean)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Value {
    fn from(datetime: chrono::DateTime<chrono::Utc>) -> Self {
        Value::DateTime(datetime)
    }
}

impl From<&str> for Value {
    fn from(string: &str) -> Self {
        Value::String(super::string::intern(string))
    }
}

impl From<super::string::Symbol> for Value {
    fn from(string: super::string::Symbol) -> Self {
        Value::String(string)
    }
}

impl From<HashMap<Value, Value>> for Value {
    fn from(map: HashMap<Value, Value>) -> Self {
        Value::Map(map)
    }
}

impl From<HashMap<super::string::Symbol, Value>> for Value {
    fn from(map: HashMap<super::string::Symbol, Value>) -> Self {
        let map = map
            .into_iter()
            .map(|(key, value)| (Value::String(key), value))
            .collect();
        Value::Map(map)
    }
}

impl From<Vec<(super::string::Symbol, Value)>> for Value {
    fn from(array: Vec<(super::string::Symbol, Value)>) -> Self {
        array.into_iter().collect()
    }
}

impl FromIterator<(super::string::Symbol, Value)> for Value {
    fn from_iter<T: IntoIterator<Item = (super::string::Symbol, Value)>>(iter: T) -> Self {
        let map = iter
            .into_iter()
            .map(|(key, value)| (Value::String(key), value))
            .collect();
        Value::Map(map)
    }
}

impl From<Vec<(Value, Value)>> for Value {
    fn from(array: Vec<(Value, Value)>) -> Self {
        let map = array.into_iter().collect();
        Value::Map(map)
    }
}

impl FromIterator<(Value, Value)> for Value {
    fn from_iter<T: IntoIterator<Item = (Value, Value)>>(iter: T) -> Self {
        let map = iter.into_iter().collect();
        Value::Map(map)
    }
}

impl From<Vec<Value>> for Value {
    fn from(array: Vec<Value>) -> Self {
        Value::Array(array)
    }
}

impl From<&Symbol> for Value {
    fn from(symbol: &Symbol) -> Self {
        Value::String(symbol.clone())
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn is_number(&self) -> bool {
        matches!(self, Value::Int(_)) || matches!(self, Value::Float(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    pub fn is_datetime(&self) -> bool {
        matches!(self, Value::DateTime(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => VALUE_TYPE_NULL,
            Value::String(_) => VALUE_TYPE_STRING,
            Value::Int(_) => VALUE_TYPE_INT,
            Value::Float(_) => VALUE_TYPE_FLOAT,
            Value::Bool(_) => VALUE_TYPE_BOOL,
            Value::DateTime(_) => VALUE_TYPE_DATETIME,
            Value::Map(_) => VALUE_TYPE_MAP,
            Value::Array(_) => VALUE_TYPE_ARRAY,
        }
    }

    pub fn cast_string(&self) -> super::Result<Self> {
        match self {
            Value::String(_) => Ok(self.clone()),
            Value::Int(number) => Ok(Value::String(number.to_string().into())),
            Value::Float(number) => Ok(Value::String(number.to_string().into())),
            Value::Bool(boolean) => Ok(Value::String(boolean.to_string().into())),
            Value::DateTime(datetime) => Ok(Value::String(datetime.to_rfc3339().into())),
            _ => Err(super::Error::CanNotCast(
                self.type_name(),
                VALUE_TYPE_STRING,
            )),
        }
    }

    pub fn cast_float(&self) -> super::Result<Self> {
        match self {
            Value::Float(_) => Ok(self.clone()),
            Value::Int(number) => Ok(Value::Float(number.clone().into())),
            Value::Bool(boolean) => Ok(Value::Float(Number {
                value: if *boolean { 1.0 } else { 0.0 },
                unit: None,
            })),
            _ => Err(super::Error::CanNotCast(self.type_name(), VALUE_TYPE_FLOAT)),
        }
    }

    pub fn ensure_string(&self) -> super::Result<&super::string::Symbol> {
        if let Value::String(string) = self {
            Ok(string)
        } else {
            Err(super::Error::UnexpectedType("string", self.type_name()))
        }
    }

    pub fn ensure_int(&self) -> super::Result<&Number<i64>> {
        if let Value::Int(number) = self {
            Ok(number)
        } else {
            Err(super::Error::UnexpectedType("int", self.type_name()))
        }
    }

    pub fn ensure_float(&self) -> super::Result<&Number<f64>> {
        if let Value::Float(number) = self {
            Ok(number)
        } else {
            Err(super::Error::UnexpectedType("float", self.type_name()))
        }
    }

    pub fn ensure_bool(&self) -> super::Result<bool> {
        if let Value::Bool(boolean) = self {
            Ok(*boolean)
        } else {
            Err(super::Error::UnexpectedType("bool", self.type_name()))
        }
    }

    pub fn ensure_datetime(&self) -> super::Result<&chrono::DateTime<chrono::Utc>> {
        if let Value::DateTime(datetime) = self {
            Ok(datetime)
        } else {
            Err(super::Error::UnexpectedType("datetime", self.type_name()))
        }
    }

    pub fn ensure_map(&self) -> super::Result<&HashMap<Value, Value>> {
        if let Value::Map(map) = self {
            Ok(map)
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn ensure_map_mut(&mut self) -> super::Result<&mut HashMap<Value, Value>> {
        if let Value::Map(map) = self {
            Ok(map)
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn ensure_array(&self) -> super::Result<&Vec<Value>> {
        if let Value::Array(array) = self {
            Ok(array)
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn ensure_array_mut(&mut self) -> super::Result<&mut Vec<Value>> {
        if let Value::Array(array) = self {
            Ok(array)
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn map_set(&mut self, key: Value, value: Value) -> super::Result<()> {
        if let Value::Map(map) = self {
            map.insert(key, value);
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_get(&self, key: &Value) -> super::Result<Option<&Value>> {
        if let Value::Map(map) = self {
            Ok(map.get(key))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_contains_key(&self, key: &Value) -> super::Result<bool> {
        if let Value::Map(map) = self {
            Ok(map.contains_key(key))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_remove(&mut self, key: &Value) -> super::Result<Option<Value>> {
        if let Value::Map(map) = self {
            Ok(map.remove(key))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_keys(&self) -> super::Result<Vec<&Value>> {
        if let Value::Map(map) = self {
            Ok(map.keys().collect())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_values(&self) -> super::Result<Vec<&Value>> {
        if let Value::Map(map) = self {
            Ok(map.values().collect())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_len(&self) -> super::Result<usize> {
        if let Value::Map(map) = self {
            Ok(map.len())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_clear(&mut self) -> super::Result<()> {
        if let Value::Map(map) = self {
            map.clear();
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn array_push(&mut self, value: Value) -> super::Result<()> {
        if let Value::Array(array) = self {
            array.push(value);
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_get(&self, index: usize) -> super::Result<Option<&Value>> {
        if let Value::Array(array) = self {
            Ok(array.get(index))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_len(&self) -> super::Result<usize> {
        if let Value::Array(array) = self {
            Ok(array.len())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_remove(&mut self, index: usize) -> super::Result<Option<Value>> {
        if let Value::Array(array) = self {
            if index < array.len() {
                Ok(Some(array.remove(index)))
            } else {
                Ok(None)
            }
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_clear(&mut self) -> super::Result<()> {
        if let Value::Array(array) = self {
            array.clear();
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_iter(&self) -> super::Result<std::slice::Iter<Value>> {
        if let Value::Array(array) = self {
            Ok(array.iter())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn map_get_or_default<'a>(
        &'a self,
        key: &Value,
        default: &'a Value,
    ) -> super::Result<&'a Value> {
        if let Value::Map(map) = self {
            Ok(map.get(key).unwrap_or(default))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_get_mut(&mut self, key: &Value) -> super::Result<Option<&mut Value>> {
        if let Value::Map(map) = self {
            Ok(map.get_mut(key))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_try_get(&self, key: &Value) -> super::Result<&Value> {
        if let Value::Map(map) = self {
            map.get(key)
                .ok_or_else(|| super::Error::MapKeyNotFound(key.to_string()))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_try_get_mut(&mut self, key: &Value) -> super::Result<&mut Value> {
        if let Value::Map(map) = self {
            map.get_mut(key)
                .ok_or_else(|| super::Error::MapKeyNotFound(key.to_string()))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_get_or_insert(&mut self, key: Value, value: Value) -> super::Result<&Value> {
        if let Value::Map(map) = self {
            Ok(map.entry(key).or_insert(value))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_get_or_insert_with<F>(&mut self, key: Value, default: F) -> super::Result<&Value>
    where
        F: FnOnce() -> Value,
    {
        if let Value::Map(map) = self {
            Ok(map.entry(key).or_insert_with(default))
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_iter(&self) -> super::Result<impl Iterator<Item = (&Value, &Value)>> {
        if let Value::Map(map) = self {
            Ok(map.iter())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_iter_mut(&mut self) -> super::Result<impl Iterator<Item = (&Value, &mut Value)>> {
        if let Value::Map(map) = self {
            Ok(map.iter_mut())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_is_empty(&self) -> super::Result<bool> {
        if let Value::Map(map) = self {
            Ok(map.is_empty())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn map_retain<F>(&mut self, f: F) -> super::Result<()>
    where
        F: FnMut(&Value, &mut Value) -> bool,
    {
        if let Value::Map(map) = self {
            map.retain(f);
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("map", self.type_name()))
        }
    }

    pub fn array_insert(&mut self, index: usize, value: Value) -> super::Result<()> {
        if let Value::Array(array) = self {
            if index <= array.len() {
                array.insert(index, value);
                Ok(())
            } else {
                Err(super::Error::IndexOutOfBounds(index, array.len()))
            }
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_set(&mut self, index: usize, value: Value) -> super::Result<()> {
        if let Value::Array(array) = self {
            if index < array.len() {
                array[index] = value;
                Ok(())
            } else {
                Err(super::Error::IndexOutOfBounds(index, array.len()))
            }
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_filter<F>(&self, predicate: F) -> super::Result<Value>
    where
        F: Fn(&Value) -> bool,
    {
        if let Value::Array(array) = self {
            let filtered: Vec<Value> = array.iter().filter(|v| predicate(v)).cloned().collect();
            Ok(Value::Array(filtered))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_map<F>(&self, f: F) -> super::Result<Value>
    where
        F: Fn(&Value) -> Value,
    {
        if let Value::Array(array) = self {
            let mapped: Vec<Value> = array.iter().map(|v| f(v)).collect();
            Ok(Value::Array(mapped))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_sort(&mut self) -> super::Result<()> {
        if let Value::Array(array) = self {
            array.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_sort_by<F>(&mut self, compare: F) -> super::Result<()>
    where
        F: FnMut(&Value, &Value) -> std::cmp::Ordering,
    {
        if let Value::Array(array) = self {
            array.sort_by(compare);
            Ok(())
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_contains(&self, value: &Value) -> super::Result<bool> {
        if let Value::Array(array) = self {
            Ok(array.contains(value))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_index_of(&self, value: &Value) -> super::Result<Option<usize>> {
        if let Value::Array(array) = self {
            Ok(array.iter().position(|v| v == value))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_join(&self, separator: &str) -> super::Result<String> {
        if let Value::Array(array) = self {
            let result = array
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(separator);
            Ok(result)
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }

    pub fn array_slice(&self, start: usize, end: Option<usize>) -> super::Result<Value> {
        if let Value::Array(array) = self {
            let end = end.unwrap_or(array.len());
            if start > end || end > array.len() {
                return Err(super::Error::InvalidSliceRange(start, end, array.len()));
            }

            let sliced: Vec<Value> = array[start..end].to_vec();
            Ok(Value::Array(sliced))
        } else {
            Err(super::Error::UnexpectedType("array", self.type_name()))
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => VALUE_TYPE_NULL.hash(state),
            Value::String(string) => {
                VALUE_TYPE_STRING.hash(state);
                string.hash(state);
            }
            Value::Int(number) => {
                VALUE_TYPE_INT.hash(state);
                number.value.hash(state);
                if let Some(unit) = &number.unit {
                    unit.hash(state);
                }
            }
            Value::Float(number) => {
                VALUE_TYPE_FLOAT.hash(state);
                number.value.to_bits().hash(state);
                if let Some(unit) = &number.unit {
                    unit.hash(state);
                }
            }
            Value::Bool(boolean) => {
                VALUE_TYPE_BOOL.hash(state);
                boolean.hash(state);
            }
            Value::DateTime(datetime) => {
                VALUE_TYPE_DATETIME.hash(state);
                datetime.timestamp().hash(state);
                datetime.timestamp_subsec_nanos().hash(state);
            }
            Value::Map(map) => {
                VALUE_TYPE_MAP.hash(state);
                for (key, value) in map {
                    key.hash(state);
                    value.hash(state);
                }
            }
            Value::Array(array) => {
                VALUE_TYPE_ARRAY.hash(state);
                for value in array {
                    value.hash(state);
                }
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::String(string) => write!(f, "{}", string),
            Value::Int(number) => write!(f, "{}", number.to_string()),
            Value::Float(number) => write!(f, "{}", number.to_string()),
            Value::Bool(boolean) => write!(f, "{}", boolean),
            Value::DateTime(datetime) => write!(f, "{}", datetime),
            Value::Map(map) => {
                let map_str = map
                    .iter()
                    .map(|(key, value)| format!("{}: {}", key, value))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{}}}", map_str)
            }
            Value::Array(array) => {
                let array_str = array
                    .iter()
                    .map(|value| format!("{}", value))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", array_str)
            }
        }
    }
}

fn parse_number_value<T>(value: &str) -> super::Result<Value>
where
    T: Num + FromStr + Into<Value>,
    Value: From<Number<T>>,
{
    let parts: Vec<&str> = value.split_whitespace().collect();
    match parts.len() {
        1 => {
            let number = parts[0].parse::<T>().map_err(|_| {
                super::Error::InvalidNumberFormat(format!("Invalid number format: {}", value))
            })?;

            Ok(number.into())
        }
        2 => {
            let number = parts[0].parse::<T>().map_err(|_| {
                super::Error::InvalidNumberFormat(format!("Invalid number format: {}", value))
            })?;

            let unit = parts[1].to_string();
            let number = Number::new_with_unit(number, unit);
            Ok(number.into())
        }
        _ => Err(super::Error::InvalidNumberFormat(value.to_string())),
    }
}

fn parse_bool_value(value: &str) -> super::Result<Value> {
    const TRUE_VALUES: [&str; 7] = ["true", "t", "yes", "y", "on", "active", "1"];
    const FALSE_VALUES: [&str; 8] = [
        "false",
        "f",
        "no",
        "n",
        "off",
        "inactive",
        "not active",
        "0",
    ];

    let value = value.to_lowercase();
    if TRUE_VALUES.contains(&value.as_str()) {
        return Ok(true.into());
    } else if FALSE_VALUES.contains(&value.as_str()) {
        return Ok(false.into());
    }

    Err(super::Error::InvalidBoolFormat(value))
}

fn parse_datetime_value(value: &str) -> super::Result<Value> {
    let value_len = value.len();
    // Check if the value is a timestamp in seconds, milliseconds, or nanoseconds
    if let Ok(timestamp) = value.parse::<i64>() {
        match value_len {
            10 => match chrono::Utc.timestamp_opt(timestamp, 0).single() {
                Some(datetime) => return Ok(datetime.into()),
                None => return Err(super::Error::NonUniqueTimestampZoneMapping(timestamp)),
            },
            13 => match chrono::Utc.timestamp_millis_opt(timestamp).single() {
                Some(datetime) => return Ok(datetime.into()),
                None => return Err(super::Error::NonUniqueTimestampZoneMapping(timestamp)),
            },
            19 => return Ok(chrono::Utc.timestamp_nanos(timestamp).into()),
            _ => {}
        }
    }

    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.with_timezone(&chrono::Utc).into());
    }

    if let Ok(datetime) = chrono::DateTime::parse_from_rfc2822(value) {
        return Ok(datetime.with_timezone(&chrono::Utc).into());
    }

    // cargo fmt:skip
    const FORMATS: [&str; 12] = [
        "%Y/%m/%d %H:%M:%S%.f",    // 2025/04/03 16:09:03.452
        "%Y/%m/%d %H:%M:%S",       // 2025/04/03 16:09:03
        "%Y-%m-%d %H:%M:%S%.f",    // 2025-04-03 16:09:03.452
        "%Y-%m-%d %H:%M:%S",       // 2025-04-03 16:09:03
        "%Y-%m-%dT%H:%M:%S%.f%:z", // 2025-04-03T16:09:03.452+08:00
        "%Y-%m-%dT%H:%M:%S%.f",    // 2025-04-03T16:09:03.452
        "%Y-%m-%dT%H:%M:%S%:z",    // 2025-04-03T16:09:03+08:00
        "%Y-%m-%dT%H:%M:%S",       // 2025-04-03T16:09:03
        "%d/%m/%Y %H:%M:%S%.f",    // 03/04/2025 16:09:03.452
        "%m/%d/%Y %H:%M:%S%.f",    // 04/03/2025 16:09:03.452
        "%d-%m-%Y %H:%M:%S%.f",    // 03-04-2025 16:09:03.452
        "%m-%d-%Y %H:%M:%S%.f",    // 04-03-2025 16:09:03.452
    ];

    const FORMATS_WITH_TZ: [&str; 4] = [
        "%Y/%m/%d %H:%M:%S%.f%:z", // 2025/04/03 16:09:03.452+08:00
        "%Y-%m-%d %H:%M:%S%.f%:z", // 2025-04-03 16:09:03.452+08:00
        "%d/%m/%Y %H:%M:%S%.f%:z", // 03/04/2025 16:09:03.452+08:00
        "%m/%d/%Y %H:%M:%S%.f%:z", // 04/03/2025 16:09:03.452+08:00
    ];

    for format in FORMATS.iter() {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(value, format) {
            let utc = datetime.and_utc() - chrono::Local::now().offset().fix();
            return Ok(utc.into());
        }
    }

    for format in FORMATS_WITH_TZ.iter() {
        if let Ok(datetime) = chrono::DateTime::parse_from_str(value, format) {
            return Ok(datetime.with_timezone(&chrono::Utc).into());
        }
    }

    Err(super::Error::UnknownDatetimeFormat(value.to_string()))
}

pub fn parse_value(value: &str, data_type: &DataType) -> super::Result<Value> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Value::Null);
    }

    let value = match data_type {
        DataType::String => value.into(),
        DataType::Int => parse_number_value::<i64>(value)?,
        DataType::Float => parse_number_value::<f64>(value)?,
        DataType::Bool => parse_bool_value(value)?,
        DataType::DateTime => parse_datetime_value(value)?,
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn null() -> Value {
        Value::Null
    }

    fn string(s: &str) -> Value {
        s.to_string().into()
    }

    fn int(n: i64) -> Value {
        n.into()
    }

    fn int_with_unit(n: i64, unit: &str) -> Value {
        Value::Int(Number::new_with_unit(n, unit.to_string()))
    }

    fn float(n: f64) -> Value {
        n.into()
    }

    fn float_with_unit(n: f64, unit: &str) -> Value {
        Value::Float(Number::new_with_unit(n, unit.to_string()))
    }

    fn bool_val(b: bool) -> Value {
        b.into()
    }

    fn datetime(timestamp: i64) -> Value {
        let dt = chrono::Utc.timestamp_opt(timestamp, 0).unwrap();
        dt.into()
    }

    fn map() -> Value {
        let mut map = HashMap::new();
        map.insert(string("key1"), string("value1"));
        map.insert(string("key2"), int(42));
        map.insert(string("key3"), float(3.14));
        map.into()
    }

    fn array() -> Value {
        vec![string("item1"), int(42), float(3.14)].into()
    }

    #[test]
    fn test_value_creation() {
        assert_eq!(null().is_null(), true);
        assert_eq!(string("test").is_string(), true);
        assert_eq!(int(42).is_int(), true);
        assert_eq!(float(3.14).is_float(), true);
        assert_eq!(bool_val(true).is_bool(), true);
        assert_eq!(datetime(1609459200).is_datetime(), true);
        assert_eq!(map().is_map(), true);
        assert_eq!(array().is_array(), true);

        let int_unit = int_with_unit(100, "kg");
        assert_eq!(int_unit.is_int(), true);
        if let Value::Int(num) = int_unit {
            assert_eq!(num.value, 100);
            assert_eq!(num.unit, Some("kg".to_string()));
        }

        let float_unit = float_with_unit(72.5, "cm");
        assert_eq!(float_unit.is_float(), true);
        if let Value::Float(num) = float_unit {
            assert_eq!(num.value, 72.5);
            assert_eq!(num.unit, Some("cm".to_string()));
        }
    }

    #[test]
    fn test_value_type_name() {
        assert_eq!(null().type_name(), VALUE_TYPE_NULL);
        assert_eq!(string("test").type_name(), VALUE_TYPE_STRING);
        assert_eq!(int(42).type_name(), VALUE_TYPE_INT);
        assert_eq!(float(3.14).type_name(), VALUE_TYPE_FLOAT);
        assert_eq!(bool_val(true).type_name(), VALUE_TYPE_BOOL);
        assert_eq!(datetime(1609459200).type_name(), VALUE_TYPE_DATETIME);
        assert_eq!(map().type_name(), VALUE_TYPE_MAP);
        assert_eq!(array().type_name(), VALUE_TYPE_ARRAY);
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", null()), "null");
        assert_eq!(format!("{}", string("test")), "test");
        assert_eq!(format!("{}", int(42)), "42");
        assert_eq!(format!("{}", float(3.14)), "3.14");
        assert_eq!(format!("{}", bool_val(true)), "true");
        assert_eq!(format!("{}", int_with_unit(100, "kg")), "100 kg");
        assert_eq!(format!("{}", float_with_unit(72.5, "cm")), "72.5 cm");

        let map_str = format!("{}", map());
        assert!(map_str.starts_with("{"));
        assert!(map_str.ends_with("}"));
        assert!(map_str.contains("key1: value1"));
        assert!(map_str.contains("key2: 42"));
        assert!(map_str.contains("key3: 3.14"));

        let array_str = format!("{}", array());
        assert!(array_str.starts_with("["));
        assert!(array_str.ends_with("]"));
        assert!(array_str.contains("item1"));
        assert!(array_str.contains("42"));
        assert!(array_str.contains("3.14"));
    }

    #[test]
    fn test_ensure_methods() {
        assert_eq!(string("test").ensure_string().unwrap().to_string(), "test");
        assert_eq!(int(42).ensure_int().unwrap().value, 42);
        assert_eq!(float(3.14).ensure_float().unwrap().value, 3.14);
        assert_eq!(bool_val(true).ensure_bool().unwrap(), true);

        assert!(string("test").ensure_int().is_err());
        assert!(int(42).ensure_bool().is_err());
        assert!(float(3.14).ensure_string().is_err());
        assert!(bool_val(true).ensure_float().is_err());
    }

    #[test]
    fn test_cast_methods() {
        assert_eq!(int(42).cast_string().unwrap(), string("42"));
        assert_eq!(float(3.14).cast_string().unwrap(), string("3.14"));
        assert_eq!(bool_val(true).cast_string().unwrap(), string("true"));

        assert_eq!(
            int(42).cast_float().unwrap().ensure_float().unwrap().value,
            42.0
        );
        assert_eq!(
            bool_val(true)
                .cast_float()
                .unwrap()
                .ensure_float()
                .unwrap()
                .value,
            1.0
        );
    }

    #[test]
    fn test_map_operations() {
        let mut m = map();

        assert_eq!(m.map_len().unwrap(), 3);
        assert_eq!(
            m.map_get(&string("key1")).unwrap().unwrap(),
            &string("value1")
        );
        assert!(m.map_contains_key(&string("key2")).unwrap());

        m.map_set(string("key4"), bool_val(false)).unwrap();
        assert_eq!(m.map_len().unwrap(), 4);
        assert_eq!(
            m.map_get(&string("key4")).unwrap().unwrap(),
            &bool_val(false)
        );

        let removed = m.map_remove(&string("key1")).unwrap().unwrap();
        assert_eq!(removed, string("value1"));
        assert_eq!(m.map_len().unwrap(), 3);
        assert!(!m.map_contains_key(&string("key1")).unwrap());

        m.map_clear().unwrap();
        assert_eq!(m.map_len().unwrap(), 0);
    }

    #[test]
    fn test_advanced_map_operations() {
        let mut m = map();

        assert_eq!(
            m.map_get_or_default(&string("key1"), &string("default"))
                .unwrap(),
            &string("value1")
        );
        assert_eq!(
            m.map_get_or_default(&string("nonexistent"), &string("default"))
                .unwrap(),
            &string("default")
        );

        if let Some(val) = m.map_get_mut(&string("key1")).unwrap() {
            *val = int(100);
        }
        assert_eq!(m.map_get(&string("key1")).unwrap().unwrap(), &int(100));

        assert_eq!(m.map_try_get(&string("key2")).unwrap(), &int(42));
        assert!(m.map_try_get(&string("nonexistent")).is_err());

        let entry_val = m
            .map_get_or_insert(string("new_key"), bool_val(true))
            .unwrap();
        assert_eq!(entry_val, &bool_val(true));
        assert_eq!(
            m.map_get(&string("new_key")).unwrap().unwrap(),
            &bool_val(true)
        );

        let lazy_val = m
            .map_get_or_insert_with(string("lazy_key"), || float(99.9))
            .unwrap();
        assert_eq!(lazy_val, &float(99.9));

        let mut iter_count = 0;
        for (k, v) in m.map_iter().unwrap() {
            assert!(k.is_string());
            iter_count += 1;
        }
        assert_eq!(iter_count, 5);

        for (_, v) in m.map_iter_mut().unwrap() {
            if let Value::Int(n) = v {
                n.value *= 2;
            }
        }
        assert_eq!(
            m.map_try_get(&string("key2")).unwrap(),
            &int(84) // 42 * 2
        );

        assert_eq!(m.map_is_empty().unwrap(), false);
        m.map_clear().unwrap();
        assert_eq!(m.map_is_empty().unwrap(), true);

        let mut m = map();
        m.map_retain(|k, _| {
            if let Value::String(s) = k {
                s.to_string() != "key1"
            } else {
                true
            }
        })
        .unwrap();
        assert_eq!(m.map_len().unwrap(), 2);
        assert!(m.map_get(&string("key1")).unwrap().is_none());
        assert!(m.map_get(&string("key2")).unwrap().is_some());
    }

    #[test]
    fn test_array_operations() {
        let mut arr = array();

        assert_eq!(arr.array_len().unwrap(), 3);
        assert_eq!(arr.array_get(0).unwrap().unwrap(), &string("item1"));

        arr.array_push(bool_val(false)).unwrap();
        assert_eq!(arr.array_len().unwrap(), 4);
        assert_eq!(arr.array_get(3).unwrap().unwrap(), &bool_val(false));

        let removed = arr.array_remove(0).unwrap().unwrap();
        assert_eq!(removed, string("item1"));
        assert_eq!(arr.array_len().unwrap(), 3);
        assert_eq!(arr.array_get(0).unwrap().unwrap(), &int(42));

        let items: Vec<&Value> = arr.array_iter().unwrap().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], &int(42));
        assert_eq!(items[1], &float(3.14));
        assert_eq!(items[2], &bool_val(false));

        arr.array_clear().unwrap();
        assert_eq!(arr.array_len().unwrap(), 0);
    }

    #[test]
    fn test_number_operations() {
        let int_num = Number::new(42i64);
        assert_eq!(int_num.to_string(), "42");

        let int_unit = Number::new_with_unit(100i64, "kg".to_string());
        assert_eq!(int_unit.to_string(), "100 kg");

        let float_num = Number::new(3.14f64);
        assert_eq!(float_num.to_string(), "3.14");

        let float_unit = Number::new_with_unit(72.5f64, "cm".to_string());
        assert_eq!(float_unit.to_string(), "72.5 cm");

        let float_from_int: Number<f64> = int_num.into();
        assert_eq!(float_from_int.value, 42.0);
        assert_eq!(float_from_int.unit, None);

        let int_from_float: Number<i64> = float_num.into();
        assert_eq!(int_from_float.value, 3);
        assert_eq!(int_from_float.unit, None);
    }

    #[test]
    fn test_parse_number_value() {
        let int_val = parse_number_value::<i64>("42").unwrap();
        assert_eq!(int_val, int(42));

        let float_val = parse_number_value::<f64>("3.14").unwrap();
        assert_eq!(float_val, float(3.14));

        let int_unit_val = parse_number_value::<i64>("100 kg").unwrap();
        if let Value::Int(num) = int_unit_val {
            assert_eq!(num.value, 100);
            assert_eq!(num.unit, Some("kg".to_string()));
        } else {
            panic!("Expected Int value");
        }

        let float_unit_val = parse_number_value::<f64>("72.5 cm").unwrap();
        if let Value::Float(num) = float_unit_val {
            assert_eq!(num.value, 72.5);
            assert_eq!(num.unit, Some("cm".to_string()));
        } else {
            panic!("Expected Float value");
        }

        assert!(parse_number_value::<i64>("not a number").is_err());
        assert!(parse_number_value::<f64>("3.14.15").is_err());
        assert!(parse_number_value::<i64>("42 kg extra").is_err());
    }

    #[test]
    fn test_parse_bool_value() {
        assert_eq!(parse_bool_value("true").unwrap(), bool_val(true));
        assert_eq!(parse_bool_value("yes").unwrap(), bool_val(true));
        assert_eq!(parse_bool_value("Y").unwrap(), bool_val(true));
        assert_eq!(parse_bool_value("1").unwrap(), bool_val(true));
        assert_eq!(parse_bool_value("ACTIVE").unwrap(), bool_val(true));

        assert_eq!(parse_bool_value("false").unwrap(), bool_val(false));
        assert_eq!(parse_bool_value("no").unwrap(), bool_val(false));
        assert_eq!(parse_bool_value("N").unwrap(), bool_val(false));
        assert_eq!(parse_bool_value("0").unwrap(), bool_val(false));
        assert_eq!(parse_bool_value("inactive").unwrap(), bool_val(false));

        assert!(parse_bool_value("maybe").is_err());
        assert!(parse_bool_value("2").is_err());
    }

    #[test]
    fn test_parse_datetime_value() {
        let dt1 = parse_datetime_value("1609459200").unwrap(); // 2021-01-01 00:00:00 UTC
        if let Value::DateTime(dt) = dt1 {
            assert_eq!(dt.timestamp(), 1609459200);
        } else {
            panic!("Expected DateTime value");
        }

        let dt2 = parse_datetime_value("2021-01-01T00:00:00Z").unwrap();
        if let Value::DateTime(dt) = dt2 {
            assert_eq!(dt.timestamp(), 1609459200);
        } else {
            panic!("Expected DateTime value");
        }

        let dt3 = parse_datetime_value("2021-01-01 00:00:00").unwrap();
        assert!(dt3.is_datetime());

        let dt4 = parse_datetime_value("2021/01/01 00:00:00").unwrap();
        assert!(dt4.is_datetime());

        assert!(parse_datetime_value("not a date").is_err());
        assert!(parse_datetime_value("2021-13-01").is_err());
    }

    #[test]
    fn test_parse_value() {
        assert_eq!(
            parse_value("test", &DataType::String).unwrap(),
            string("test")
        );
        assert_eq!(parse_value("42", &DataType::Int).unwrap(), int(42));
        assert_eq!(parse_value("3.14", &DataType::Float).unwrap(), float(3.14));
        assert_eq!(
            parse_value("true", &DataType::Bool).unwrap(),
            bool_val(true)
        );

        let dt = parse_value("2021-01-01T00:00:00Z", &DataType::DateTime).unwrap();
        assert!(dt.is_datetime());

        assert_eq!(parse_value("", &DataType::String).unwrap(), Value::Null);
        assert_eq!(parse_value("   ", &DataType::Int).unwrap(), Value::Null);
    }

    #[test]
    fn test_advanced_array_operations() {
        let mut arr = array();

        // Test array_insert
        arr.array_insert(1, bool_val(true)).unwrap();
        assert_eq!(arr.array_len().unwrap(), 4);
        assert_eq!(arr.array_get(1).unwrap().unwrap(), &bool_val(true));

        // Test array_set
        arr.array_set(0, string("replaced")).unwrap();
        assert_eq!(arr.array_get(0).unwrap().unwrap(), &string("replaced"));

        // Test array_filter
        let filtered = arr.array_filter(|v| v.is_bool() || v.is_int()).unwrap();
        if let Value::Array(filtered_arr) = filtered {
            assert_eq!(filtered_arr.len(), 2);
            assert!(filtered_arr.contains(&bool_val(true)));
            assert!(filtered_arr.contains(&int(42)));
        }

        // Test array_map
        let mapped = arr
            .array_map(|v| {
                if let Value::Int(num) = v {
                    Value::Int(Number::new(num.value * 2))
                } else {
                    v.clone()
                }
            })
            .unwrap();

        if let Value::Array(mapped_arr) = mapped {
            assert_eq!(mapped_arr.len(), 4);
            for v in &mapped_arr {
                if v.is_int() {
                    if let Value::Int(num) = v {
                        assert_eq!(num.value, 84); // 42 * 2
                    }
                }
            }
        }

        // Test array_sort
        let mut unsorted = Value::Array(vec![int(3), int(1), int(2)]);
        unsorted.array_sort().unwrap();
        let sorted_arr = unsorted.ensure_array().unwrap();
        assert_eq!(sorted_arr[0], int(1));
        assert_eq!(sorted_arr[1], int(2));
        assert_eq!(sorted_arr[2], int(3));

        // Test array_sort_by (descending)
        let mut unsorted = Value::Array(vec![int(3), int(1), int(2)]);
        unsorted
            .array_sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap();
        let sorted_arr = unsorted.ensure_array().unwrap();
        assert_eq!(sorted_arr[0], int(3));
        assert_eq!(sorted_arr[1], int(2));
        assert_eq!(sorted_arr[2], int(1));

        // Test array_contains
        assert_eq!(arr.array_contains(&int(42)).unwrap(), true);
        assert_eq!(arr.array_contains(&string("nonexistent")).unwrap(), false);

        // Test array_index_of
        assert_eq!(arr.array_index_of(&int(42)).unwrap(), Some(2));
        assert_eq!(arr.array_index_of(&string("nonexistent")).unwrap(), None);

        // Test array_join
        let joined = arr.array_join(", ").unwrap();
        assert!(joined.contains("replaced"));
        assert!(joined.contains("true"));
        assert!(joined.contains("42"));
        assert!(joined.contains("3.14"));

        // Test array_slice
        let sliced = arr.array_slice(1, Some(3)).unwrap();
        if let Value::Array(sliced_arr) = sliced {
            assert_eq!(sliced_arr.len(), 2);
            assert_eq!(sliced_arr[0], bool_val(true));
            assert_eq!(sliced_arr[1], int(42));
        }
    }
}
