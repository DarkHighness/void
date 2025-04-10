use std::{collections::HashMap, fmt::Display, str::FromStr};

use chrono::{Offset, TimeZone};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

pub use super::data_type::{
    BOOL_TYPE, DATETIME_TYPE, FLOAT_TYPE, INT_TYPE, NULL_TYPE, STRING_TYPE,
};
use super::Primitive;

pub const MAP_TYPE: &'static str = "Map";
pub const ARRAY_TYPE: &'static str = "Array";

// Add Guard type definition
pub struct StringGuard<'a>(&'a super::string::Symbol);
pub struct IntGuard<'a>(&'a Number<i64>);
pub struct FloatGuard<'a>(&'a Number<f64>);
pub struct BoolGuard(bool);
pub struct DateTimeGuard<'a>(&'a chrono::DateTime<chrono::Utc>);
pub struct MapGuard<'a>(&'a HashMap<Value, Value>);
pub struct MapGuardMut<'a>(&'a mut HashMap<Value, Value>);
pub struct ArrayGuard<'a>(&'a Vec<Value>);
pub struct ArrayGuardMut<'a>(&'a mut Vec<Value>);

// Implement methods for Guard
impl<'a> StringGuard<'a> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_symbol(&self) -> &super::string::Symbol {
        self.0
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl<'a> IntGuard<'a> {
    pub fn value(&self) -> i64 {
        self.0.value
    }

    pub fn unit(&self) -> Option<&String> {
        self.0.unit.as_ref()
    }

    pub fn as_number(&self) -> &Number<i64> {
        self.0
    }
}

impl<'a> FloatGuard<'a> {
    pub fn value(&self) -> f64 {
        self.0.value
    }

    pub fn unit(&self) -> Option<&String> {
        self.0.unit.as_ref()
    }

    pub fn as_number(&self) -> &Number<f64> {
        self.0
    }
}

impl BoolGuard {
    pub fn value(&self) -> bool {
        self.0
    }
}

impl<'a> DateTimeGuard<'a> {
    pub fn as_datetime(&self) -> &chrono::DateTime<chrono::Utc> {
        self.0
    }

    pub fn timestamp_seconds(&self) -> i64 {
        self.0.timestamp()
    }

    pub fn timestamp_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }

    pub fn timestamp_nanos(&self) -> i64 {
        self.0.timestamp_nanos_opt().expect("Out of range datetime")
    }

    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

    pub fn to_rfc2822(&self) -> String {
        self.0.to_rfc2822()
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn to_local(&self) -> chrono::DateTime<chrono::Local> {
        self.0.with_timezone(&chrono::Local)
    }

    pub fn to_offset(&self, offset: &chrono::FixedOffset) -> chrono::DateTime<chrono::FixedOffset> {
        self.0.with_timezone(offset)
    }

    pub fn strftime(&self, format: &str) -> String {
        self.0.format(format).to_string()
    }
}

impl<'a> MapGuard<'a> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, key: &Value) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn contains_key(&self, key: &Value) -> bool {
        self.0.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &Value> {
        self.0.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.values()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Value, &Value)> {
        self.0.iter()
    }

    pub fn as_hashmap(&self) -> &HashMap<Value, Value> {
        self.0
    }
}

impl<'a> MapGuardMut<'a> {
    pub fn set(&mut self, key: Value, value: Value) {
        self.0.insert(key, value);
    }

    pub fn remove(&mut self, key: &Value) -> Option<Value> {
        self.0.remove(key)
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn get_mut(&mut self, key: &Value) -> Option<&mut Value> {
        self.0.get_mut(key)
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Value, &mut Value) -> bool,
    {
        self.0.retain(f);
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&Value, &mut Value)> {
        self.0.iter_mut()
    }

    pub fn as_hashmap_mut(&mut self) -> &mut HashMap<Value, Value> {
        self.0
    }
}

impl<'a> ArrayGuard<'a> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    pub fn contains(&self, value: &Value) -> bool {
        self.0.contains(value)
    }

    pub fn index_of(&self, value: &Value) -> Option<usize> {
        self.0.iter().position(|v| v == value)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.0.iter()
    }

    pub fn as_slice(&self) -> &[Value] {
        self.0.as_slice()
    }

    pub fn join(&self, separator: &str) -> String {
        self.0
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .join(separator)
    }
}

impl<'a> ArrayGuardMut<'a> {
    pub fn push(&mut self, value: Value) {
        self.0.push(value);
    }

    pub fn remove(&mut self, index: usize) -> Option<Value> {
        if index < self.0.len() {
            Some(self.0.remove(index))
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn insert(&mut self, index: usize, value: Value) -> Result<(), super::Error> {
        if index <= self.0.len() {
            self.0.insert(index, value);
            Ok(())
        } else {
            Err(super::Error::IndexOutOfBounds(index, self.0.len()))
        }
    }

    pub fn set(&mut self, index: usize, value: Value) -> Result<(), super::Error> {
        if index < self.0.len() {
            self.0[index] = value;
            Ok(())
        } else {
            Err(super::Error::IndexOutOfBounds(index, self.0.len()))
        }
    }

    pub fn sort(&mut self) {
        self.0
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    pub fn sort_by<F>(&mut self, compare: F)
    where
        F: FnMut(&Value, &Value) -> std::cmp::Ordering,
    {
        self.0.sort_by(compare);
    }

    pub fn as_mut_slice(&mut self) -> &mut [Value] {
        self.0.as_mut_slice()
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    Null,
    String,
    Int,
    Float,
    Bool,
    DateTime,
    Map,
    Array,
}

impl ValueType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValueType::Null => NULL_TYPE,
            ValueType::String => STRING_TYPE,
            ValueType::Int => INT_TYPE,
            ValueType::Float => FLOAT_TYPE,
            ValueType::Bool => BOOL_TYPE,
            ValueType::DateTime => DATETIME_TYPE,
            ValueType::Map => MAP_TYPE,
            ValueType::Array => ARRAY_TYPE,
        }
    }

    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            ValueType::Null
                | ValueType::String
                | ValueType::Int
                | ValueType::Float
                | ValueType::Bool
        )
    }

    pub fn is_complex(&self) -> bool {
        matches!(self, ValueType::Map | ValueType::Array)
    }
}

impl TryInto<Primitive> for ValueType {
    type Error = super::Error;

    fn try_into(self) -> Result<Primitive, Self::Error> {
        match self {
            ValueType::Null => Ok(Primitive::Null),
            ValueType::String => Ok(Primitive::String),
            ValueType::Int => Ok(Primitive::Int),
            ValueType::Float => Ok(Primitive::Float),
            ValueType::Bool => Ok(Primitive::Bool),
            ValueType::DateTime => Ok(Primitive::DateTime),
            ValueType::Map => Err(super::Error::InvalidValueType(MAP_TYPE.to_string())),
            ValueType::Array => Err(super::Error::InvalidValueType(ARRAY_TYPE.to_string())),
        }
    }
}

impl From<&Primitive> for ValueType {
    fn from(primitive: &Primitive) -> Self {
        match primitive {
            Primitive::Null => ValueType::Null,
            Primitive::String => ValueType::String,
            Primitive::Int => ValueType::Int,
            Primitive::Float => ValueType::Float,
            Primitive::Bool => ValueType::Bool,
            Primitive::DateTime => ValueType::DateTime,
        }
    }
}

impl From<Primitive> for ValueType {
    fn from(primitive: Primitive) -> Self {
        From::from(&primitive)
    }
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

impl From<&super::string::Symbol> for Value {
    fn from(string: &super::string::Symbol) -> Self {
        Value::String(string.clone())
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

    pub fn is_primitive(&self) -> bool {
        self.type_().is_primitive()
    }

    pub fn is_complex(&self) -> bool {
        self.type_().is_complex()
    }

    pub fn type_(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::String(_) => ValueType::String,
            Value::Int(_) => ValueType::Int,
            Value::Float(_) => ValueType::Float,
            Value::Bool(_) => ValueType::Bool,
            Value::DateTime(_) => ValueType::DateTime,
            Value::Map(_) => ValueType::Map,
            Value::Array(_) => ValueType::Array,
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => NULL_TYPE,
            Value::String(_) => STRING_TYPE,
            Value::Int(_) => INT_TYPE,
            Value::Float(_) => FLOAT_TYPE,
            Value::Bool(_) => BOOL_TYPE,
            Value::DateTime(_) => DATETIME_TYPE,
            Value::Map(_) => MAP_TYPE,
            Value::Array(_) => ARRAY_TYPE,
        }
    }

    pub fn cast_string(&self) -> super::Result<Self> {
        match self {
            Value::String(_) => Ok(self.clone()),
            Value::Int(number) => Ok(Value::String(number.to_string().into())),
            Value::Float(number) => Ok(Value::String(number.to_string().into())),
            Value::Bool(boolean) => Ok(Value::String(boolean.to_string().into())),
            Value::DateTime(datetime) => Ok(Value::String(datetime.to_rfc3339().into())),
            _ => Err(super::Error::CanNotCast(self.type_name(), STRING_TYPE)),
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
            _ => Err(super::Error::CanNotCast(self.type_name(), FLOAT_TYPE)),
        }
    }

    pub fn string(&self) -> super::Result<StringGuard> {
        if let Value::String(string) = self {
            Ok(StringGuard(string))
        } else {
            Err(super::Error::UnexpectedType(STRING_TYPE, self.type_name()))
        }
    }

    pub fn int(&self) -> super::Result<IntGuard> {
        if let Value::Int(number) = self {
            Ok(IntGuard(number))
        } else {
            Err(super::Error::UnexpectedType(INT_TYPE, self.type_name()))
        }
    }

    pub fn float(&self) -> super::Result<FloatGuard> {
        if let Value::Float(number) = self {
            Ok(FloatGuard(number))
        } else {
            Err(super::Error::UnexpectedType(FLOAT_TYPE, self.type_name()))
        }
    }

    pub fn bool(&self) -> super::Result<BoolGuard> {
        if let Value::Bool(boolean) = self {
            Ok(BoolGuard(*boolean))
        } else {
            Err(super::Error::UnexpectedType(BOOL_TYPE, self.type_name()))
        }
    }

    pub fn datetime(&self) -> super::Result<DateTimeGuard> {
        if let Value::DateTime(datetime) = self {
            Ok(DateTimeGuard(datetime))
        } else {
            Err(super::Error::UnexpectedType(
                DATETIME_TYPE,
                self.type_name(),
            ))
        }
    }

    pub fn map(&self) -> super::Result<MapGuard> {
        if let Value::Map(map) = self {
            Ok(MapGuard(map))
        } else {
            Err(super::Error::UnexpectedType(MAP_TYPE, self.type_name()))
        }
    }

    pub fn map_mut(&mut self) -> super::Result<MapGuardMut> {
        if let Value::Map(map) = self {
            Ok(MapGuardMut(map))
        } else {
            Err(super::Error::UnexpectedType(MAP_TYPE, self.type_name()))
        }
    }

    pub fn array(&self) -> super::Result<ArrayGuard> {
        if let Value::Array(array) = self {
            Ok(ArrayGuard(array))
        } else {
            Err(super::Error::UnexpectedType(ARRAY_TYPE, self.type_name()))
        }
    }

    pub fn array_mut(&mut self) -> super::Result<ArrayGuardMut> {
        if let Value::Array(array) = self {
            Ok(ArrayGuardMut(array))
        } else {
            Err(super::Error::UnexpectedType(ARRAY_TYPE, self.type_name()))
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => NULL_TYPE.hash(state),
            Value::String(string) => {
                STRING_TYPE.hash(state);
                string.hash(state);
            }
            Value::Int(number) => {
                INT_TYPE.hash(state);
                number.value.hash(state);
                if let Some(unit) = &number.unit {
                    unit.hash(state);
                }
            }
            Value::Float(number) => {
                FLOAT_TYPE.hash(state);
                number.value.to_bits().hash(state);
                if let Some(unit) = &number.unit {
                    unit.hash(state);
                }
            }
            Value::Bool(boolean) => {
                BOOL_TYPE.hash(state);
                boolean.hash(state);
            }
            Value::DateTime(datetime) => {
                DATETIME_TYPE.hash(state);
                datetime.timestamp().hash(state);
                datetime.timestamp_subsec_nanos().hash(state);
            }
            Value::Map(map) => {
                MAP_TYPE.hash(state);
                for (key, value) in map {
                    key.hash(state);
                    value.hash(state);
                }
            }
            Value::Array(array) => {
                ARRAY_TYPE.hash(state);
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
            let number = parts[0]
                .parse::<T>()
                .map_err(|_| super::Error::InvalidNumberFormat(value.to_string()))?;

            Ok(number.into())
        }
        2 => {
            let number = parts[0]
                .parse::<T>()
                .map_err(|_| super::Error::InvalidNumberFormat(value.to_string()))?;

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

fn parse_map_value(value: &str) -> super::Result<Value> {
    let mut map = HashMap::new();

    for field in value.split(',') {
        let parts: Vec<&str> = field.split('=').collect();
        if parts.len() != 2 {
            return Err(super::Error::InvalidMapFormat(value.to_string()));
        }
        let key = Value::from(parts[0].trim());
        let value = parse_primitive_value(parts[1].trim())?;
        map.insert(key, value);
    }

    Ok(Value::Map(map))
}

fn parse_array_value(value: &str) -> super::Result<Value> {
    let mut array = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if !item.is_empty() {
            let value = parse_primitive_value(item)?;
            array.push(value);
        }
    }
    Ok(Value::Array(array))
}

fn parse_primitive_value(value: &str) -> super::Result<Value> {
    if value.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    } else if value.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    } else if value.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    } else if let Ok(int_val) = value.parse::<i64>() {
        return Ok(Value::Int(Number::new(int_val)));
    } else if let Ok(float_val) = value.parse::<f64>() {
        return Ok(Value::Float(Number::new(float_val)));
    } else if value.starts_with("{") && value.ends_with("}") {
        let inner = &value[1..value.len() - 1];
        return parse_map_value(inner);
    } else if value.starts_with("[") && value.ends_with("]") {
        let inner = &value[1..value.len() - 1];
        return parse_array_value(inner);
    }

    Ok(Value::String(super::string::intern(value)))
}

pub fn parse_value(value: &str, typ: ValueType) -> super::Result<Value> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Value::Null);
    }

    match typ {
        ValueType::Null => return Ok(Value::Null),
        ValueType::String => return Ok(Value::String(super::string::intern(value))),
        ValueType::Int => return parse_number_value::<i64>(value),
        ValueType::Float => return parse_number_value::<f64>(value),
        ValueType::Bool => return parse_bool_value(value),
        ValueType::DateTime => return parse_datetime_value(value),
        ValueType::Map => {
            if value.starts_with('{') && value.ends_with('}') {
                let inner = &value[1..value.len() - 1];
                return parse_map_value(inner);
            }

            return Err(super::Error::InvalidMapFormat(value.to_string()));
        }
        ValueType::Array => {
            if value.starts_with('[') && value.ends_with(']') {
                let inner = &value[1..value.len() - 1];
                return parse_array_value(inner);
            }

            return Err(super::Error::InvalidArrayFormat(value.to_string()));
        }
    }

    parse_primitive_value(value)
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
        assert!(int_unit.is_int());
        if let Value::Int(num) = int_unit {
            assert_eq!(num.value, 100);
            assert_eq!(num.unit, Some("kg".to_string()));
        }

        let float_unit = float_with_unit(72.5, "cm");
        assert!(float_unit.is_float());
        if let Value::Float(num) = float_unit {
            assert_eq!(num.value, 72.5);
            assert_eq!(num.unit, Some("cm".to_string()));
        }
    }

    #[test]
    fn test_value_type_name() {
        assert_eq!(null().type_name(), NULL_TYPE);
        assert_eq!(string("test").type_name(), STRING_TYPE);
        assert_eq!(int(42).type_name(), INT_TYPE);
        assert_eq!(float(3.14).type_name(), FLOAT_TYPE);
        assert_eq!(bool_val(true).type_name(), BOOL_TYPE);
        assert_eq!(datetime(1609459200).type_name(), DATETIME_TYPE);
        assert_eq!(map().type_name(), MAP_TYPE);
        assert_eq!(array().type_name(), ARRAY_TYPE);
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
    fn test_cast_methods() {
        assert_eq!(int(42).cast_string().unwrap(), string("42"));
        assert_eq!(float(3.14).cast_string().unwrap(), string("3.14"));
        assert_eq!(bool_val(true).cast_string().unwrap(), string("true"));

        assert_eq!(int(42).cast_float().unwrap().float().unwrap().value(), 42.0);
        assert_eq!(
            bool_val(true)
                .cast_float()
                .unwrap()
                .float()
                .unwrap()
                .value(),
            1.0
        );
    }

    #[test]
    fn test_map_operations() {
        let mut m = map();

        assert_eq!(m.map().unwrap().len(), 3);
        assert_eq!(
            m.map().unwrap().get(&string("key1")).unwrap(),
            &string("value1")
        );
        assert!(m.map().unwrap().contains_key(&string("key2")));

        m.map_mut().unwrap().set(string("key4"), bool_val(false));
        assert_eq!(m.map().unwrap().len(), 4);
        assert_eq!(
            m.map().unwrap().get(&string("key4")).unwrap(),
            &bool_val(false)
        );

        let removed = m.map_mut().unwrap().remove(&string("key1")).unwrap();
        assert_eq!(removed, string("value1"));
        assert_eq!(m.map().unwrap().len(), 3);
        assert!(!m.map().unwrap().contains_key(&string("key1")));

        m.map_mut().unwrap().clear();
        assert_eq!(m.map().unwrap().len(), 0);
    }

    #[test]
    fn test_advanced_map_operations() {
        let mut m = map();

        // To avoid temporary variable reference issues, use a separate variable to store the guard
        let map_guard = m.map().unwrap();
        assert_eq!(map_guard.get(&string("key1")).unwrap(), &string("value1"));

        // Use the cloned default value
        let default_val = string("default");
        let nonexistent_key = string("nonexistent");
        let result = match map_guard.get(&nonexistent_key) {
            Some(val) => val,
            None => &default_val,
        };
        assert_eq!(result, &default_val);

        // Modify value
        {
            let mut map_guard_mut = m.map_mut().unwrap();
            if let Some(val) = map_guard_mut.get_mut(&string("key1")) {
                *val = int(100);
            }
        }

        assert_eq!(m.map().unwrap().get(&string("key1")).unwrap(), &int(100));

        // To avoid the non-existent try_get method, use get and error handling
        assert!(m.map().unwrap().get(&string("key2")).is_some());
        assert!(m.map().unwrap().get(&string("nonexistent")).is_none());

        // Use set instead of get_or_insert
        {
            let mut map_guard_mut = m.map_mut().unwrap();
            map_guard_mut.set(string("new_key"), bool_val(true));
        }
        assert_eq!(
            m.map().unwrap().get(&string("new_key")).unwrap(),
            &bool_val(true)
        );

        // Use set instead of get_or_insert_with
        {
            let mut map_guard_mut = m.map_mut().unwrap();
            map_guard_mut.set(string("lazy_key"), float(99.9));
        }
        assert_eq!(
            m.map().unwrap().get(&string("lazy_key")).unwrap(),
            &float(99.9)
        );

        // Iterate count
        let mut iter_count = 0;
        for (k, _) in m.map().unwrap().iter() {
            assert!(k.is_string());
            iter_count += 1;
        }
        assert_eq!(iter_count, 5);

        // Use iter_mut
        {
            let mut map_guard_mut = m.map_mut().unwrap();
            for (_, v) in map_guard_mut.iter_mut() {
                if let Value::Int(n) = v {
                    n.value *= 2;
                }
            }
        }
        assert_eq!(
            m.map().unwrap().get(&string("key2")).unwrap(),
            &int(84) // 42 * 2
        );

        assert!(!m.map().unwrap().is_empty());
        m.map_mut().unwrap().clear();
        assert!(m.map().unwrap().is_empty());

        let mut m = map();
        {
            let mut map_guard_mut = m.map_mut().unwrap();
            map_guard_mut.retain(|k, _| {
                if let Value::String(s) = k {
                    s.to_string() != "key1"
                } else {
                    true
                }
            });
        }
        assert_eq!(m.map().unwrap().len(), 2);
        assert!(m.map().unwrap().get(&string("key1")).is_none());
        assert!(m.map().unwrap().get(&string("key2")).is_some());
    }

    #[test]
    fn test_array_operations() {
        let mut arr = array();

        assert_eq!(arr.array().unwrap().len(), 3);
        assert_eq!(arr.array().unwrap().get(0).unwrap(), &string("item1"));

        arr.array_mut().unwrap().push(bool_val(false));
        assert_eq!(arr.array().unwrap().len(), 4);
        assert_eq!(arr.array().unwrap().get(3).unwrap(), &bool_val(false));

        let removed = arr.array_mut().unwrap().remove(0).unwrap();
        assert_eq!(removed, string("item1"));
        assert_eq!(arr.array().unwrap().len(), 3);
        assert_eq!(arr.array().unwrap().get(0).unwrap(), &int(42));

        let arr_guard = arr.array().unwrap();
        let items: Vec<&Value> = arr_guard.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], &int(42));
        assert_eq!(items[1], &float(3.14));
        assert_eq!(items[2], &bool_val(false));

        arr.array_mut().unwrap().clear();
        assert_eq!(arr.array().unwrap().len(), 0);
    }

    #[test]
    fn test_advanced_array_operations() {
        let mut arr = array();

        // Test array_insert
        arr.array_mut().unwrap().insert(1, bool_val(true)).unwrap();
        assert_eq!(arr.array().unwrap().len(), 4);
        assert_eq!(arr.array().unwrap().get(1).unwrap(), &bool_val(true));

        // Test array_set
        arr.array_mut().unwrap().set(0, string("replaced")).unwrap();
        assert_eq!(arr.array().unwrap().get(0).unwrap(), &string("replaced"));

        // Test array_filter (requires manual implementation of filtering logic)
        let filtered = {
            let mut result = Vec::new();
            for v in arr.array().unwrap().iter() {
                if v.is_bool() || v.is_int() {
                    result.push(v.clone());
                }
            }
            Value::Array(result)
        };

        if let Value::Array(filtered_arr) = filtered {
            assert_eq!(filtered_arr.len(), 2);
            assert!(filtered_arr.contains(&bool_val(true)));
            assert!(filtered_arr.contains(&int(42)));
        }

        // Test array mapping (manually implement mapping logic)
        let mapped = {
            let mut result = Vec::new();
            for v in arr.array().unwrap().iter() {
                if let Value::Int(num) = v {
                    result.push(Value::Int(Number::new(num.value * 2)));
                } else {
                    result.push(v.clone());
                }
            }
            Value::Array(result)
        };

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

        // Test array sorting
        let mut unsorted = Value::Array(vec![int(3), int(1), int(2)]);
        unsorted.array_mut().unwrap().sort();
        let unsorted_array_guard = unsorted.array().unwrap();
        let sorted_arr = unsorted_array_guard.as_slice();
        assert_eq!(sorted_arr[0], int(1));
        assert_eq!(sorted_arr[1], int(2));
        assert_eq!(sorted_arr[2], int(3));

        // Test array sorting with custom comparator (descending order)
        let mut unsorted = Value::Array(vec![int(3), int(1), int(2)]);
        unsorted
            .array_mut()
            .unwrap()
            .sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        let unsorted_array_guard = unsorted.array().unwrap();
        let sorted_arr = unsorted_array_guard.as_slice();
        assert_eq!(sorted_arr[0], int(3));
        assert_eq!(sorted_arr[1], int(2));
        assert_eq!(sorted_arr[2], int(1));

        // Test array contains
        assert!(arr.array().unwrap().contains(&int(42)));
        assert_eq!(arr.array().unwrap().contains(&string("nonexistent")), false);

        // Test array index_of
        assert_eq!(arr.array().unwrap().index_of(&int(42)), Some(2));
        assert!(!arr.array().unwrap().contains(&string("nonexistent")));

        // Test array join
        let joined = arr.array().unwrap().join(", ");
        assert!(joined.contains("replaced"));
        assert!(joined.contains("true"));
        assert!(joined.contains("42"));
        assert!(joined.contains("3.14"));

        // Test array slice (manually implement slicing logic)
        let sliced = {
            let arr_guard = arr.array().unwrap();
            let arr_slice = arr_guard.as_slice();
            let start = 1;
            let end = 3.min(arr_slice.len());
            let result = arr_slice[start..end].to_vec();
            Value::Array(result)
        };

        if let Value::Array(sliced_arr) = sliced {
            assert_eq!(sliced_arr.len(), 2);
            assert_eq!(sliced_arr[0], bool_val(true));
            assert_eq!(sliced_arr[1], int(42));
        }
    }
}
