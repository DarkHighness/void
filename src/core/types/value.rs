use std::{fmt::Display, str::FromStr};

use chrono::TimeZone;
use serde::{Deserialize, Serialize};

use super::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    String(String),
    Int(Number<i64>),
    Float(Number<f64>),
    Bool(bool),
    DateTime(chrono::DateTime<chrono::Utc>),
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
        Value::String(string)
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
        Value::String(string.to_string())
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
            if let Some(datetime) = datetime.and_local_timezone(chrono::Utc).single() {
                return Ok(datetime.into());
            }
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
        DataType::String => value.to_string().into(),
        DataType::Int => parse_number_value::<i64>(value)?,
        DataType::Float => parse_number_value::<f64>(value)?,
        DataType::Bool => parse_bool_value(value)?,
        DataType::DateTime => parse_datetime_value(value)?,
    };

    Ok(value)
}
