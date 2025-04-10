use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use thiserror::Error;

use crate::core::types::{intern, resolve, value::Number, Record, Value};

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Invalid type, expected a {0} but got {1}")]
    InvalidType(&'static str, &'static str),
}

impl TryFrom<&Value> for JsonValue {
    type Error = ConversionError;
    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        let json_value = match value {
            Value::Null => JsonValue::Null,
            Value::Bool(b) => JsonValue::Bool(*b),
            Value::Int(i) => JsonValue::Number((i.value).into()),
            Value::Float(f) => {
                let f = f.value;
                // Handle potential NaN and Infinity values that are not supported in JSON
                if f.is_nan() {
                    JsonValue::Null
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        JsonValue::String("Infinity".into())
                    } else {
                        JsonValue::String("-Infinity".into())
                    }
                } else {
                    match serde_json::Number::from_f64(f) {
                        Some(num) => JsonValue::Number(num),
                        None => JsonValue::Null,
                    }
                }
            }
            Value::String(s) => JsonValue::String(s.to_string()),
            Value::Array(arr) => {
                let json_array = arr
                    .iter()
                    .map(|v| JsonValue::try_from(v))
                    .collect::<Result<Vec<_>, _>>()?;
                JsonValue::Array(json_array)
            }
            Value::Map(map) => {
                let mut json_map = Map::new();
                for (key, val) in map {
                    let key_guard = key
                        .string()
                        .map_err(|_| ConversionError::InvalidType("String", key.type_name()))?;
                    json_map.insert(key_guard.as_str().into(), JsonValue::try_from(val)?);
                }
                JsonValue::Object(json_map)
            }
            Value::DateTime(dt) => {
                let dt_str = dt.to_string();
                JsonValue::String(dt_str)
            }
        };

        Ok(json_value)
    }
}

impl TryFrom<Value> for JsonValue {
    type Error = ConversionError;
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        JsonValue::try_from(&value)
    }
}

impl TryFrom<&JsonValue> for Value {
    type Error = ConversionError;
    fn try_from(json: &JsonValue) -> std::result::Result<Self, Self::Error> {
        match json {
            JsonValue::Null => Ok(Value::Null),
            JsonValue::Bool(b) => Ok(Value::Bool(*b)),
            JsonValue::Number(n) => {
                if n.is_i64() {
                    Ok(Value::Int(Number::new(n.as_i64().unwrap())))
                } else if n.is_f64() {
                    Ok(Value::Float(Number::new(n.as_f64().unwrap())))
                } else {
                    // Default to i64 for u64
                    Ok(Value::Int(Number::new(n.as_u64().unwrap() as i64)))
                }
            }
            JsonValue::String(s) => {
                // Handle special floating point values
                if s == "Infinity" {
                    return Ok(Value::Float(Number::new(f64::INFINITY)));
                } else if s == "-Infinity" {
                    return Ok(Value::Float(Number::new(f64::NEG_INFINITY)));
                }
                Ok(Value::String(intern(s)))
            }
            JsonValue::Array(arr) => {
                let values = arr
                    .iter()
                    .map(|v| Value::try_from(v))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Value::Array(values))
            }
            JsonValue::Object(map) => {
                let mut values = HashMap::new();
                for (k, v) in map {
                    values.insert(Value::from(intern(k)), Value::try_from(v)?);
                }
                Ok(Value::Map(values))
            }
        }
    }
}

impl TryFrom<JsonValue> for Value {
    type Error = ConversionError;
    fn try_from(json: JsonValue) -> std::result::Result<Self, Self::Error> {
        Value::try_from(&json)
    }
}

impl Record {
    /// Convert the Record to a serde_json::Value
    pub fn to_json(&self) -> std::result::Result<JsonValue, ConversionError> {
        let mut map = Map::new();

        // Convert regular values
        for (key, value) in self.iter() {
            map.insert(resolve(key).into(), JsonValue::try_from(value)?);
        }

        // Convert attributes
        for (key, value) in self.attributes().iter() {
            map.insert(key.to_string(), JsonValue::try_from(value)?);
        }

        let json_value = JsonValue::Object(map);

        Ok(json_value)
    }

    /// Create a Record from a serde_json::Value
    pub fn from_json(json: &JsonValue) -> std::result::Result<Self, ConversionError> {
        match json {
            JsonValue::Object(map) => {
                let mut record = Record::empty();

                for (key, val) in map {
                    // Check if key is an attribute (starts with "__" and ends with "__")
                    if key.starts_with("__") && key.ends_with("__") {
                        // Handle attribute keys
                        match key.as_str() {
                            "__type__" => {
                                if let JsonValue::String(type_val) = val {
                                    record.set_attribute(
                                        crate::core::types::Attribute::Type,
                                        Value::String(intern(type_val)),
                                    );
                                }
                            }
                            // Add other attribute handlers as needed
                            _ => {
                                // For unknown attributes, just add them as regular fields
                                record.set(intern(key), Value::try_from(val)?);
                            }
                        }
                    } else {
                        // Regular key-value pair
                        record.set(intern(key), Value::try_from(val)?);
                    }
                }
                Ok(record)
            }
            _ => Err(ConversionError::InvalidType(
                "Object",
                "non-object JSON value",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{intern, Attribute};
    use serde_json::json;

    #[test]
    fn test_value_to_json() {
        // Test simple values
        assert_eq!(JsonValue::try_from(&Value::Null).unwrap(), JsonValue::Null);
        assert_eq!(
            JsonValue::try_from(&Value::Bool(true)).unwrap(),
            json!(true)
        );
        assert_eq!(
            JsonValue::try_from(&Value::Int(Number::new(42))).unwrap(),
            json!(42)
        );
        assert_eq!(
            JsonValue::try_from(&Value::Float(Number::new(3.14))).unwrap(),
            json!(3.14)
        );

        // Test string
        let sym = intern("hello");
        assert_eq!(
            JsonValue::try_from(&Value::String(sym)).unwrap(),
            json!("hello")
        );

        // Test array
        let arr = vec![
            Value::Int(Number::new(1)),
            Value::String(intern("test")),
            Value::Bool(false),
        ];
        let expected_arr = json!([1, "test", false]);
        assert_eq!(
            JsonValue::try_from(&Value::Array(arr)).unwrap(),
            expected_arr
        );

        // Test map
        let mut map = HashMap::new();
        map.insert(intern("key1").into(), Value::Int(Number::new(100)));
        map.insert(intern("key2").into(), Value::Bool(true));
        let expected_map = json!({
            "key1": 100,
            "key2": true
        });
        assert_eq!(JsonValue::try_from(&Value::Map(map)).unwrap(), expected_map);
    }

    #[test]
    fn test_json_to_value() {
        // Test simple values
        assert_eq!(Value::try_from(&JsonValue::Null).unwrap(), Value::Null);
        assert_eq!(Value::try_from(&json!(true)).unwrap(), Value::Bool(true));
        assert_eq!(
            Value::try_from(&json!(42)).unwrap(),
            Value::Int(Number::new(42))
        );
        assert_eq!(
            Value::try_from(&json!(3.14)).unwrap(),
            Value::Float(Number::new(3.14))
        );

        // Test string
        assert_eq!(
            Value::try_from(&json!("hello")).unwrap(),
            Value::String(intern("hello"))
        );

        // Test special values
        assert_eq!(
            Value::try_from(&json!("Infinity")).unwrap(),
            Value::Float(Number::new(f64::INFINITY))
        );
        assert_eq!(
            Value::try_from(&json!("-Infinity")).unwrap(),
            Value::Float(Number::new(f64::NEG_INFINITY))
        );

        // Test array
        let json_arr = json!([1, "test", false]);
        let expected_arr = Value::Array(vec![
            Value::Int(Number::new(1)),
            Value::String(intern("test")),
            Value::Bool(false),
        ]);
        assert_eq!(Value::try_from(&json_arr).unwrap(), expected_arr);

        // Test object
        let json_obj = json!({
            "key1": 100,
            "key2": true
        });
        let mut expected_map = HashMap::new();
        expected_map.insert(intern("key1").into(), Value::Int(Number::new(100)));
        expected_map.insert(intern("key2").into(), Value::Bool(true));
        assert_eq!(
            Value::try_from(&json_obj).unwrap(),
            Value::Map(expected_map)
        );
    }

    #[test]
    fn test_record_to_json() {
        let mut record = Record::empty();
        record.set(intern("name"), Value::String(intern("test")));
        record.set(intern("age"), Value::Int(Number::new(30)));
        record.set_attribute(Attribute::Type, Value::String(intern("Person")));

        let json = record.to_json();
        let expected = json!({
            "name": "test",
            "age": 30,
            "__type__": "Person"
        });

        assert_eq!(json.unwrap(), expected);
    }

    #[test]
    fn test_json_to_record() {
        let json_val = json!({
            "name": "test",
            "age": 30,
            "__type__": "Person"
        });

        let record = Record::from_json(&json_val).unwrap();

        assert_eq!(
            record.get(&intern("name")).unwrap(),
            &Value::String(intern("test"))
        );
        assert_eq!(
            record.get(&intern("age")).unwrap(),
            &Value::Int(Number::new(30))
        );
        assert_eq!(
            record.get_attribute(&Attribute::Type).unwrap(),
            &Value::String(intern("Person"))
        );
    }

    #[test]
    fn test_special_float_values() {
        // Test NaN
        let nan = Value::Float(Number::new(f64::NAN));
        assert_eq!(JsonValue::try_from(&nan).unwrap(), JsonValue::Null);

        // Test Infinity
        let pos_inf = Value::Float(Number::new(f64::INFINITY));
        assert_eq!(
            JsonValue::try_from(&pos_inf).unwrap(),
            JsonValue::String("Infinity".into())
        );

        // Test -Infinity
        let neg_inf = Value::Float(Number::new(f64::NEG_INFINITY));
        assert_eq!(
            JsonValue::try_from(&neg_inf).unwrap(),
            JsonValue::String("-Infinity".into())
        );
    }

    #[test]
    fn test_nested_structures() {
        // Create a complex nested structure
        let mut nested_map = HashMap::new();
        nested_map.insert(intern("nested").into(), Value::String(intern("value")));

        let arr = vec![Value::Int(Number::new(1)), Value::Map(nested_map.clone())];

        let mut record = Record::empty();
        record.set(intern("array").into(), Value::Array(arr));
        record.set(intern("map").into(), Value::Map(nested_map));

        let json = record.to_json();
        let expected = json!({
            "array": [1, {"nested": "value"}],
            "map": {"nested": "value"}
        });

        assert_eq!(json.unwrap(), expected);
    }

    #[test]
    fn test_bidirectional_conversion() {
        // Create a complex record
        let mut record = Record::empty();
        record.set(intern("name"), Value::String(intern("test")));
        record.set(
            intern("numbers"),
            Value::Array(vec![
                Value::Int(Number::new(1)),
                Value::Int(Number::new(2)),
                Value::Int(Number::new(3)),
            ]),
        );

        let mut nested_map = HashMap::new();
        nested_map.insert(intern("key").into(), Value::String(intern("value")));
        record.set(intern("metadata"), Value::Map(nested_map));
        record.set_attribute(Attribute::Type, Value::String(intern("TestRecord")));

        // Convert to JSON and back
        let json = record.to_json().unwrap();
        let round_trip = Record::from_json(&json).unwrap();

        // Verify key values
        assert_eq!(round_trip.get(&intern("name")), record.get(&intern("name")));
        assert_eq!(
            round_trip.get(&intern("numbers")),
            record.get(&intern("numbers"))
        );
        assert_eq!(
            round_trip.get(&intern("metadata")),
            record.get(&intern("metadata"))
        );
        assert_eq!(
            round_trip.get_attribute(&Attribute::Type),
            record.get_attribute(&Attribute::Type)
        );
    }
}
