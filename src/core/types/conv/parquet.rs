use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::basic::{ConvertedType, LogicalType, Type as ParquetType};
use parquet::schema::types::{Type, TypePtr};
use thiserror::Error;

use crate::core::types::value::Number;
use crate::core::types::{intern, Attribute, Record, Value};
use crate::utils::tracing::TracingContext;

#[derive(Debug, Error)]
pub enum ParquetConversionError {
    #[error("Invalid type, expected a {0} but got {1}")]
    InvalidType(&'static str, String),

    #[error("Unsupported type for conversion to Parquet: {0}")]
    UnsupportedType(String),

    #[error("Error creating Parquet schema: {0}")]
    SchemaCreationError(String),

    #[error("Error creating Arrow record batch: {0}")]
    RecordBatchCreationError(String),

    #[error("Error writing to Parquet: {0}")]
    WriteError(String),

    #[error("Error reading from Parquet: {0}")]
    ReadError(String),

    #[error("Inconsistent array length in batch conversion")]
    InconsistentArrayLength,

    #[error("Missing column {0} in schema")]
    MissingColumn(String),

    #[error("Empty record set")]
    EmptyRecordSet,
}

/// Convert a Record to a Parquet schema
pub fn record_to_schema(record: &Record) -> Result<TypePtr, ParquetConversionError> {
    let mut fields = Vec::new();

    // Process regular fields
    for (key, value) in record.iter() {
        let field_name = key.to_string();
        let field_type = value_to_parquet_type(&field_name, value)?;
        fields.push(field_type);
    }

    // Create a group type for the record
    let schema_name = match record.get_type() {
        Some(Value::String(sym)) => sym.to_string(),
        _ => "record".to_string(),
    };

    // Create the Parquet schema
    let schema = Type::group_type_builder(&schema_name)
        .with_fields(fields)
        .build()
        .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?;

    Ok(Arc::new(schema))
}

/// Convert a Value to a Parquet type
fn value_to_parquet_type(name: &str, value: &Value) -> Result<TypePtr, ParquetConversionError> {
    match value {
        Value::Null => {
            // For null values, we'll default to OPTIONAL STRING
            Ok(Arc::new(
                Type::primitive_type_builder(name, ParquetType::BYTE_ARRAY)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::UTF8)
                    .build()
                    .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
            ))
        }
        Value::String(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::BYTE_ARRAY)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_converted_type(parquet::basic::ConvertedType::UTF8)
                .build()
                .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
        )),
        Value::Int(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
        )),
        Value::Float(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::DOUBLE)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
        )),
        Value::Bool(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::BOOLEAN)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
        )),
        Value::DateTime(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_converted_type(parquet::basic::ConvertedType::TIMESTAMP_MILLIS)
                .build()
                .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
        )),
        Value::Map(_) => {
            // For maps, create a group with key-value pairs
            let mut fields = Vec::new();

            if let Value::Map(map_data) = value {
                for (k, v) in map_data {
                    let key_name = match k {
                        Value::String(s) => s.to_string(),
                        _ => k.to_string(),
                    };

                    let field_type = value_to_parquet_type(&key_name, v)?;
                    fields.push(field_type);
                }
            }

            Ok(Arc::new(
                Type::group_type_builder(name)
                    .with_fields(fields)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .build()
                    .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
            ))
        }
        Value::Array(arr) => {
            // For homogeneous arrays, create a list type
            if arr.is_empty() {
                // Default to string list for empty arrays
                return Ok(Arc::new(
                    Type::group_type_builder(name)
                        .with_repetition(parquet::basic::Repetition::OPTIONAL)
                        .with_converted_type(parquet::basic::ConvertedType::LIST)
                        .with_fields(vec![Arc::new(
                            Type::group_type_builder("list")
                                .with_repetition(parquet::basic::Repetition::REPEATED)
                                .with_fields(vec![Arc::new(
                                    Type::primitive_type_builder(
                                        "element",
                                        ParquetType::BYTE_ARRAY,
                                    )
                                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                                    .with_converted_type(parquet::basic::ConvertedType::UTF8)
                                    .build()
                                    .map_err(|e| {
                                        ParquetConversionError::SchemaCreationError(e.to_string())
                                    })?,
                                )])
                                .build()
                                .map_err(|e| {
                                    ParquetConversionError::SchemaCreationError(e.to_string())
                                })?,
                        )])
                        .build()
                        .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
                ));
            }

            // Determine type from the first element
            let element_type = match &arr[0] {
                Value::String(_) => "string",
                Value::Int(_) => "int64",
                Value::Float(_) => "double",
                Value::Bool(_) => "boolean",
                Value::DateTime(_) => "timestamp",
                _ => {
                    return Err(ParquetConversionError::UnsupportedType(
                        "complex type in array".to_string(),
                    ))
                }
            };

            // Create appropriate list element type
            let element_parquet_type = match element_type {
                "string" => Type::primitive_type_builder("element", ParquetType::BYTE_ARRAY)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::UTF8)
                    .build(),
                "int64" => Type::primitive_type_builder("element", ParquetType::INT64)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .build(),
                "double" => Type::primitive_type_builder("element", ParquetType::DOUBLE)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .build(),
                "boolean" => Type::primitive_type_builder("element", ParquetType::BOOLEAN)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .build(),
                "timestamp" => Type::primitive_type_builder("element", ParquetType::INT64)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::TIMESTAMP_MILLIS)
                    .build(),
                _ => unreachable!(),
            }
            .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?;

            Ok(Arc::new(
                Type::group_type_builder(name)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::LIST)
                    .with_fields(vec![Arc::new(
                        Type::group_type_builder("list")
                            .with_repetition(parquet::basic::Repetition::REPEATED)
                            .with_fields(vec![Arc::new(element_parquet_type)])
                            .build()
                            .map_err(|e| {
                                ParquetConversionError::SchemaCreationError(e.to_string())
                            })?,
                    )])
                    .build()
                    .map_err(|e| ParquetConversionError::SchemaCreationError(e.to_string()))?,
            ))
        }
    }
}

/// Convert a collection of Records to Arrow schema
pub fn records_to_arrow_schema(records: &[Record]) -> Result<SchemaRef, ParquetConversionError> {
    if records.is_empty() {
        return Err(ParquetConversionError::EmptyRecordSet);
    }

    // Use the first record to determine schema
    let first_record = &records[0];
    let mut fields = Vec::new();

    // Add fields for each key in the first record
    for (key, value) in first_record.iter() {
        let field_name = key.to_string();
        let field_type = value_to_arrow_type(value)?;
        fields.push(Field::new(field_name, field_type, true)); // nullable=true
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Convert a Value to Arrow DataType
fn value_to_arrow_type(value: &Value) -> Result<ArrowDataType, ParquetConversionError> {
    match value {
        Value::Null => Ok(ArrowDataType::Utf8), // Default to string for null
        Value::String(_) => Ok(ArrowDataType::Utf8),
        Value::Int(_) => Ok(ArrowDataType::Int64),
        Value::Float(_) => Ok(ArrowDataType::Float64),
        Value::Bool(_) => Ok(ArrowDataType::Boolean),
        Value::DateTime(_) => Ok(ArrowDataType::Timestamp(
            arrow::datatypes::TimeUnit::Millisecond,
            None,
        )),
        Value::Map(_) => {
            // Convert map to struct
            let mut fields = Vec::new();

            if let Value::Map(map_data) = value {
                for (k, v) in map_data {
                    let key_name = match k {
                        Value::String(s) => s.to_string(),
                        _ => k.to_string(),
                    };

                    let field_type = value_to_arrow_type(v)?;
                    fields.push(Field::new(key_name, field_type, true));
                }
            }

            Ok(ArrowDataType::Struct(fields.into()))
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                // Default to list of strings for empty arrays
                return Ok(ArrowDataType::List(Arc::new(Field::new(
                    "item",
                    ArrowDataType::Utf8,
                    true,
                ))));
            }

            // Get type from first element
            let item_type = value_to_arrow_type(&arr[0])?;
            Ok(ArrowDataType::List(Arc::new(Field::new(
                "item", item_type, true,
            ))))
        }
    }
}

/// Convert Records to Arrow RecordBatch
pub fn records_to_record_batch(
    records: &[Record],
    schema: SchemaRef,
) -> Result<RecordBatch, ParquetConversionError> {
    if records.is_empty() {
        return Err(ParquetConversionError::EmptyRecordSet);
    }

    let mut columns: HashMap<String, Vec<Option<Value>>> = HashMap::new();

    // Initialize columns based on schema
    for field in schema.fields() {
        columns.insert(field.name().clone(), Vec::with_capacity(records.len()));
    }

    // Populate columns
    for record in records {
        for field in schema.fields() {
            let field_name = field.name();
            let column = columns
                .get_mut(field_name)
                .ok_or_else(|| ParquetConversionError::MissingColumn(field_name.clone()))?;

            let symbol = intern(field_name);
            let value = record.get(&symbol).cloned();
            column.push(value);
        }
    }

    // Convert columns to Arrow arrays
    let mut arrow_columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        let column_data = columns
            .get(field_name)
            .ok_or_else(|| ParquetConversionError::MissingColumn(field_name.clone()))?;

        let arrow_array = match field.data_type() {
            ArrowDataType::Utf8 => {
                let string_array: StringArray = column_data
                    .iter()
                    .map(|opt_val| match opt_val {
                        Some(Value::String(s)) => Some(s.to_string()),
                        Some(Value::Null) => None,
                        Some(val) => Some(val.to_string()),
                        None => None,
                    })
                    .collect();
                Arc::new(string_array) as ArrayRef
            }
            ArrowDataType::Int64 => {
                let int_array: Int64Array = column_data
                    .iter()
                    .map(|opt_val| match opt_val {
                        Some(Value::Int(n)) => Some(n.value),
                        Some(Value::Float(n)) => Some(n.value as i64),
                        Some(Value::Null) => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(int_array) as ArrayRef
            }
            ArrowDataType::Float64 => {
                let float_array: Float64Array = column_data
                    .iter()
                    .map(|opt_val| match opt_val {
                        Some(Value::Float(n)) => Some(n.value),
                        Some(Value::Int(n)) => Some(n.value as f64),
                        Some(Value::Null) => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(float_array) as ArrayRef
            }
            ArrowDataType::Boolean => {
                let bool_array: BooleanArray = column_data
                    .iter()
                    .map(|opt_val| match opt_val {
                        Some(Value::Bool(b)) => Some(*b),
                        Some(Value::Null) => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(bool_array) as ArrayRef
            }
            ArrowDataType::Timestamp(_, _) => {
                let ts_array: Int64Array = column_data
                    .iter()
                    .map(|opt_val| match opt_val {
                        Some(Value::DateTime(dt)) => Some(dt.timestamp_millis()),
                        Some(Value::Null) => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(ts_array) as ArrayRef
            }
            // For complex types like lists and structs, we would need more elaborate handling
            // This is a simplified implementation
            _ => {
                return Err(ParquetConversionError::UnsupportedType(
                    field.data_type().to_string(),
                ))
            }
        };

        arrow_columns.push(arrow_array);
    }

    RecordBatch::try_new(schema, arrow_columns)
        .map_err(|e| ParquetConversionError::RecordBatchCreationError(e.to_string()))
}

/// Convert from Arrow RecordBatch to Records
pub fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<Record>, ParquetConversionError> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut records = Vec::with_capacity(num_rows);

    // Create a Record for each row
    for row_idx in 0..num_rows {
        let ctx = TracingContext::new_root();
        let mut record = Record::new(ctx);

        // Set fields from columns
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let key = intern(field.name());

            let value =
                match field.data_type() {
                    ArrowDataType::Utf8 => {
                        let string_array = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                ParquetConversionError::InvalidType(
                                    "StringArray",
                                    "unknown".to_string(),
                                )
                            })?;

                        if string_array.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::from(string_array.value(row_idx))
                        }
                    }
                    ArrowDataType::Int64 => {
                        let int_array =
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    ParquetConversionError::InvalidType(
                                        "Int64Array",
                                        "unknown".to_string(),
                                    )
                                })?;

                        if int_array.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::from(int_array.value(row_idx))
                        }
                    }
                    ArrowDataType::Float64 => {
                        let float_array = column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                ParquetConversionError::InvalidType(
                                    "Float64Array",
                                    "unknown".to_string(),
                                )
                            })?;

                        if float_array.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::from(float_array.value(row_idx))
                        }
                    }
                    ArrowDataType::Boolean => {
                        let bool_array = column
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                ParquetConversionError::InvalidType(
                                    "BooleanArray",
                                    "unknown".to_string(),
                                )
                            })?;

                        if bool_array.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::from(bool_array.value(row_idx))
                        }
                    }
                    ArrowDataType::Timestamp(time_unit, _) => {
                        let ts_array =
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    ParquetConversionError::InvalidType(
                                        "Int64Array for Timestamp",
                                        "unknown".to_string(),
                                    )
                                })?;

                        if ts_array.is_null(row_idx) {
                            Value::Null
                        } else {
                            let ts_value = ts_array.value(row_idx);
                            match time_unit {
                                arrow::datatypes::TimeUnit::Millisecond => {
                                    let dt = chrono::DateTime::from_timestamp_millis(ts_value)
                                        .ok_or_else(|| {
                                            ParquetConversionError::InvalidType(
                                                "valid timestamp",
                                                ts_value.to_string(),
                                            )
                                        })?;
                                    Value::DateTime(dt)
                                }
                                _ => {
                                    return Err(ParquetConversionError::UnsupportedType(
                                        "non-millisecond timestamp".to_string(),
                                    ))
                                }
                            }
                        }
                    }
                    // For complex types like lists and structs, we would need more elaborate handling
                    _ => {
                        return Err(ParquetConversionError::UnsupportedType(
                            field.data_type().to_string(),
                        ))
                    }
                };

            record.set(key, value);
        }

        records.push(record);
    }

    Ok(records)
}

// Add conversion support for a single record
impl TryFrom<&Record> for SchemaRef {
    type Error = ParquetConversionError;

    fn try_from(record: &Record) -> Result<Self, Self::Error> {
        records_to_arrow_schema(&[record.clone()])
    }
}

/// Convert from Parquet type to Rust type value
fn convert_parquet_type_to_value(
    field: &Type,
    default_value: Option<&str>,
) -> Result<Value, ParquetConversionError> {
    // Convert based on the physical type of the field
    match field.get_physical_type() {
        ParquetType::BOOLEAN => Ok(Value::Bool(default_value.map_or(false, |v| v == "true"))),
        ParquetType::INT32 => Ok(Value::Int(Number::new(
            default_value.map_or(0, |v| v.parse::<i64>().unwrap_or(0)),
        ))),
        ParquetType::INT96 => Ok(Value::Int(Number::new(
            default_value.map_or(0, |v| v.parse::<i64>().unwrap_or(0)),
        ))),
        ParquetType::INT64 => Ok(Value::Int(Number::new(
            default_value.map_or(0, |v| v.parse::<i64>().unwrap_or(0)),
        ))),
        ParquetType::FLOAT => Ok(Value::Float(Number::new(
            default_value.map_or(0.0, |v| v.parse::<f64>().unwrap_or(0.0)),
        ))),
        ParquetType::DOUBLE => Ok(Value::Float(Number::new(
            default_value.map_or(0.0, |v| v.parse::<f64>().unwrap_or(0.0)),
        ))),
        ParquetType::BYTE_ARRAY => {
            // Check if it's a UTF8 string
            let basic_info = field.get_basic_info();
            if basic_info.converted_type() == ConvertedType::UTF8
                || matches!(basic_info.logical_type(), Some(LogicalType::String))
            {
                Ok(Value::String(intern(default_value.unwrap_or(""))))
            } else {
                // Other binary types default to string
                Ok(Value::String(intern(default_value.unwrap_or(""))))
            }
        }
        ParquetType::FIXED_LEN_BYTE_ARRAY => {
            // Check if it's a specific logical type
            let basic_info = field.get_basic_info();
            if basic_info.converted_type() == ConvertedType::DECIMAL
                || matches!(basic_info.logical_type(), Some(LogicalType::Decimal { .. }))
            {
                // For decimals, convert to float
                Ok(Value::Float(Number::new(
                    default_value.map_or(0.0, |v| v.parse::<f64>().unwrap_or(0.0)),
                )))
            } else {
                // Default to string
                Ok(Value::String(intern(default_value.unwrap_or(""))))
            }
        }
    }
}

/// Recursively parse Parquet type structure
fn parse_parquet_type(field: &Type) -> Result<Value, ParquetConversionError> {
    if field.is_primitive() {
        // Handle primitive data types
        convert_parquet_type_to_value(field, None)
    } else if field.is_group() {
        // Handle group types
        let basic_info = field.get_basic_info();

        // Check if it's a LIST type
        if basic_info.converted_type() == ConvertedType::LIST
            || matches!(basic_info.logical_type(), Some(LogicalType::List))
        {
            // Parse LIST type
            // Parquet LIST structure: group list { repeated group element { field item; } }
            let children = field.get_fields();
            if !children.is_empty() && children[0].is_group() {
                let list_group = children[0].as_ref();
                let list_items = list_group.get_fields();

                if !list_items.is_empty() {
                    // Create an empty array, actual data handling needed in real applications
                    return Ok(Value::Array(Vec::new()));
                }
            }

            // Default to empty array
            Ok(Value::Array(Vec::new()))
        }
        // Check if it's a MAP type
        else if basic_info.converted_type() == ConvertedType::MAP
            || matches!(basic_info.logical_type(), Some(LogicalType::Map))
        {
            // Parquet MAP structure: group map { repeated group key_value { required type key; optional type value; } }
            let mut map_data = HashMap::new();

            // Process map children fields
            let children = field.get_fields();
            if !children.is_empty() && children[0].is_group() {
                // Get the key-value pairs group
                let key_value_group = children[0].as_ref();
                let entries = key_value_group.get_fields();

                // A valid map should have both key and value fields
                if entries.len() >= 2 {
                    // For schema definition, we'll just create a map with sample key-value
                    // The key field (typically index 0)
                    let key_field = entries[0].as_ref();
                    let key_value = convert_parquet_type_to_value(key_field, Some("key"))?;

                    // The value field (typically index 1)
                    let value_field = entries[1].as_ref();
                    let value_value = parse_parquet_type(value_field)?;

                    // Add the key-value sample to map
                    map_data.insert(key_value, value_value);
                }
            }

            Ok(Value::Map(map_data))
        }
        // Regular struct
        else {
            let mut record_map = HashMap::new();

            // Recursively handle child fields
            for child in field.get_fields() {
                let child_name = child.name();
                let child_value = parse_parquet_type(child.as_ref())?;

                // Remove "name=" prefix from the name
                let clean_name = if let Some(name) = child_name.strip_prefix("name=") {
                    name
                } else {
                    child_name
                };

                record_map.insert(Value::from(clean_name), child_value);
            }

            Ok(Value::Map(record_map))
        }
    } else {
        Err(ParquetConversionError::UnsupportedType(format!(
            "Unsupported Parquet type: {:?}",
            field
        )))
    }
}

// Implement TryFrom<TypePtr> for Record
impl TryFrom<&TypePtr> for Record {
    type Error = ParquetConversionError;

    fn try_from(schema: &TypePtr) -> Result<Self, Self::Error> {
        let mut record = Record::new_root();
        let schema_ref = schema.as_ref();

        // Set record type
        let type_name = if let Some(schema_name) = schema_ref.name().strip_prefix("name=") {
            schema_name
        } else {
            schema_ref.name()
        };

        record.set_attribute(Attribute::Type, Value::String(intern(type_name)));

        // If it's a group type, parse all fields
        if schema_ref.is_group() {
            for field in schema_ref.get_fields() {
                let field_name = if let Some(name) = field.name().strip_prefix("name=") {
                    name
                } else {
                    field.name()
                };

                // Recursively parse fields and add to record
                let field_value = parse_parquet_type(field.as_ref())?;
                record.set(intern(field_name), field_value);
            }
        }

        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::value::Number;
    use crate::core::types::{intern, Value};

    use parquet::arrow::ArrowWriter;
    use parquet::basic::{ConvertedType, LogicalType};
    use parquet::file::properties::WriterProperties;

    use std::fs::File;

    use std::io::Write;
    use tempfile::tempdir;

    fn create_test_record() -> Record {
        let mut record = Record::empty();
        record.set(intern("string_field"), Value::from("string value"));
        record.set(intern("int_field"), Value::Int(Number::new(42)));
        record.set(intern("float_field"), Value::Float(Number::new(3.14)));
        record.set(intern("bool_field"), Value::Bool(true));

        let datetime = chrono::Utc::now();
        record.set(intern("datetime_field"), Value::DateTime(datetime));

        let mut nested_map = HashMap::new();
        nested_map.insert(Value::from("nested_key"), Value::from("nested_value"));
        record.set(intern("map_field"), Value::Map(nested_map));

        let array_values = vec![
            Value::Int(Number::new(1)),
            Value::Int(Number::new(2)),
            Value::Int(Number::new(3)),
        ];
        record.set(intern("array_field"), Value::Array(array_values));

        record.set_attribute(Attribute::Type, Value::from("TestRecord"));

        record
    }

    #[test]
    fn test_record_to_schema() {
        let record = create_test_record();
        let schema = record_to_schema(&record).unwrap();

        assert_eq!(schema.name(), "TestRecord");

        // Verify the schema has the correct fields
        let fields = schema.get_fields();
        assert_eq!(fields.len(), 7); // 7 fields in our test record

        // Check field names (ordering might vary)
        let field_names: Vec<_> = fields.iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&"string_field"));
        assert!(field_names.contains(&"int_field"));
        assert!(field_names.contains(&"float_field"));
        assert!(field_names.contains(&"bool_field"));
        assert!(field_names.contains(&"datetime_field"));
        assert!(field_names.contains(&"map_field"));
        assert!(field_names.contains(&"array_field"));

        // Check field types
        for field in fields {
            match field.name() {
                "string_field" => assert_eq!(field.get_physical_type(), ParquetType::BYTE_ARRAY),
                "int_field" => assert_eq!(field.get_physical_type(), ParquetType::INT64),
                "float_field" => assert_eq!(field.get_physical_type(), ParquetType::DOUBLE),
                "bool_field" => assert_eq!(field.get_physical_type(), ParquetType::BOOLEAN),
                "datetime_field" => assert_eq!(field.get_physical_type(), ParquetType::INT64),
                "array_field" => {
                    let is_list = |field: &Arc<Type>| {
                        if field.is_group() {
                            let basic_info = field.get_basic_info();
                            if let Some(logical_type) = basic_info.logical_type() {
                                return logical_type == LogicalType::List;
                            }
                            return basic_info.converted_type() == ConvertedType::LIST;
                        }
                        false
                    };

                    assert!(is_list(field))
                }
                "map_field" => assert_eq!(field.is_group(), true),
                _ => panic!("Unexpected field: {}", field.name()),
            }
        }
    }

    #[test]
    fn test_value_to_arrow_type() {
        assert_eq!(
            value_to_arrow_type(&Value::Null).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            value_to_arrow_type(&Value::String(intern("test"))).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            value_to_arrow_type(&Value::Int(Number::new(42))).unwrap(),
            ArrowDataType::Int64
        );
        assert_eq!(
            value_to_arrow_type(&Value::Float(Number::new(3.14))).unwrap(),
            ArrowDataType::Float64
        );
        assert_eq!(
            value_to_arrow_type(&Value::Bool(true)).unwrap(),
            ArrowDataType::Boolean
        );

        let dt = chrono::Utc::now();
        assert_eq!(
            value_to_arrow_type(&Value::DateTime(dt)).unwrap(),
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        );

        // Test array type
        let array = Value::Array(vec![Value::Int(Number::new(1))]);
        if let ArrowDataType::List(field) = value_to_arrow_type(&array).unwrap() {
            assert_eq!(field.data_type(), &ArrowDataType::Int64);
        } else {
            panic!("Expected List type");
        }

        // Test map/struct type
        let mut map = HashMap::new();
        map.insert(Value::from("key"), Value::from("value"));
        let map_value = Value::Map(map);

        if let ArrowDataType::Struct(fields) = value_to_arrow_type(&map_value).unwrap() {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "key");
            assert_eq!(fields[0].data_type(), &ArrowDataType::Utf8);
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_records_to_arrow_schema() {
        let records = vec![create_test_record()];
        let schema = records_to_arrow_schema(&records).unwrap();

        assert_eq!(schema.fields().len(), 7);

        // Check field names
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"string_field"));
        assert!(field_names.contains(&"int_field"));
        assert!(field_names.contains(&"float_field"));
        assert!(field_names.contains(&"bool_field"));
        assert!(field_names.contains(&"datetime_field"));
        assert!(field_names.contains(&"map_field"));
        assert!(field_names.contains(&"array_field"));

        // Check field types
        for field in schema.fields() {
            match field.name().as_str() {
                "string_field" => assert_eq!(field.data_type(), &ArrowDataType::Utf8),
                "int_field" => assert_eq!(field.data_type(), &ArrowDataType::Int64),
                "float_field" => assert_eq!(field.data_type(), &ArrowDataType::Float64),
                "bool_field" => assert_eq!(field.data_type(), &ArrowDataType::Boolean),
                "datetime_field" => assert_eq!(
                    field.data_type(),
                    &ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
                ),
                "array_field" => {
                    if let ArrowDataType::List(_) = field.data_type() {
                        // OK
                    } else {
                        panic!("Expected List type for array_field");
                    }
                }
                "map_field" => {
                    if let ArrowDataType::Struct(_) = field.data_type() {
                        // OK
                    } else {
                        panic!("Expected Struct type for map_field");
                    }
                }
                _ => panic!("Unexpected field: {}", field.name()),
            }
        }
    }

    #[test]
    fn test_record_batch_conversion() -> Result<(), Box<dyn std::error::Error>> {
        // Create multiple test records
        let mut records = Vec::new();
        for i in 0..5 {
            let mut record = Record::empty();
            record.set(intern("id"), Value::Int(Number::new(i)));
            record.set(intern("name"), Value::from(format!("Person {}", i)));
            record.set(intern("active"), Value::Bool(i % 2 == 0));
            records.push(record);
        }

        // Convert records to arrow schema and record batch
        let schema = records_to_arrow_schema(&records)?;
        let batch = records_to_record_batch(&records, schema)?;

        // Verify the batch
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);

        // Convert batch back to records
        let round_trip_records = record_batch_to_records(&batch)?;

        // Verify round trip
        assert_eq!(round_trip_records.len(), records.len());

        for (i, record) in round_trip_records.iter().enumerate() {
            assert_eq!(record.get(&intern("id")), records[i].get(&intern("id")));
            assert_eq!(record.get(&intern("name")), records[i].get(&intern("name")));
            assert_eq!(
                record.get(&intern("active")),
                records[i].get(&intern("active"))
            );
        }

        Ok(())
    }

    #[test]
    fn test_parquet_file_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // Create test directory
        let dir = tempdir()?;
        let file_path = dir.path().join("test.parquet");

        // Create test records
        let mut records = Vec::new();
        for i in 0..10 {
            let mut record = Record::empty();
            record.set(intern("id"), Value::Int(Number::new(i)));
            record.set(intern("name"), Value::from(format!("Person {}", i)));
            record.set(intern("score"), Value::Float(Number::new(i as f64 * 1.5)));
            record.set(intern("active"), Value::Bool(i % 2 == 0));
            records.push(record);
        }

        // Convert to Arrow and write to Parquet
        let schema = records_to_arrow_schema(&records)?;
        let batch = records_to_record_batch(&records, schema.clone())?;

        let file = File::create(&file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        // Read from Parquet
        let file = File::open(&file_path)?;

        let arrow_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
            file, 10, // Batch size
        )?;

        let mut retrieved_records = Vec::new();
        for batch_result in arrow_reader {
            let batch = batch_result?;
            let mut records_from_batch = record_batch_to_records(&batch)?;
            retrieved_records.append(&mut records_from_batch);
        }

        // Verify round trip
        assert_eq!(retrieved_records.len(), records.len());

        for (i, record) in retrieved_records.iter().enumerate() {
            assert_eq!(record.get(&intern("id")), records[i].get(&intern("id")));
            assert_eq!(record.get(&intern("name")), records[i].get(&intern("name")));
            assert_eq!(
                record.get(&intern("active")),
                records[i].get(&intern("active"))
            );

            // For floats, check approximate equality
            let original = records[i]
                .get(&intern("score"))
                .and_then(|v| v.float().ok())
                .map(|g| g.value());

            let retrieved = record
                .get(&intern("score"))
                .and_then(|v| v.float().ok())
                .map(|g| g.value());

            if let (Some(orig), Some(retr)) = (original, retrieved) {
                assert!((orig - retr).abs() < 1e-10);
            } else {
                panic!("Missing or invalid score value");
            }
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() {
        // Test empty record set
        let empty_records: Vec<Record> = Vec::new();
        let empty_result = records_to_arrow_schema(&empty_records);
        assert!(empty_result.is_err());

        if let Err(ParquetConversionError::EmptyRecordSet) = empty_result {
            // Expected error
        } else {
            panic!("Expected EmptyRecordSet error");
        }

        // Test unsupported type
        let mut record = Record::empty();
        let mut complex_map = HashMap::new();
        let mut nested_map = HashMap::new();
        nested_map.insert(Value::from("nested"), Value::from("value"));
        complex_map.insert(Value::from("complex"), Value::Map(nested_map));
        record.set(intern("complex_nested"), Value::Map(complex_map));

        // Creating schema should work even with complex types
        let schema_result = record_to_schema(&record);
        assert!(schema_result.is_ok());

        // But converting to Arrow with deeply nested types may fail
        let records = vec![record];
        let arrow_schema_result = records_to_arrow_schema(&records);

        // If it succeeds, trying to create a record batch might still fail
        if let Ok(schema) = arrow_schema_result {
            let batch_result = records_to_record_batch(&records, schema);
            // Either it fails here with UnsupportedType or earlier
            if let Err(ParquetConversionError::UnsupportedType(_)) = batch_result {
                // Expected error
            } else if batch_result.is_ok() {
                // Some implementations might handle this case
            } else {
                panic!("Unexpected error type");
            }
        }
    }

    #[test]
    fn test_tryfrom_record_batch() -> Result<(), Box<dyn std::error::Error>> {
        // Create test records
        let mut records = Vec::new();
        for i in 0..5 {
            let mut record = Record::empty();
            record.set(intern("id"), Value::Int(Number::new(i)));
            record.set(intern("name"), Value::from(format!("Person {}", i)));
            record.set(intern("active"), Value::Bool(i % 2 == 0));
            records.push(record);
        }

        // Use TryFrom to convert to RecordBatch
        let schema = records_to_arrow_schema(&records)?;
        let batch = records_to_record_batch(&records, schema)?;

        // Verify batch content
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);

        // Use TryFrom to convert back to Vec<Record>
        let round_trip_records = record_batch_to_records(&batch)?;

        // Verify round trip conversion
        assert_eq!(round_trip_records.len(), records.len());

        for (i, record) in round_trip_records.iter().enumerate() {
            assert_eq!(record.get(&intern("id")), records[i].get(&intern("id")));
            assert_eq!(record.get(&intern("name")), records[i].get(&intern("name")));
            assert_eq!(
                record.get(&intern("active")),
                records[i].get(&intern("active"))
            );
        }

        Ok(())
    }

    #[test]
    fn test_tryfrom_schema() -> Result<(), Box<dyn std::error::Error>> {
        let record = create_test_record();

        // Create Parquet schema
        let schema = record_to_schema(&record)?;

        // Use TryFrom to convert schema back to Record
        let converted_record = Record::try_from(&schema)?;

        // Verify type attribute is preserved
        assert_eq!(
            converted_record.get_attribute(&Attribute::Type).unwrap(),
            &Value::String(intern("TestRecord"))
        );

        Ok(())
    }

    #[test]
    fn test_record_schema_conversion() -> Result<(), Box<dyn std::error::Error>> {
        // Create a record with special values
        let mut record = Record::empty();
        record.set(
            intern("special_float"),
            Value::Float(Number::new(f64::INFINITY)),
        );
        record.set(
            intern("neg_inf"),
            Value::Float(Number::new(f64::NEG_INFINITY)),
        );
        record.set(intern("nan_value"), Value::Float(Number::new(f64::NAN)));

        // Convert to Arrow schema
        let schema = SchemaRef::try_from(&record)?;

        // Verify schema contains correct fields
        assert_eq!(schema.fields().len(), 3);

        // Create RecordBatch
        let schema = records_to_arrow_schema(&[record.clone()])?;
        let batch = records_to_record_batch(&[record], schema)?;

        // Verify batch content
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        Ok(())
    }
}
