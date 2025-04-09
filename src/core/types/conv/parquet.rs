use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use miette::Diagnostic;
use parquet::basic::{ConvertedType, LogicalType, Type as ParquetType};
use parquet::file::reader::FileReader;
use parquet::schema::types::{Type, TypePtr};
use thiserror::Error;

use crate::core::types::value::Number;
use crate::core::types::{intern, Attribute, Record, Value};
use crate::utils::tracing::TracingContext;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
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
pub fn record_to_schema(record: &Record) -> Result<TypePtr, Error> {
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
        .map_err(|e| Error::SchemaCreationError(e.to_string()))?;

    Ok(Arc::new(schema))
}

/// Convert a Value to a Parquet type
fn value_to_parquet_type(name: &str, value: &Value) -> Result<TypePtr, Error> {
    match value {
        Value::Null => {
            // For null values, we'll default to OPTIONAL STRING
            Ok(Arc::new(
                Type::primitive_type_builder(name, ParquetType::BYTE_ARRAY)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::UTF8)
                    .build()
                    .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
            ))
        }
        Value::String(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::BYTE_ARRAY)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_converted_type(parquet::basic::ConvertedType::UTF8)
                .build()
                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
        )),
        Value::Int(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
        )),
        Value::Float(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::DOUBLE)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
        )),
        Value::Bool(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::BOOLEAN)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
        )),
        Value::DateTime(_) => Ok(Arc::new(
            Type::primitive_type_builder(name, ParquetType::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .with_converted_type(parquet::basic::ConvertedType::TIMESTAMP_MILLIS)
                .build()
                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
        )),
        Value::Map(map_data) => {
            // 处理嵌套Map，将其转换为Parquet group
            let mut fields = Vec::new();

            for (k, v) in map_data {
                let key_name = match k {
                    Value::String(s) => s.to_string(),
                    _ => k.to_string(),
                };

                // 尝试转换每个字段，如果失败则降级为字符串类型
                let field_type = match value_to_parquet_type(&key_name, v) {
                    Ok(t) => t,
                    Err(Error::UnsupportedType(_)) => {
                        // 当字段类型不支持时降级为UTF8字符串
                        Arc::new(
                            Type::primitive_type_builder(&key_name, ParquetType::BYTE_ARRAY)
                                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                                .with_converted_type(parquet::basic::ConvertedType::UTF8)
                                .build()
                                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
                        )
                    }
                    Err(e) => return Err(e),
                };

                fields.push(field_type);
            }

            Ok(Arc::new(
                Type::group_type_builder(name)
                    .with_fields(fields)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .build()
                    .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
            ))
        }
        Value::Array(arr) => {
            // 处理数组
            if arr.is_empty() {
                // 空数组默认处理为字符串列表
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
                                    .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
                                )])
                                .build()
                                .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
                        )])
                        .build()
                        .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
                ));
            }

            // 确定元素类型
            let first_item = &arr[0];
            let element_type = match first_item {
                Value::String(_) => create_string_element_type()?,
                Value::Int(_) => create_int64_element_type()?,
                Value::Float(_) => create_double_element_type()?,
                Value::Bool(_) => create_boolean_element_type()?,
                Value::DateTime(_) => create_timestamp_element_type()?,
                _ => {
                    // 对于复杂类型，尝试转换为JSON字符串表示
                    create_string_element_type()?
                }
            };

            Ok(Arc::new(
                Type::group_type_builder(name)
                    .with_repetition(parquet::basic::Repetition::OPTIONAL)
                    .with_converted_type(parquet::basic::ConvertedType::LIST)
                    .with_fields(vec![Arc::new(
                        Type::group_type_builder("list")
                            .with_repetition(parquet::basic::Repetition::REPEATED)
                            .with_fields(vec![element_type])
                            .build()
                            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
                    )])
                    .build()
                    .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
            ))
        }
    }
}

// 辅助函数：创建字符串元素类型
fn create_string_element_type() -> Result<TypePtr, Error> {
    Ok(Arc::new(
        Type::primitive_type_builder("element", ParquetType::BYTE_ARRAY)
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .with_converted_type(parquet::basic::ConvertedType::UTF8)
            .build()
            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
    ))
}

// 辅助函数：创建Int64元素类型
fn create_int64_element_type() -> Result<TypePtr, Error> {
    Ok(Arc::new(
        Type::primitive_type_builder("element", ParquetType::INT64)
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .build()
            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
    ))
}

// 辅助函数：创建Double元素类型
fn create_double_element_type() -> Result<TypePtr, Error> {
    Ok(Arc::new(
        Type::primitive_type_builder("element", ParquetType::DOUBLE)
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .build()
            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
    ))
}

// 辅助函数：创建Boolean元素类型
fn create_boolean_element_type() -> Result<TypePtr, Error> {
    Ok(Arc::new(
        Type::primitive_type_builder("element", ParquetType::BOOLEAN)
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .build()
            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
    ))
}

// 辅助函数：创建时间戳元素类型
fn create_timestamp_element_type() -> Result<TypePtr, Error> {
    Ok(Arc::new(
        Type::primitive_type_builder("element", ParquetType::INT64)
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .with_converted_type(parquet::basic::ConvertedType::TIMESTAMP_MILLIS)
            .build()
            .map_err(|e| Error::SchemaCreationError(e.to_string()))?,
    ))
}

/// Directly write Records to a Parquet file
pub fn write_records_to_parquet(
    records: &[Record],
    path: &str,
    props: Option<parquet::file::properties::WriterProperties>,
) -> Result<(), Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    // Convert to Arrow schema and record batch
    let schema = records_to_arrow_schema(records)?;
    let batch = records_to_record_batch(records, schema.clone())?;

    // Create file and write
    let file = std::fs::File::create(path).map_err(|e| Error::WriteError(e.to_string()))?;

    let props =
        props.unwrap_or_else(|| parquet::file::properties::WriterProperties::builder().build());

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| Error::WriteError(e.to_string()))?;

    writer
        .write(&batch)
        .map_err(|e| Error::WriteError(e.to_string()))?;

    writer
        .close()
        .map_err(|e| Error::WriteError(e.to_string()))?;

    Ok(())
}

/// Read Parquet file into Records
pub fn read_parquet_to_records(path: &str, batch_size: usize) -> Result<Vec<Record>, Error> {
    // Open file
    let file = std::fs::File::open(path)
        .map_err(|e| Error::ReadError(format!("Failed to open file: {}", e)))?;

    // Create reader
    let arrow_reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, batch_size)
            .map_err(|e| Error::ReadError(format!("Failed to create reader: {}", e)))?;

    let mut records = Vec::new();

    // Process each batch
    for batch_result in arrow_reader {
        let batch =
            batch_result.map_err(|e| Error::ReadError(format!("Failed to read batch: {}", e)))?;

        let mut batch_records = record_batch_to_records(&batch)?;
        records.append(&mut batch_records);
    }

    Ok(records)
}

/// Get Parquet schema from a file
pub fn get_parquet_schema(path: &str) -> Result<TypePtr, Error> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::ReadError(format!("Failed to open file: {}", e)))?;

    let reader = parquet::file::reader::SerializedFileReader::new(file)
        .map_err(|e| Error::ReadError(format!("Failed to create reader: {}", e)))?;

    let metadata = reader.metadata();
    let file_schema = metadata.file_schema();
    Ok(file_schema.root_schema_ptr())
}

/// Create a Record template from a Parquet schema
pub fn parquet_schema_to_record_template(schema: &TypePtr) -> Result<Record, Error> {
    Record::try_from(schema)
}

/// Convert Records to Arrow schema
pub fn records_to_arrow_schema(records: &[Record]) -> Result<SchemaRef, Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    // Use the first record to determine the schema
    let mut fields = Vec::new();

    for (key, value) in records[0].iter() {
        let field_name = key.to_string();
        let field_type = match value {
            Value::Null => ArrowDataType::Utf8,
            Value::String(_) => ArrowDataType::Utf8,
            Value::Int(_) => ArrowDataType::Int64,
            Value::Float(_) => ArrowDataType::Float64,
            Value::Bool(_) => ArrowDataType::Boolean,
            Value::DateTime(_) => ArrowDataType::Int64,
            Value::Map(_) => ArrowDataType::Utf8, // Convert maps to JSON strings
            Value::Array(_) => ArrowDataType::Utf8, // Convert arrays to JSON strings
        };

        fields.push(Field::new(field_name, field_type, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Convert Records to Arrow RecordBatch
pub fn records_to_record_batch(
    records: &[Record],
    schema: SchemaRef,
) -> Result<RecordBatch, Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    let mut column_data: HashMap<String, Vec<Option<Value>>> = HashMap::new();

    // Initialize columns based on schema
    for field in schema.fields() {
        column_data.insert(field.name().clone(), Vec::with_capacity(records.len()));
    }

    // Populate data
    for record in records {
        for field in schema.fields() {
            let field_name = field.name();
            if let Some(column) = column_data.get_mut(field_name) {
                let value = record.get(&intern(field_name)).cloned();
                column.push(value);
            }
        }
    }

    // Convert to Arrow arrays
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let field_name = field.name();
        let values = column_data
            .get(field_name)
            .ok_or_else(|| Error::MissingColumn(field_name.clone()))?;

        match field.data_type() {
            ArrowDataType::Utf8 => {
                let string_array = StringArray::from_iter(
                    values.iter().map(|v| v.as_ref().map(|val| val.to_string())),
                );
                arrays.push(Arc::new(string_array));
            }
            ArrowDataType::Int64 => {
                let int_array = Int64Array::from_iter(values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Int(n) => Some(n.as_i64()),
                        Value::DateTime(d) => Some(d.timestamp_millis()),
                        _ => None,
                    })
                }));
                arrays.push(Arc::new(int_array));
            }
            ArrowDataType::Float64 => {
                let float_array = Float64Array::from_iter(values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Float(f) => Some(f.as_f64()),
                        _ => None,
                    })
                }));
                arrays.push(Arc::new(float_array));
            }
            ArrowDataType::Boolean => {
                let bool_array = BooleanArray::from_iter(values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Bool(b) => Some(*b),
                        _ => None,
                    })
                }));
                arrays.push(Arc::new(bool_array));
            }
            _ => {
                // Default to string for unsupported types
                let string_array = StringArray::from_iter(
                    values.iter().map(|v| v.as_ref().map(|val| val.to_string())),
                );
                arrays.push(Arc::new(string_array));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(|e| Error::RecordBatchCreationError(e.to_string()))
}

/// Convert Arrow RecordBatch to Records
pub fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<Record>, Error> {
    let mut records = Vec::with_capacity(batch.num_rows());
    let schema = batch.schema();
    let tracing_context = TracingContext::new_root();

    for row_idx in 0..batch.num_rows() {
        let mut record = Record::new(&tracing_context);

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let field_name = intern(field.name());

            match field.data_type() {
                ArrowDataType::Utf8 => {
                    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                        if !array.is_null(row_idx) {
                            record.set(field_name, Value::from(array.value(row_idx)));
                        }
                    }
                }
                ArrowDataType::Int64 => {
                    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                        if !array.is_null(row_idx) {
                            record.set(field_name, Value::Int(Number::new(array.value(row_idx))));
                        }
                    }
                }
                ArrowDataType::Float64 => {
                    if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                        if !array.is_null(row_idx) {
                            record.set(field_name, Value::Float(Number::new(array.value(row_idx))));
                        }
                    }
                }
                ArrowDataType::Boolean => {
                    if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                        if !array.is_null(row_idx) {
                            record.set(field_name, Value::Bool(array.value(row_idx)));
                        }
                    }
                }
                _ => {
                    // Try to convert to string as fallback
                    if let Ok(value) =
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                Error::InvalidType("StringArray", field.data_type().to_string())
                            })
                    {
                        if !value.is_null(row_idx) {
                            record.set(field_name, Value::from(value.value(row_idx)));
                        }
                    }
                }
            }
        }

        records.push(record);
    }

    Ok(records)
}

/// A higher-level utility for inspecting a Parquet file
pub fn inspect_parquet_file(path: &str) -> Result<Record, Error> {
    let schema = get_parquet_schema(path)?;
    let template = parquet_schema_to_record_template(&schema)?;

    // Add some metadata about the file to the record
    let ctx = template.ctx();
    let mut result = Record::new(ctx.clone());

    // Try to get file metadata
    if let Ok(file) = std::fs::File::open(path) {
        if let Ok(reader) = parquet::file::reader::SerializedFileReader::new(file) {
            let metadata = reader.metadata();
            let num_rows = metadata.file_metadata().num_rows();
            let num_row_groups = metadata.num_row_groups();

            // Add file info to result
            result.set(intern("file_path"), Value::from(path));
            result.set(intern("num_rows"), Value::Int(Number::new(num_rows as i64)));
            result.set(
                intern("num_row_groups"),
                Value::Int(Number::new(num_row_groups as i64)),
            );

            // Add schema template
            let mut fields = HashMap::new();
            for (key, value) in template.iter() {
                fields.insert(Value::String(key.clone()), value.clone());
            }
            result.set(intern("schema"), Value::Map(fields));

            // Set type
            if let Some(type_value) = template.get_attribute(&Attribute::Type) {
                result.set_attribute(Attribute::Type, type_value.clone());
            }
        }
    }

    Ok(result)
}

/// Batch conversion for multiple records with optimized memory usage
pub fn batch_write_records(
    records_iter: impl Iterator<Item = Record>,
    path: &str,
    batch_size: usize,
    props: Option<parquet::file::properties::WriterProperties>,
) -> Result<(), Error> {
    // Create file
    let file = std::fs::File::create(path).map_err(|e| Error::WriteError(e.to_string()))?;

    // Initialize with empty records to derive schema
    let mut records_batch = Vec::with_capacity(batch_size);
    let mut writer: Option<parquet::arrow::ArrowWriter<std::fs::File>> = None;
    let props =
        props.unwrap_or_else(|| parquet::file::properties::WriterProperties::builder().build());

    // Process records in batches
    for record in records_iter {
        records_batch.push(record);

        // When we reach batch size, process the batch
        if records_batch.len() >= batch_size {
            if writer.is_none() {
                // For the first batch, create schema and writer
                let schema = records_to_arrow_schema(&records_batch)?;
                writer = Some(
                    parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
                        .map_err(|e| Error::WriteError(e.to_string()))?,
                );
            }

            // Convert batch to Arrow and write
            if let Some(w) = writer.as_mut() {
                // Create schema from the current batch of records
                let schema = records_to_arrow_schema(&records_batch)?;
                let batch = records_to_record_batch(&records_batch, schema)?;
                w.write(&batch)
                    .map_err(|e| Error::WriteError(e.to_string()))?;
            }

            // Clear batch for next iteration
            records_batch.clear();
        }
    }

    // Process any remaining records
    if !records_batch.is_empty() {
        if writer.is_none() {
            // If we never created a writer (fewer records than batch size)
            let schema = records_to_arrow_schema(&records_batch)?;
            writer = Some(
                parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
                    .map_err(|e| Error::WriteError(e.to_string()))?,
            );
        }

        if let Some(w) = writer.as_mut() {
            let schema = w.schema();
            let batch = records_to_record_batch(&records_batch, schema.clone())?;
            w.write(&batch)
                .map_err(|e| Error::WriteError(e.to_string()))?;
        }
    }

    // Close the writer
    if let Some(w) = writer {
        w.close().map_err(|e| Error::WriteError(e.to_string()))?;
    } else {
        return Err(Error::EmptyRecordSet);
    }

    Ok(())
}
