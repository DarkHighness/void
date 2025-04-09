use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use miette::Diagnostic;
use parquet::file::reader::FileReader;
use thiserror::Error;

use crate::core::types::value::Number;
use crate::core::types::{intern, Record, Value};
use crate::utils::tracing::TracingContext;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Inconsistent array type in batch conversion")]
    InconsistentArrayType,

    #[error("Missing column {0} in schema")]
    MissingColumn(String),

    #[error("Empty record set")]
    EmptyRecordSet,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

pub fn value_to_data_type(value: &Value) -> Result<ArrowDataType, Error> {
    match value {
        Value::Null => Ok(ArrowDataType::Utf8),
        Value::String(_) => Ok(ArrowDataType::Utf8),
        Value::Int(_) => Ok(ArrowDataType::Int64),
        Value::Float(_) => Ok(ArrowDataType::Float64),
        Value::Bool(_) => Ok(ArrowDataType::Boolean),
        Value::DateTime(_) => Ok(ArrowDataType::Int64),
        Value::Map(fields) => {
            let mut arrow_fields = Vec::new();

            for (k, v) in fields {
                let key_name = match k {
                    Value::String(s) => s.to_string(),
                    _ => k.to_string(),
                };

                let field_type = match value_to_data_type(v) {
                    Ok(t) => t,
                    Err(e) => return Err(e),
                };

                let field = Field::new(&key_name, field_type.clone(), true);
                arrow_fields.push(field);
            }

            let fields = Fields::from(arrow_fields);
            Ok(ArrowDataType::Struct(fields))
        }
        Value::Array(fields) => {
            if fields.is_empty() {
                return Ok(ArrowDataType::List(Arc::new(Field::new(
                    "element",
                    ArrowDataType::Utf8,
                    true,
                ))));
            }

            let first_type = value_to_data_type(&fields[0])?;
            for field in fields.iter().skip(1) {
                let current_type = value_to_data_type(field)?;
                if first_type != current_type {
                    return Err(Error::InconsistentArrayType);
                }
            }

            let first_item = &fields[0];
            let element_type = match first_item {
                Value::String(_) => ArrowDataType::Utf8,
                Value::Int(_) => ArrowDataType::Int64,
                Value::Float(_) => ArrowDataType::Float64,
                Value::Bool(_) => ArrowDataType::Boolean,
                Value::DateTime(_) => ArrowDataType::Int64,
                _ => ArrowDataType::Utf8,
            };

            Ok(ArrowDataType::List(Arc::new(Field::new(
                "element",
                element_type,
                true,
            ))))
        }
    }
}

/// Convert a Record to a Parquet schema
pub fn record_to_schema(record: &Record) -> Result<SchemaRef, Error> {
    let mut fields = Vec::new();

    for (key, value) in record.iter() {
        let field_name = key.as_str();
        let field_type = value_to_data_type(value)?;

        let field = Field::new(field_name, field_type, true);
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}

/// Directly write Records to a Parquet file
pub fn write_records_to_parquet(
    records: &[Record],
    schema: SchemaRef,
    path: &str,
    props: Option<parquet::file::properties::WriterProperties>,
) -> Result<(), Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    let batch = records_to_record_batch(records, schema.clone())?;

    let file = std::fs::File::create(path)?;

    let props =
        props.unwrap_or_else(|| parquet::file::properties::WriterProperties::builder().build());

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;

    writer.write(&batch)?;

    writer.close()?;

    Ok(())
}

/// Convert Records to Arrow RecordBatch
pub fn records_to_record_batch(
    records: &[Record],
    schema: SchemaRef,
) -> Result<RecordBatch, Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    let mut column_data: BTreeMap<String, Vec<Option<Value>>> = BTreeMap::new();

    for field in schema.fields() {
        column_data.insert(field.name().clone(), Vec::with_capacity(records.len()));
    }

    for record in records {
        for field in schema.fields() {
            let field_name = field.name();
            if let Some(column) = column_data.get_mut(field_name) {
                let value = record.get(&intern(field_name)).cloned();
                column.push(value);
            }
        }
    }

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
                        Value::Int(n) => Some(n.value),
                        Value::DateTime(d) => Some(d.timestamp_millis()),
                        _ => None,
                    })
                }));
                arrays.push(Arc::new(int_array));
            }
            ArrowDataType::Float64 => {
                let float_array = Float64Array::from_iter(values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Float(f) => Some(f.value),
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
            ArrowDataType::Struct(fields) => {
                panic!("Struct type is not supported yet");
            }
            ArrowDataType::List(_) => {
                panic!("List type is not supported yet");
            }
            _ => {
                let string_array = StringArray::from_iter(
                    values.iter().map(|v| v.as_ref().map(|val| val.to_string())),
                );
                arrays.push(Arc::new(string_array));
            }
        }
    }

    RecordBatch::try_new(schema, arrays).map_err(From::from)
}

/// Convert Arrow RecordBatch to Records
pub fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<Record>, Error> {
    let mut records = Vec::with_capacity(batch.num_rows());
    let schema = batch.schema();
    let tracing_context = TracingContext::new_root();

    for row_idx in 0..batch.num_rows() {
        let mut record = Record::new(tracing_context.clone());

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
                    if let Some(value) = column.as_any().downcast_ref::<StringArray>() {
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

/// A writer for writing Records to Parquet files
pub struct ParquetWriter {
    writer: parquet::arrow::ArrowWriter<std::fs::File>,
    schema: SchemaRef,
    path: String,
}

impl ParquetWriter {
    /// Create a new ParquetWriter with default properties
    pub fn new(path: &str, schema: SchemaRef) -> Result<Self, Error> {
        Self::with_properties(path, schema, None)
    }

    /// Create a new ParquetWriter with custom properties
    pub fn with_properties(
        path: &str,
        schema: SchemaRef,
        props: Option<parquet::file::properties::WriterProperties>,
    ) -> Result<Self, Error> {
        let file = std::fs::File::create(path)?;

        let props =
            props.unwrap_or_else(|| parquet::file::properties::WriterProperties::builder().build());

        let writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(Self {
            writer,
            schema,
            path: path.to_string(),
        })
    }

    /// Create a ParquetWriter from a sample record
    pub fn from_record(path: &str, record: &Record) -> Result<Self, Error> {
        let schema = record_to_schema(record)?;
        Self::new(path, schema)
    }

    /// Write a single record to the parquet file
    pub fn write_record(&mut self, record: &Record) -> Result<(), Error> {
        let batch = records_to_record_batch(&[record.clone()], self.schema.clone())?;
        self.writer.write(&batch).map_err(From::from)
    }

    /// Write multiple records to the parquet file
    pub fn write_records(&mut self, records: &[Record]) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }

        let batch = records_to_record_batch(records, self.schema.clone())?;
        self.writer.write(&batch).map_err(From::from)
    }

    /// Write a RecordBatch directly to the parquet file
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        self.writer.write(batch).map_err(From::from)
    }

    /// Flush any buffered data and close the writer
    pub fn close(self) -> Result<(), Error> {
        self.writer.close()?;

        Ok(())
    }

    /// Returns the schema used by this writer
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the path being written to
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// A reader for reading Records from Parquet files
pub struct ParquetReader {
    path: String,
    batch_size: usize,
}

impl ParquetReader {
    /// Create a new ParquetReader
    pub fn new(path: &str, batch_size: usize) -> Self {
        Self {
            path: path.to_string(),
            batch_size,
        }
    }

    /// Read all records from the parquet file
    pub fn read_all(&self) -> Result<Vec<Record>, Error> {
        let file = std::fs::File::open(&self.path)?;

        let arrow_reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, self.batch_size)?;

        let mut all_records = Vec::new();

        for batch_result in arrow_reader {
            let batch = batch_result?;
            let records = record_batch_to_records(&batch)?;
            all_records.extend(records);
        }

        Ok(all_records)
    }

    /// Read as batches
    pub fn read_as_batches(&self) -> Result<Vec<RecordBatch>, Error> {
        let file = std::fs::File::open(&self.path)?;

        let arrow_reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, self.batch_size)?;

        let mut batches = Vec::new();
        for batch_result in arrow_reader {
            let batch = batch_result?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Get the schema of the parquet file
    pub fn schema(&self) -> Result<SchemaRef, Error> {
        let file = std::fs::File::open(&self.path)?;

        let reader = parquet::file::reader::SerializedFileReader::new(file)?;

        // Convert the Parquet schema to Arrow schema
        let parquet_schema = reader.metadata().file_metadata().schema_descr();
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(parquet_schema, None)?;

        Ok(Arc::new(arrow_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::value::Number;
    use crate::utils::tracing::TracingContext;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_value_to_data_type() {
        assert_eq!(
            value_to_data_type(&Value::String("test".into())).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            value_to_data_type(&Value::Int(Number::new(10))).unwrap(),
            ArrowDataType::Int64
        );
        assert_eq!(
            value_to_data_type(&Value::Float(Number::new(10.5))).unwrap(),
            ArrowDataType::Float64
        );
        assert_eq!(
            value_to_data_type(&Value::Bool(true)).unwrap(),
            ArrowDataType::Boolean
        );
    }

    #[test]
    fn test_record_to_schema() {
        let context = TracingContext::new_root();
        let mut record = Record::new(context);
        record.set(intern("string_field"), Value::String("test".into()));
        record.set(intern("int_field"), Value::Int(Number::new(10)));
        record.set(intern("float_field"), Value::Float(Number::new(10.5)));
        record.set(intern("bool_field"), Value::Bool(true));

        let schema = record_to_schema(&record).unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(
            schema.field_with_name("string_field").unwrap().data_type(),
            &ArrowDataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("int_field").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            schema.field_with_name("float_field").unwrap().data_type(),
            &ArrowDataType::Float64
        );
        assert_eq!(
            schema.field_with_name("bool_field").unwrap().data_type(),
            &ArrowDataType::Boolean
        );
    }

    #[test]
    fn test_records_to_record_batch_to_records() {
        let context = TracingContext::new_root();

        let mut record1 = Record::new(context.clone());
        record1.set(intern("string_field"), Value::String("test1".into()));
        record1.set(intern("int_field"), Value::Int(Number::new(10)));

        let mut record2 = Record::new(context.clone());
        record2.set(intern("string_field"), Value::String("test2".into()));
        record2.set(intern("int_field"), Value::Int(Number::new(20)));

        let records = vec![record1, record2];

        let schema = record_to_schema(&records[0]).unwrap();
        let batch = records_to_record_batch(&records, schema).unwrap();

        let converted_records = record_batch_to_records(&batch).unwrap();

        assert_eq!(converted_records.len(), 2);

        let r1 = &converted_records[0];
        let r2 = &converted_records[1];

        assert_eq!(
            r1.get(&intern("string_field")).unwrap().to_string(),
            "test1"
        );
        assert_eq!(r1.get(&intern("int_field")).unwrap().to_string(), "10");
        assert_eq!(
            r2.get(&intern("string_field")).unwrap().to_string(),
            "test2"
        );
        assert_eq!(r2.get(&intern("int_field")).unwrap().to_string(), "20");
    }

    #[test]
    fn test_write_and_read_parquet() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.parquet");
        let file_path_str = file_path.to_str().unwrap();

        let context = TracingContext::new_root();

        let mut record = Record::new(context.clone());
        record.set(intern("string_field"), Value::String("test".into()));
        record.set(intern("int_field"), Value::Int(Number::new(10)));
        record.set(intern("float_field"), Value::Float(Number::new(10.5)));
        record.set(intern("bool_field"), Value::Bool(true));

        let records = vec![record];

        let schema = record_to_schema(&records[0]).unwrap();
        write_records_to_parquet(&records, schema, file_path_str, None).unwrap();

        assert!(file_path.exists());

        let file = fs::File::open(file_path_str).unwrap();

        let arrow_reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 1024).unwrap();

        let mut record_batches = Vec::new();
        for batch_result in arrow_reader {
            let batch = batch_result.unwrap();
            record_batches.push(batch);
        }

        assert!(!record_batches.is_empty());

        let converted_records = record_batch_to_records(&record_batches[0]).unwrap();
        assert_eq!(converted_records.len(), 1);

        let r = &converted_records[0];
        assert_eq!(r.get(&intern("string_field")).unwrap().to_string(), "test");
        assert_eq!(r.get(&intern("int_field")).unwrap().to_string(), "10");
        assert_eq!(r.get(&intern("float_field")).unwrap().to_string(), "10.5");
        assert_eq!(r.get(&intern("bool_field")).unwrap().to_string(), "true");

        dir.close().unwrap();
    }

    #[test]
    fn test_parquet_writer() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("writer_test.parquet");
        let file_path_str = file_path.to_str().unwrap();

        let context = TracingContext::new_root();

        let mut record1 = Record::new(context.clone());
        record1.set(intern("string_field"), Value::String("test1".into()));
        record1.set(intern("int_field"), Value::Int(Number::new(10)));

        let mut record2 = Record::new(context.clone());
        record2.set(intern("string_field"), Value::String("test2".into()));
        record2.set(intern("int_field"), Value::Int(Number::new(20)));

        let mut writer = ParquetWriter::from_record(file_path_str, &record1).unwrap();

        writer.write_records(&[record1, record2]).unwrap();
        writer.close().unwrap();

        let reader = ParquetReader::new(file_path_str, 1024);
        let records = reader.read_all().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].get(&intern("string_field")).unwrap().to_string(),
            "test1"
        );
        assert_eq!(
            records[0].get(&intern("int_field")).unwrap().to_string(),
            "10"
        );
        assert_eq!(
            records[1].get(&intern("string_field")).unwrap().to_string(),
            "test2"
        );
        assert_eq!(
            records[1].get(&intern("int_field")).unwrap().to_string(),
            "20"
        );

        dir.close().unwrap();
    }

    #[test]
    fn test_parquet_writer_batches() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("batch_test.parquet");
        let file_path_str = file_path.to_str().unwrap();

        let context = TracingContext::new_root();

        let mut record = Record::new(context.clone());
        record.set(intern("field1"), Value::String("value1".into()));
        record.set(intern("field2"), Value::Int(Number::new(42)));

        let schema = record_to_schema(&record).unwrap();
        let mut writer = ParquetWriter::new(file_path_str, schema.clone()).unwrap();

        let batch = records_to_record_batch(&[record], schema).unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        let reader = ParquetReader::new(file_path_str, 1024);
        let batches = reader.read_as_batches().unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);

        dir.close().unwrap();
    }
}
