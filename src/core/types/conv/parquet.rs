use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, MapArray, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use miette::Diagnostic;
use parquet::file::reader::FileReader;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
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

//
// 类型转换基础函数
//

/// 从Value确定对应的Arrow数据类型
pub fn value_to_data_type(value: &Value) -> Result<ArrowDataType, Error> {
    match value {
        Value::Null | Value::String(_) => Ok(ArrowDataType::Utf8),
        Value::Int(_) => Ok(ArrowDataType::Int64),
        Value::Float(_) => Ok(ArrowDataType::Float64),
        Value::Bool(_) => Ok(ArrowDataType::Boolean),
        Value::DateTime(_) => Ok(ArrowDataType::Int64),
        Value::Map(fields) => {
            // 使用迭代器直接构建字段集合
            let arrow_fields = fields
                .iter()
                .map(|(k, v)| {
                    let key_name = match k {
                        Value::String(s) => s.to_string(),
                        _ => k.to_string(),
                    };

                    value_to_data_type(v).map(|field_type| Field::new(&key_name, field_type, true))
                })
                .collect::<Result<Vec<Field>, Error>>()?;

            Ok(ArrowDataType::Struct(Fields::from(arrow_fields)))
        }
        Value::Array(fields) => {
            if fields.is_empty() {
                return Ok(ArrowDataType::List(Arc::new(Field::new(
                    "element",
                    ArrowDataType::Utf8,
                    true,
                ))));
            }

            // 获取第一个元素的类型
            let first_type = value_to_data_type(&fields[0])?;

            // 对于Map类型的数组，需要特殊处理以确保结构兼容性
            if let Value::Map(_) = &fields[0] {
                // 对Map数组，我们只需确保所有键集合相同，不要求内部顺序一致
                let mut all_same_structure = true;
                for field in fields.iter().skip(1) {
                    if let Value::Map(map_data) = field {
                        // 检查当前Map的键集合是否与第一个Map相同
                        if let Value::Map(first_map) = &fields[0] {
                            // 检查键集合
                            let keys_match = map_data.keys().all(|k| {
                                first_map.contains_key(k) &&
                                // 对于相同的键，值类型应该兼容
                                if let (Some(v1), Some(v2)) = (first_map.get(k), map_data.get(k)) {
                                    // 简单比较值类型
                                    std::mem::discriminant(v1) == std::mem::discriminant(v2)
                                } else {
                                    false
                                }
                            }) && first_map.keys().count()
                                == map_data.keys().count();

                            if !keys_match {
                                all_same_structure = false;
                                break;
                            }
                        }
                    } else {
                        // 如果不是Map类型，则类型不一致
                        all_same_structure = false;
                        break;
                    }
                }

                if !all_same_structure {
                    return Err(Error::InconsistentArrayType);
                }

                // 所有Map结构一致，返回List类型
                return Ok(ArrowDataType::List(Arc::new(Field::new(
                    "element", first_type, true,
                ))));
            }

            // 对于非Map类型的数组，使用标准类型比较
            let all_same_type = fields.iter().skip(1).try_fold(true, |_, field| {
                value_to_data_type(field).map(|t| t == first_type)
            })?;

            if !all_same_type {
                return Err(Error::InconsistentArrayType);
            }

            Ok(ArrowDataType::List(Arc::new(Field::new(
                "element", first_type, true,
            ))))
        }
    }
}

/// 将单个Value转换为Arrow数组元素
fn value_to_array_element(value: &Value, data_type: &ArrowDataType) -> Option<Value> {
    match (value, data_type) {
        (Value::Null, _) => None,
        (_, ArrowDataType::Utf8) => Some(Value::String(value.to_string().into())),
        (Value::Int(n), ArrowDataType::Int64) => Some(Value::Int(n.clone())),
        (Value::Float(f), ArrowDataType::Float64) => Some(Value::Float(f.clone())),
        (Value::Bool(b), ArrowDataType::Boolean) => Some(Value::Bool(*b)),
        (Value::DateTime(dt), ArrowDataType::Int64) => {
            Some(Value::Int(Number::new(dt.timestamp_millis())))
        }
        (_, _) => Some(value.clone()),
    }
}

//
// 基本数据类型转换函数
//

/// 将Values转换为StringArray
fn values_to_string_array(values: &[Option<Value>]) -> StringArray {
    StringArray::from_iter(values.iter().map(|v| {
        v.as_ref().map(|val| match val {
            Value::Null => "null".to_string(),
            _ => val.to_string(),
        })
    }))
}

/// 将Values转换为Int64Array
fn values_to_int_array(values: &[Option<Value>]) -> Int64Array {
    Int64Array::from_iter(values.iter().map(|v| {
        v.as_ref().and_then(|val| match val {
            Value::Int(n) => Some(n.value),
            Value::DateTime(d) => Some(d.timestamp_millis()),
            _ => None,
        })
    }))
}

/// 将Values转换为Float64Array
fn values_to_float_array(values: &[Option<Value>]) -> Float64Array {
    Float64Array::from_iter(values.iter().map(|v| {
        v.as_ref().and_then(|val| match val {
            Value::Float(f) => Some(f.value),
            _ => None,
        })
    }))
}

/// 将Values转换为BooleanArray
fn values_to_bool_array(values: &[Option<Value>]) -> BooleanArray {
    BooleanArray::from_iter(values.iter().map(|v| {
        v.as_ref().and_then(|val| match val {
            Value::Bool(b) => Some(*b),
            _ => None,
        })
    }))
}

//
// 复合数据类型转换
//

/// 将Value数组转换为ListArray
fn values_to_list_array(values: &[Option<Value>], field: &Field) -> Result<ListArray, Error> {
    // 获取元素类型
    let element_field = match field.data_type() {
        ArrowDataType::List(element_field) => element_field.clone(),
        _ => {
            // 从数据推断元素类型
            let element_type = values
                .iter()
                .find_map(|v| {
                    v.as_ref().and_then(|val| {
                        if let Value::Array(items) = val {
                            if !items.is_empty() {
                                return Some(value_to_data_type(&items[0]).ok()?);
                            }
                        }
                        None
                    })
                })
                .unwrap_or(ArrowDataType::Utf8);

            Arc::new(Field::new("element", element_type, true))
        }
    };

    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut child_values = Vec::new();
    offsets.push(0);

    for value in values {
        if let Some(Value::Array(items)) = value {
            for item in items {
                child_values.push(Some(item.clone()));
            }
        }
        offsets.push(child_values.len() as i32);
    }

    // 创建子数组
    let child_array = values_to_array(&child_values, element_field.data_type())?;

    // 创建偏移缓冲区
    let offset_buffer = OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));

    // 创建列表数组
    Ok(ListArray::new(
        element_field,
        offset_buffer,
        child_array,
        None,
    ))
}

/// 将Value映射转换为MapArray
fn values_to_map_array(values: &[Option<Value>], field: &Field) -> Result<MapArray, Error> {
    // 准备键值对存储
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut key_values = Vec::new();
    let mut item_values = Vec::new();
    offsets.push(0);

    // 确定值类型
    let mut value_type = ArrowDataType::Utf8;

    // 收集所有键值对数据
    for value in values {
        if let Some(Value::Map(map)) = value {
            for (k, v) in map {
                // 键总是使用字符串
                match k {
                    Value::String(s) => key_values.push(Some(Value::String(s.clone()))),
                    _ => key_values.push(Some(Value::String(k.to_string().into()))),
                }

                // 记录第一个非空值的类型
                if value_type == ArrowDataType::Utf8 {
                    if let Ok(vt) = value_to_data_type(v) {
                        value_type = vt;
                    }
                }

                item_values.push(Some(v.clone()));
            }
        }
        offsets.push(key_values.len() as i32);
    }

    // 创建键和值数组
    let key_array = values_to_string_array(&key_values);
    let value_array = values_to_array(&item_values, &value_type)?;
    let offset_buffer = OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));

    // 创建结构类型和结构数组
    let entry_fields = Fields::from(vec![
        Field::new("key", ArrowDataType::Utf8, false),
        Field::new("value", value_type, true),
    ]);

    let struct_array = StructArray::try_new(
        entry_fields.clone(),
        vec![Arc::new(key_array), value_array.clone()],
        None,
    )?;

    // 创建映射数组
    Ok(MapArray::new(
        Arc::new(Field::new(
            "",
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(entry_fields),
                    true,
                )),
                false,
            ),
            true,
        )),
        offset_buffer,
        struct_array,
        None,
        false,
    ))
}

/// 将Value结构转换为StructArray
fn values_to_struct_array(values: &[Option<Value>], field: &Field) -> Result<StructArray, Error> {
    let struct_fields = match field.data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        _ => {
            // 尝试从数据推断结构字段
            values
                .iter()
                .find_map(|v| {
                    v.as_ref().and_then(|val| {
                        if let Value::Map(map) = val {
                            let mut fields = Vec::new();
                            for (k, v) in map {
                                let key_name = match k {
                                    Value::String(s) => s.to_string(),
                                    _ => k.to_string(),
                                };
                                if let Ok(field_type) = value_to_data_type(v) {
                                    fields.push(Field::new(&key_name, field_type, true));
                                }
                            }
                            return Some(Fields::from(fields));
                        }
                        None
                    })
                })
                .unwrap_or_else(Fields::empty)
        }
    };

    // 准备字段值存储
    let mut field_values: BTreeMap<String, Vec<Option<Value>>> = BTreeMap::new();

    // 为每个字段创建存储空间
    for field in struct_fields.iter() {
        field_values.insert(field.name().clone(), vec![None; values.len()]);
    }

    // 填充字段值
    for (i, value) in values.iter().enumerate() {
        if let Some(Value::Map(map)) = value {
            for (k, v) in map {
                let key_str = match k {
                    Value::String(s) => s.to_string(),
                    _ => k.to_string(),
                };

                if let Some(values) = field_values.get_mut(&key_str) {
                    values[i] = Some(v.clone());
                }
            }
        }
    }

    // 为每个字段创建数组
    let mut field_arrays = Vec::with_capacity(struct_fields.len());

    for field in struct_fields.iter() {
        let field_name = field.name();
        let default_value = vec![None; values.len()];
        let field_value_array = field_values.get(field_name).unwrap_or(&default_value);
        let array = values_to_array(field_value_array, field.data_type())?;
        field_arrays.push(array);
    }

    // 构建结构数组
    Ok(StructArray::try_new(struct_fields, field_arrays, None)?)
}

/// 通用的Value到Arrow数组转换函数
fn values_to_array(values: &[Option<Value>], data_type: &ArrowDataType) -> Result<ArrayRef, Error> {
    match data_type {
        ArrowDataType::Utf8 => Ok(Arc::new(values_to_string_array(values))),
        ArrowDataType::Int64 => Ok(Arc::new(values_to_int_array(values))),
        ArrowDataType::Float64 => Ok(Arc::new(values_to_float_array(values))),
        ArrowDataType::Boolean => Ok(Arc::new(values_to_bool_array(values))),
        ArrowDataType::List(element_field) => {
            let list_field = Field::new("item", ArrowDataType::List(element_field.clone()), true);
            Ok(Arc::new(values_to_list_array(values, &list_field)?))
        }
        ArrowDataType::Map(_, _) => {
            let field = Field::new("item", data_type.clone(), true);
            Ok(Arc::new(values_to_map_array(values, &field)?))
        }
        ArrowDataType::Struct(_) => {
            let field = Field::new("item", data_type.clone(), true);
            Ok(Arc::new(values_to_struct_array(values, &field)?))
        }
        _ => Err(Error::InconsistentArrayType),
    }
}

/// 从Arrow数组提取指定索引的值
fn extract_value_from_array(array: &ArrayRef, index: usize) -> Result<Option<Value>, Error> {
    if array.is_null(index) {
        return Ok(None);
    }

    // 尝试根据数组类型转换
    match array.data_type() {
        ArrowDataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Some(Value::String(array.value(index).into())))
        }
        ArrowDataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Some(Value::Int(Number::new(array.value(index)))))
        }
        ArrowDataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(Some(Value::Float(Number::new(array.value(index)))))
        }
        ArrowDataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Some(Value::Bool(array.value(index))))
        }
        ArrowDataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let mut values = Vec::new();
            let value_array = array.value(index);

            for i in 0..value_array.len() {
                if let Some(v) = extract_value_from_array(&value_array, i)? {
                    values.push(v);
                }
            }

            Ok(Some(Value::Array(values)))
        }
        ArrowDataType::Map(_, _) => extract_map_value(array, index),
        ArrowDataType::Struct(_) => extract_struct_value(array, index),
        _ => Err(Error::InconsistentArrayType),
    }
}

/// 从MapArray提取Value
fn extract_map_value(array: &ArrayRef, index: usize) -> Result<Option<Value>, Error> {
    let array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or(Error::InconsistentArrayType)?;

    let mut map = HashMap::new();
    let map_array = array.value(index);
    let key_array = map_array.column(0);
    let value_array = map_array.column(1);

    for i in 0..map_array.len() {
        // 尝试提取键
        let key = extract_map_key(key_array, i)?;

        if let Some(key) = key {
            // 提取值
            if let Some(value) = extract_value_from_array(value_array, i)? {
                map.insert(key, value);
            }
        }
    }

    Ok(Some(Value::Map(map)))
}

/// 从各种类型的数组中提取Map键
fn extract_map_key(array: &ArrayRef, index: usize) -> Result<Option<Value>, Error> {
    if array.is_null(index) {
        return Ok(None);
    }

    // 尝试从不同类型的数组中提取键
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        Ok(Some(Value::String(array.value(index).into())))
    } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        Ok(Some(Value::String(array.value(index).to_string().into())))
    } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        Ok(Some(Value::String(array.value(index).to_string().into())))
    } else if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        Ok(Some(Value::String(array.value(index).to_string().into())))
    } else {
        Err(Error::InconsistentArrayType)
    }
}

/// 从StructArray提取Value
fn extract_struct_value(array: &ArrayRef, index: usize) -> Result<Option<Value>, Error> {
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(Error::InconsistentArrayType)?;

    let mut map = HashMap::new();

    for (field_idx, field) in array.fields().iter().enumerate() {
        let field_array = array.column(field_idx);
        if let Some(value) = extract_value_from_array(field_array, index)? {
            map.insert(Value::String(field.name().clone().into()), value);
        }
    }

    Ok(Some(Value::Map(map)))
}

//
// Record与Schema转换
//

/// 将Record转换为Parquet schema
pub fn record_to_schema(record: &Record) -> Result<SchemaRef, Error> {
    let mut fields = Vec::new();

    for (key, value) in record.iter() {
        let field_name = key.as_str();
        let field_type = value_to_data_type(value)?;
        fields.push(Field::new(field_name, field_type, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// 将Records转换为Arrow RecordBatch
pub fn records_to_record_batch(
    records: &[Record],
    schema: SchemaRef,
) -> Result<RecordBatch, Error> {
    if records.is_empty() {
        return Err(Error::EmptyRecordSet);
    }

    // 准备每列的数据
    let mut column_data: BTreeMap<String, Vec<Option<Value>>> = BTreeMap::new();
    for field in schema.fields() {
        column_data.insert(field.name().clone(), Vec::with_capacity(records.len()));
    }

    // 收集每个记录的值
    for record in records {
        for field in schema.fields() {
            let field_name = field.name();
            if let Some(column) = column_data.get_mut(field_name) {
                let value = record.get(&intern(field_name)).cloned();
                column.push(value);
            }
        }
    }

    // 为每一列创建Arrow数组
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let field_name = field.name();
        let values = column_data
            .get(field_name)
            .ok_or_else(|| Error::MissingColumn(field_name.clone()))?;

        let array = values_to_array(values, field.data_type())?;
        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays).map_err(From::from)
}

/// 将Arrow RecordBatch转换为Records
pub fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<Record>, Error> {
    let schema = batch.schema();
    let tracing_context = TracingContext::new_root();
    let mut records = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut record = Record::new(tracing_context.clone());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let field_name = intern(field.name());

            if let Some(value) = extract_value_from_array(column, row_idx)? {
                record.set(field_name, value);
            }
        }

        records.push(record);
    }

    Ok(records)
}

//
// 文件操作
//

/// 直接将Records写入Parquet文件
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

/// 用于写入Records到Parquet文件的writer
pub struct ParquetWriter {
    writer: parquet::arrow::ArrowWriter<std::fs::File>,
    schema: SchemaRef,
    path: String,
}

impl ParquetWriter {
    /// 创建一个带有默认属性的ParquetWriter
    pub fn new(path: &str, schema: SchemaRef) -> Result<Self, Error> {
        Self::with_properties(path, schema, None)
    }

    /// 创建一个带有自定义属性的ParquetWriter
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

    /// 从样本记录创建ParquetWriter
    pub fn from_record(path: &str, record: &Record) -> Result<Self, Error> {
        let schema = record_to_schema(record)?;
        Self::new(path, schema)
    }

    /// 写入单条记录到parquet文件
    pub fn write_record(&mut self, record: &Record) -> Result<(), Error> {
        let batch = records_to_record_batch(&[record.clone()], self.schema.clone())?;
        self.writer.write(&batch).map_err(From::from)
    }

    /// 写入多条记录到parquet文件
    pub fn write_records(&mut self, records: &[Record]) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }
        let batch = records_to_record_batch(records, self.schema.clone())?;
        self.writer.write(&batch).map_err(From::from)
    }

    /// 直接写入RecordBatch到parquet文件
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        self.writer.write(batch).map_err(From::from)
    }

    /// 刷新缓冲数据并关闭writer
    pub fn close(self) -> Result<(), Error> {
        self.writer.close()?;
        Ok(())
    }

    /// 返回writer使用的schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// 返回写入的文件路径
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// 用于从Parquet文件读取Records的reader
pub struct ParquetReader {
    path: String,
    batch_size: usize,
}

impl ParquetReader {
    /// 创建一个新的ParquetReader
    pub fn new(path: &str, batch_size: usize) -> Self {
        Self {
            path: path.to_string(),
            batch_size,
        }
    }

    /// 读取parquet文件中的所有记录
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

    /// 以批次形式读取
    pub fn read_as_batches(&self) -> Result<Vec<RecordBatch>, Error> {
        let file = std::fs::File::open(&self.path)?;
        let arrow_reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, self.batch_size)?;

        let mut batches = Vec::new();
        for batch_result in arrow_reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }

    /// 获取parquet文件的schema
    pub fn schema(&self) -> Result<SchemaRef, Error> {
        let file = std::fs::File::open(&self.path)?;
        let reader = parquet::file::reader::SerializedFileReader::new(file)?;
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

    #[test]
    fn test_list_array_conversion() {
        let context = TracingContext::new_root();

        let mut record = Record::new(context.clone());
        record.set(
            intern("list_field"),
            Value::Array(vec![
                Value::String("item1".into()),
                Value::String("item2".into()),
                Value::String("item3".into()),
            ]),
        );

        let schema = record_to_schema(&record).unwrap();

        // Convert to RecordBatch and back
        let batch = records_to_record_batch(&[record], schema).unwrap();
        let converted_records = record_batch_to_records(&batch).unwrap();

        assert_eq!(converted_records.len(), 1);

        if let Value::Array(items) = converted_records[0].get(&intern("list_field")).unwrap() {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].to_string(), "item1");
            assert_eq!(items[1].to_string(), "item2");
            assert_eq!(items[2].to_string(), "item3");
        } else {
            panic!("Expected array value");
        }
    }

    #[test]
    fn test_numeric_list_array_conversion() {
        let context = TracingContext::new_root();

        let mut record = Record::new(context.clone());
        record.set(
            intern("int_list"),
            Value::Array(vec![
                Value::Int(Number::new(10)),
                Value::Int(Number::new(20)),
                Value::Int(Number::new(30)),
            ]),
        );

        let schema = record_to_schema(&record).unwrap();
        let batch = records_to_record_batch(&[record], schema).unwrap();
        let converted_records = record_batch_to_records(&batch).unwrap();

        if let Value::Array(items) = converted_records[0].get(&intern("int_list")).unwrap() {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].to_string(), "10");
            assert_eq!(items[1].to_string(), "20");
            assert_eq!(items[2].to_string(), "30");
        } else {
            panic!("Expected array value");
        }
    }

    #[test]
    fn test_map_array_conversion() {
        let context = TracingContext::new_root();

        let mut record = Record::new(context.clone());
        let mut map = HashMap::new();
        map.insert(Value::String("key1".into()), Value::String("value1".into()));
        map.insert(Value::String("key2".into()), Value::String("value2".into()));

        record.set(intern("map_field"), Value::Map(map));

        let schema = record_to_schema(&record).unwrap();
        let batch = records_to_record_batch(&[record.clone()], schema).unwrap();
        let converted_records = record_batch_to_records(&batch).unwrap();

        if let Value::Map(map) = converted_records[0].get(&intern("map_field")).unwrap() {
            assert_eq!(map.len(), 2);

            let key1 = Value::String("key1".into());
            let key2 = Value::String("key2".into());

            assert!(map.contains_key(&key1));
            assert!(map.contains_key(&key2));
            assert_eq!(map.get(&key1).unwrap().to_string(), "value1");
            assert_eq!(map.get(&key2).unwrap().to_string(), "value2");
        } else {
            panic!("Expected map value");
        }
    }

    #[test]
    fn test_nested_structures() {
        let context = TracingContext::new_root();

        // Create nested structures
        let mut record = Record::new(context.clone());

        // Nested map
        let mut inner_map = HashMap::new();
        inner_map.insert(
            Value::String("nested_key".into()),
            Value::Int(Number::new(42)),
        );

        // Nested list
        let inner_list = vec![
            Value::String("list_item1".into()),
            Value::String("list_item2".into()),
        ];

        // Add nested structures to the main record
        record.set(intern("nested_map"), Value::Map(inner_map));
        record.set(intern("nested_list"), Value::Array(inner_list));

        // Write the record to a parquet file and read it back
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nested_test.parquet");
        let file_path_str = file_path.to_str().unwrap();

        let schema = record_to_schema(&record).unwrap();
        write_records_to_parquet(&[record], schema, file_path_str, None).unwrap();

        // Read and verify
        let reader = ParquetReader::new(file_path_str, 1024);
        let records = reader.read_all().unwrap();

        assert_eq!(records.len(), 1);
        let r = &records[0];

        // Verify nested map
        if let Value::Map(map) = r.get(&intern("nested_map")).unwrap() {
            let key = Value::String("nested_key".into());
            assert!(map.contains_key(&key));
            assert_eq!(map.get(&key).unwrap().to_string(), "42");
        } else {
            panic!("Expected map value");
        }

        // Verify nested list
        if let Value::Array(list) = r.get(&intern("nested_list")).unwrap() {
            assert_eq!(list.len(), 2);
            assert_eq!(list[0].to_string(), "list_item1");
            assert_eq!(list[1].to_string(), "list_item2");
        } else {
            panic!("Expected array value");
        }

        dir.close().unwrap();
    }

    #[test]
    fn test_complex_structure() {
        let context = TracingContext::new_root();

        // Create a complex record with multiple levels of nesting
        let mut record = Record::new(context.clone());

        // Map in a list - ensure all maps have same structure and types
        let mut map1 = HashMap::new();
        map1.insert(
            Value::String("name".into()),
            Value::String("Project1".into()),
        );
        map1.insert(Value::String("value".into()), Value::Int(Number::new(100)));

        let mut map2 = HashMap::new();
        map2.insert(
            Value::String("name".into()),
            Value::String("Project2".into()),
        );
        map2.insert(Value::String("value".into()), Value::Int(Number::new(200)));

        let list_of_maps = vec![Value::Map(map1), Value::Map(map2)];
        record.set(intern("items"), Value::Array(list_of_maps));

        // List in a map - ensure consistent types in the array
        let values_list = vec![
            Value::Float(Number::new(1.1)),
            Value::Float(Number::new(2.2)),
            Value::Float(Number::new(3.3)),
        ];

        let mut config_map = HashMap::new();
        config_map.insert(Value::String("name".into()), Value::String("Config".into()));
        config_map.insert(Value::String("values".into()), Value::Array(values_list));

        record.set(intern("config"), Value::Map(config_map));

        // Convert to RecordBatch and back
        let schema = record_to_schema(&record).unwrap();
        let batch = records_to_record_batch(&[record], schema.clone()).unwrap();

        // Write to file and read back for testing
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("complex_test.parquet");
        let file_path_str = file_path.to_str().unwrap();

        let mut writer = ParquetWriter::new(file_path_str, schema).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        let reader = ParquetReader::new(file_path_str, 1024);
        let records = reader.read_all().unwrap();

        assert_eq!(records.len(), 1);
        let r = &records[0];

        // Verify map in a list
        if let Value::Array(items) = r.get(&intern("items")).unwrap() {
            assert_eq!(items.len(), 2);

            if let Value::Map(item1) = &items[0] {
                let name_key = Value::String("name".into());
                let value_key = Value::String("value".into());

                assert_eq!(item1.get(&name_key).unwrap().to_string(), "Project1");
                assert_eq!(item1.get(&value_key).unwrap().to_string(), "100");
            } else {
                panic!("Expected map in array");
            }
        } else {
            panic!("Expected array value");
        }

        // Verify list in a map
        if let Value::Map(config) = r.get(&intern("config")).unwrap() {
            let name_key = Value::String("name".into());
            let values_key = Value::String("values".into());

            assert_eq!(config.get(&name_key).unwrap().to_string(), "Config");

            if let Value::Array(values) = config.get(&values_key).unwrap() {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0].to_string(), "1.1");
                assert_eq!(values[1].to_string(), "2.2");
                assert_eq!(values[2].to_string(), "3.3");
            } else {
                panic!("Expected array in map");
            }
        } else {
            panic!("Expected map value");
        }

        dir.close().unwrap();
    }

    #[test]
    fn test_map_with_null_values() {
        let context = TracingContext::new_root();
        let mut record = Record::new(context.clone());
        let mut map = HashMap::new();
        map.insert(Value::String("key1".into()), Value::String("value1".into()));
        map.insert(Value::String("key2".into()), Value::Null);

        record.set(intern("map_field"), Value::Map(map));

        let schema = record_to_schema(&record).unwrap();
        let batch = records_to_record_batch(&[record.clone()], schema).unwrap();
        let converted_records = record_batch_to_records(&batch).unwrap();

        if let Value::Map(map) = converted_records[0].get(&intern("map_field")).unwrap() {
            assert_eq!(map.len(), 2);

            let key1 = Value::String("key1".into());
            let key2 = Value::String("key2".into());

            assert!(map.contains_key(&key1));
            assert!(map.contains_key(&key2));
            assert_eq!(map.get(&key1).unwrap().to_string(), "value1");
            assert_eq!(map.get(&key2).unwrap().to_string(), "null"); // Null should convert to "null" string
        } else {
            panic!("Expected map value");
        }
    }

    #[test]
    fn test_array_with_null_values() {
        let context = TracingContext::new_root();
        let mut record = Record::new(context.clone());
        record.set(
            intern("list_field"),
            Value::Array(vec![
                Value::String("item1".into()),
                Value::Null,
                Value::String("item3".into()),
            ]),
        );

        let schema = record_to_schema(&record).unwrap();
        let batch = records_to_record_batch(&[record], schema).unwrap();
        let converted_records = record_batch_to_records(&batch).unwrap();

        assert_eq!(converted_records.len(), 1);

        if let Value::Array(items) = converted_records[0].get(&intern("list_field")).unwrap() {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].to_string(), "item1");
            assert_eq!(items[1].to_string(), "null"); // Null should convert to "null" string
            assert_eq!(items[2].to_string(), "item3");
        } else {
            panic!("Expected array value");
        }
    }
}
