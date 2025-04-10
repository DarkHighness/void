use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use chrono::TimeZone;
use nom::{
    bytes::complete::take_while1,
    character::complete::{char, digit1, space1},
    multi::many0,
    sequence::{preceded, separated_pair},
    IResult, Parser,
};
use std::collections::HashMap;
use tokio::io::AsyncReadExt;

use crate::{
    config::protocol::graphite::GraphiteProtocolConfig,
    core::{
        pipe::{NAME_FIELD, TIMESTAMP_FIELD, VALUE_FIELD},
        protocol,
        types::{parse_value, Record, Symbol, SymbolMap, Value, ValueType},
    },
    utils::tracing::TracingContext,
};

/// 解析指标名称 (任何非空格字符)
fn parse_metric_name(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| !c.is_whitespace())(input)
}

use nom::number::complete::double;

/// 解析数值 (浮点数)
fn parse_metric_value(input: &str) -> IResult<&str, f64> {
    double(input)
}

/// 解析时间戳 (Unix timestamp)，并转换成 DateTime<UTC>
fn parse_timestamp(input: &str) -> IResult<&str, chrono::DateTime<chrono::Utc>> {
    digit1(input).and_then(|(next_input, s)| {
        s.parse::<i64>()
            .map(|timestamp| {
                // 根据时间戳位数确定转换策略
                let datetime = match s.len() {
                    10 => chrono::Utc.timestamp_opt(timestamp, 0).single(),
                    13 => chrono::Utc.timestamp_millis_opt(timestamp).single(),
                    16 => {
                        // 微秒转纳秒
                        let nanos = timestamp * 1_000;
                        Some(chrono::Utc.timestamp_nanos(nanos))
                    }
                    19 => Some(chrono::Utc.timestamp_nanos(timestamp)),
                    _ => None, // 不支持其他长度的时间戳
                };

                match datetime {
                    Some(dt) => Ok((next_input, dt)),
                    None => Err(nom::Err::Error(nom::error::Error::new(
                        next_input,
                        nom::error::ErrorKind::MapRes,
                    ))),
                }
            })
            .unwrap_or_else(|_| {
                Err(nom::Err::Error(nom::error::Error::new(
                    next_input,
                    nom::error::ErrorKind::MapRes,
                )))
            })
    })
}

/// 解析键值对，形如 key=value
fn parse_key_value(input: &str) -> IResult<&str, (String, String)> {
    let key_parser = take_while1(|c: char| c != '=' && !c.is_whitespace());
    let value_parser = take_while1(|c: char| !c.is_whitespace());

    separated_pair(key_parser, char('='), value_parser)
        .map(|(k, v): (&str, &str)| (k.to_string(), v.to_string()))
        .parse(input)
}

/// 解析属性 (空格分隔的键值对)
fn parse_attributes(input: &str) -> IResult<&str, HashMap<String, String>> {
    many0(preceded(space1, parse_key_value))
        .map(|pairs| pairs.into_iter().collect::<HashMap<String, String>>())
        .parse(input)
}

/// 直接解析 Graphite 格式数据为 Record
pub fn parse_graphite_to_record<'a>(
    input: &'a str,
    config: &GraphiteProtocolConfig,
) -> IResult<&'a str, Record> {
    let (input, metric_name) = parse_metric_name(input)?;
    let (input, _) = space1(input)?;
    let (input, value) = parse_metric_value(input)?;
    let (input, _) = space1(input)?;
    let (input, timestamp) = parse_timestamp(input)?;
    let (input, raw_attributes) = parse_attributes(input)?;

    // 创建一个新的 Record
    let mut record = Record::new_with_values(SymbolMap::new(), TracingContext::new_root());

    // 添加指标
    record.set(metric_name.into(), Value::Float(value.into()));

    // 添加时间戳
    record.set(TIMESTAMP_FIELD.clone(), Value::DateTime(timestamp));

    // 处理属性，根据配置指定类型
    for (key, value_str) in raw_attributes {
        let key_symbol = Symbol::new(&key);

        // 获取配置中指定的属性类型，如果没有则默认为字符串
        let attribute_type = get_attribute_type(config, &key).unwrap_or(ValueType::String);

        // 解析值为指定类型
        let parsed_value = parse_value(&value_str, attribute_type).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::MapRes))
        })?;

        record.set(key_symbol, parsed_value);
    }

    Ok((input, record))
}

/// 从配置中获取属性类型
fn get_attribute_type(config: &GraphiteProtocolConfig, key: &str) -> Option<ValueType> {
    if let Some(attributes) = &config.attributes {
        if let Some(primitive) = attributes.get(key) {
            return Some(ValueType::from(primitive));
        }
    }
    None
}

const BUFFER_SIZE: usize = 16 * 1024;

pub struct GraphiteProtocolParser<R> {
    reader: R,
    config: GraphiteProtocolConfig,
    input_buf: BytesMut,
}

impl<R> GraphiteProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    pub fn try_create_from(reader: R, cfg: GraphiteProtocolConfig) -> protocol::Result<Self> {
        Ok(Self {
            reader,
            config: cfg,
            input_buf: BytesMut::with_capacity(BUFFER_SIZE),
        })
    }

    async fn read_line(&mut self) -> protocol::Result<Option<String>> {
        let mut line_buf = Vec::new();

        loop {
            // 如果缓冲区不为空，尝试在现有数据中查找行结束符
            if !self.input_buf.is_empty() {
                if let Some(pos) = self.find_line_end() {
                    let line_end_len = if pos < self.input_buf.len() - 1
                        && self.input_buf[pos] == b'\r'
                        && self.input_buf[pos + 1] == b'\n'
                    {
                        2
                    } else {
                        1
                    };

                    // 添加当前行到line_buf
                    line_buf.extend_from_slice(&self.input_buf[..pos]);
                    self.input_buf.advance(pos + line_end_len);

                    return Ok(Some(String::from_utf8_lossy(&line_buf).into_owned()));
                } else {
                    // 没有找到结束符，将所有数据添加到line_buf
                    line_buf.extend_from_slice(&self.input_buf);
                    self.input_buf.clear();
                }
            }

            // 尝试读取更多数据
            match self.reader.read_buf(&mut self.input_buf).await {
                Ok(0) => {
                    // EOF reached
                    if line_buf.is_empty() {
                        return Ok(None);
                    } else {
                        // 返回剩余数据作为最后一行
                        return Ok(Some(String::from_utf8_lossy(&line_buf).into_owned()));
                    }
                }
                Ok(_) => {
                    // 成功读取更多数据，继续循环处理
                }
                Err(e) => return Err(protocol::Error::Io(e)),
            }
        }
    }

    fn find_line_end(&self) -> Option<usize> {
        for i in 0..self.input_buf.len() {
            // 检测换行符 \n 或 \r
            if self.input_buf[i] == b'\n' || self.input_buf[i] == b'\r' {
                return Some(i);
            }
        }
        None
    }
}

#[async_trait]
impl<R> protocol::ProtocolParser for GraphiteProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    async fn read_next(&mut self) -> protocol::Result<Record> {
        loop {
            match self.read_line().await? {
                Some(line) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    match parse_graphite_to_record(&line, &self.config) {
                        Ok((_, record)) => {
                            return Ok(record);
                        }
                        Err(e) => {
                            return Err(protocol::Error::MismatchedFormat(format!(
                                "Failed to parse Graphite line: {:?}",
                                e
                            )));
                        }
                    }
                }
                None => {
                    // EOF reached
                    return Err(protocol::Error::EOF);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{tag::ProtocolTagId, types::Primitive};
    use std::collections::HashMap;

    fn create_test_config() -> GraphiteProtocolConfig {
        GraphiteProtocolConfig {
            tag: ProtocolTagId::new("test"),
            attributes: None,
        }
    }

    fn create_config_with_attributes() -> GraphiteProtocolConfig {
        let mut attributes = HashMap::new();
        attributes.insert("host".to_string(), Primitive::String);
        attributes.insert("region".to_string(), Primitive::String);
        attributes.insert("int_val".to_string(), Primitive::Int);
        attributes.insert("float_val".to_string(), Primitive::Float);
        attributes.insert("bool_val".to_string(), Primitive::Bool);

        GraphiteProtocolConfig {
            tag: ProtocolTagId::new("test"),
            attributes: Some(attributes),
        }
    }

    #[test]
    fn test_parse_graphite_to_record_basic() {
        let input = "system.cpu.usage 42.5 1620000000";
        let config = create_test_config();

        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());

        let (remaining, record) = result.unwrap();
        assert_eq!(remaining, "");

        // 验证记录中的字段
        let metric_name = record.get(&NAME_FIELD).unwrap();
        if let Value::String(name) = metric_name {
            assert_eq!(name.as_str(), "system.cpu.usage");
        } else {
            panic!("Metric name is not a string");
        }

        let value = record.get(&VALUE_FIELD).unwrap();
        if let Value::Float(num) = value {
            assert_eq!(num.value, 42.5);
        } else {
            panic!("Metric value is not a float");
        }

        let timestamp = record.get(&TIMESTAMP_FIELD).unwrap();
        if let Value::DateTime(dt) = timestamp {
            assert_eq!(dt.timestamp(), 1620000000);
        } else {
            panic!("Timestamp is not a datetime");
        }
    }

    #[test]
    fn test_parse_graphite_to_record_with_attributes() {
        let input = "system.memory.free 1024.0 1620000000 host=server01 region=us-west";
        let config = create_config_with_attributes();

        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());

        let (remaining, record) = result.unwrap();
        assert_eq!(remaining, "");

        // 验证属性
        let host = record.get(&Symbol::new("host")).unwrap();
        if let Value::String(host_str) = host {
            assert_eq!(host_str.as_str(), "server01");
        } else {
            panic!("Host is not a string");
        }

        let region = record.get(&Symbol::new("region")).unwrap();
        if let Value::String(region_str) = region {
            assert_eq!(region_str.as_str(), "us-west");
        } else {
            panic!("Region is not a string");
        }
    }

    #[test]
    fn test_attribute_type_conversion() {
        let input = "test.metric 42.5 1620000000 int_val=123 float_val=45.6 bool_val=true";
        let config = create_config_with_attributes();

        let (_, record) = parse_graphite_to_record(input, &config).unwrap();

        // 验证整型属性
        let int_val = record.get(&Symbol::new("int_val")).unwrap();
        if let Value::Int(num) = int_val {
            assert_eq!(num.value, 123);
        } else {
            panic!("int_val is not an integer");
        }

        // 验证浮点型属性
        let float_val = record.get(&Symbol::new("float_val")).unwrap();
        if let Value::Float(num) = float_val {
            assert_eq!(num.value, 45.6);
        } else {
            panic!("float_val is not a float");
        }

        // 验证布尔型属性
        let bool_val = record.get(&Symbol::new("bool_val")).unwrap();
        if let Value::Bool(val) = bool_val {
            assert_eq!(*val, true);
        } else {
            panic!("bool_val is not a boolean");
        }
    }

    #[test]
    fn test_parse_timestamp_seconds() {
        let input = "1620000000";
        let result = parse_timestamp(input);
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        assert_eq!(dt.timestamp(), 1620000000);
    }

    #[test]
    fn test_parse_timestamp_millis() {
        let input = "1620000000123";
        let result = parse_timestamp(input);
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        assert_eq!(dt.timestamp_millis(), 1620000000123);
    }

    #[test]
    fn test_parse_timestamp_micros() {
        let input = "1620000000123456";
        let result = parse_timestamp(input);
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        // 验证微秒部分（转换为纳秒后）
        assert_eq!(dt.timestamp_nanos_opt().unwrap(), 1620000000123456000);
    }

    #[test]
    fn test_parse_timestamp_nanos() {
        let input = "1620000000123456789";
        let result = parse_timestamp(input);
        assert!(result.is_ok());
        let (_, dt) = result.unwrap();
        assert_eq!(dt.timestamp_nanos_opt().unwrap(), 1620000000123456789);
    }

    #[test]
    fn test_parse_timestamp_invalid_length() {
        let input = "16200000001"; // 11位，不是标准长度
        let result = parse_timestamp(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_metric_format() {
        let input = "system.cpu.usage abc 1620000000";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_timestamp() {
        let input = "system.cpu.usage 42.5 timestamp";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_negative_values() {
        let input = "system.temp -10.5 1620000000";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());
        let (_, record) = result.unwrap();

        let value = record.get(&VALUE_FIELD).unwrap();
        if let Value::Float(num) = value {
            assert_eq!(num.value, -10.5);
        } else {
            panic!("Value is not a float");
        }
    }

    #[test]
    fn test_parse_with_special_chars_in_metric_name() {
        let input = "system.cpu-usage.percentage 99.9 1620000000";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());
        let (_, record) = result.unwrap();

        let metric_name = record.get(&NAME_FIELD).unwrap();
        if let Value::String(name) = metric_name {
            assert_eq!(name.as_str(), "system.cpu-usage.percentage");
        } else {
            panic!("Metric name is not a string");
        }
    }

    #[test]
    fn test_parse_with_invalid_attribute_type() {
        // 在配置中标记为整数，但提供字符串值
        let input = "test.metric 42.5 1620000000 int_val=not_a_number";
        let config = create_config_with_attributes();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_with_multiple_attributes() {
        let input =
            "system.load 3.14 1620000000 host=server01 region=us-west datacenter=dc1 rack=r42";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());
        let (_, record) = result.unwrap();

        // 验证所有属性都被解析
        let host = record.get(&Symbol::new("host")).unwrap();
        let region = record.get(&Symbol::new("region")).unwrap();
        let datacenter = record.get(&Symbol::new("datacenter")).unwrap();
        let rack = record.get(&Symbol::new("rack")).unwrap();

        assert!(matches!(host, Value::String(_)));
        assert!(matches!(region, Value::String(_)));
        assert!(matches!(datacenter, Value::String(_)));
        assert!(matches!(rack, Value::String(_)));
    }

    #[test]
    fn test_parse_scientific_notation() {
        let input = "system.memory 1.2e6 1620000000";
        let config = create_test_config();
        let result = parse_graphite_to_record(input, &config);
        assert!(result.is_ok());
        let (_, record) = result.unwrap();

        let value = record.get(&VALUE_FIELD).unwrap();
        if let Value::Float(num) = value {
            assert_eq!(num.value, 1.2e6);
        } else {
            panic!("Value is not a float");
        }
    }
}
