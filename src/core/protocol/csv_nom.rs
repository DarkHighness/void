use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while, take_while1},
    character::complete::{char, line_ending},
    combinator::{eof, map, opt},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, terminated, tuple},
    IResult, Parser,
};
use tokio::io::AsyncReadExt;

use crate::{
    config::protocol::csv::CSVProtocolConfig,
    core::protocol,
    core::types::{parse_value, DataType, Record, Symbol, SymbolMap, Value},
    utils::tracing::TracingContext,
};

const BUFFER_SIZE: usize = 16 * 1024;

pub struct CSVProtocolParser<R> {
    reader: R,
    config: CSVProtocolConfig,

    has_header: bool,
    header_skipped: bool,

    fields: HashMap<usize, (Symbol, DataType, bool)>,

    num_required_fields: usize,
    num_optional_fields: usize,
    num_fields: usize,

    input_buf: BytesMut,
}

impl<R> CSVProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    pub fn try_create_from(reader: R, cfg: CSVProtocolConfig) -> protocol::Result<Self> {
        let fields = cfg
            .fields
            .iter()
            .map(|c| (c.index, (c.name.clone(), c.r#type.clone(), c.optional)))
            .collect::<HashMap<usize, _>>();

        let num_required_fields = fields
            .iter()
            .filter(|(_, (_, _, optional))| !optional)
            .count();

        let num_optional_fields = fields
            .iter()
            .filter(|(_, (_, _, optional))| *optional)
            .count();

        Ok(Self {
            reader,
            config: cfg.clone(),
            has_header: cfg.has_header,
            header_skipped: !cfg.has_header,
            num_required_fields,
            num_optional_fields,
            num_fields: num_optional_fields + num_required_fields,
            fields,
            input_buf: BytesMut::with_capacity(BUFFER_SIZE),
        })
    }

    async fn skip_header(&mut self) -> protocol::Result<()> {
        if self.has_header && !self.header_skipped {
            loop {
                match self.reader.read_u8().await {
                    Ok(0) => {
                        // EOF reached
                        return Err(protocol::Error::EOF);
                    }
                    Ok(c) => {
                        if c == b'\n' {
                            self.header_skipped = true;
                            break;
                        }
                    }
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::UnexpectedEof => {
                            // EOF reached
                            return Err(protocol::Error::EOF);
                        }
                        std::io::ErrorKind::WouldBlock => {
                            // Would block, continue reading
                            continue;
                        }
                        _ => {
                            // Other IO error
                            return Err(protocol::Error::Io(e));
                        }
                    },
                }
            }
        }
        Ok(())
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

    fn parse_record(&self, record: Vec<String>) -> protocol::Result<Record> {
        // 查找必填字段的最大索引
        let max_required_index = self
            .fields
            .iter()
            .filter(|(_, (_, _, optional))| !optional)
            .map(|(index, _)| *index)
            .max()
            .unwrap_or(0);

        // 检查是否有足够的字段覆盖所有必填字段
        if record.len() <= max_required_index {
            return Err(protocol::Error::MismatchedFormat(format!(
                "Too few fields in CSV record. Expected at least {} fields for required fields, got {}",
                max_required_index + 1,
                record.len()
            )));
        }

        // 检查是否有过多的字段
        let max_defined_index = self.fields.keys().max().copied().unwrap_or(0);
        if record.len() > max_defined_index + 1 {
            return Err(protocol::Error::MismatchedFormat(format!(
                "Too many fields in CSV record. Expected at most {} fields, got {}",
                max_defined_index + 1,
                record.len()
            )));
        }

        // 计数所有非空的必填字段
        let mut required_fields_count = 0;
        for (index, val) in record.iter().enumerate() {
            if let Some((_, _, optional)) = self.fields.get(&index) {
                if !optional && !val.is_empty() {
                    required_fields_count += 1;
                }
            }
        }

        if required_fields_count < self.num_required_fields {
            return Err(protocol::Error::MismatchedFormat(format!(
                "Too few non-empty required fields in CSV record. Expected {} required fields, got {}",
                self.num_required_fields, required_fields_count
            )));
        }

        let mut map = SymbolMap::new();

        for (i, field_str) in record.iter().enumerate() {
            if let Some((name, data_type, optional)) = self.fields.get(&i) {
                if field_str.is_empty() {
                    if *optional {
                        continue; // Skip optional empty fields
                    } else {
                        return Err(protocol::Error::MismatchedFormat(format!(
                            "Required field {} cannot be empty",
                            name
                        )));
                    }
                }

                let parsed_value = parse_value(field_str, data_type).map_err(|_| {
                    protocol::Error::MismatchedFormat(format!(
                        "Failed to parse field {}: {}, expected {}",
                        name, field_str, data_type
                    ))
                })?;

                map.insert(name.clone(), parsed_value);
            }
        }

        let record = Record::new_with_values(map, TracingContext::new_root());
        Ok(record)
    }
}

fn parse_csv_line(input: &str, delimiter: char) -> IResult<&str, Vec<String>> {
    // 定义字段解析器
    let field_content = |c| c != delimiter && c != '\n' && c != '\r';
    let quoted_field = map(
        delimited(char('"'), take_while(|c| c != '"'), char('"')),
        |s: &str| s.to_string(),
    );
    let unquoted_field = map(take_while(field_content), |s: &str| s.trim().to_string());

    let field = alt((quoted_field, unquoted_field));

    // 处理字段列表
    let fields = separated_list0(char(delimiter), field);

    // 处理行尾
    let line_end = alt((tag("\r\n"), tag("\n"), eof));

    // 完整行解析
    let (input, result) = terminated(fields, opt(line_end)).parse(input)?;

    Ok((input, result))
}

#[async_trait]
impl<R> protocol::ProtocolParser for CSVProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    async fn read_next(&mut self) -> protocol::Result<Record> {
        self.skip_header().await?;

        loop {
            match self.read_line().await? {
                Some(line) => {
                    if line.trim().is_empty() {
                        return Err(protocol::Error::EOF);
                    }
                    match parse_csv_line(&line, self.config.delimiter) {
                        Ok((_, record)) => {
                            let parsed_record = self.parse_record(record)?;
                            return Ok(parsed_record);
                        }
                        Err(e) => {
                            return Err(protocol::Error::MismatchedFormat(format!(
                                "Failed to parse CSV line: {:?}",
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
    use std::io::Cursor;

    use crate::config::protocol::csv::{CSVField, CSVProtocolConfig};
    use crate::config::Verify;
    use crate::core::protocol::{Error, ProtocolParser};
    use crate::core::tag::{TagId, PROTOCOL_TAG_SCOPE};
    use crate::core::types::intern;
    use crate::core::types::{DataType, Symbol, Value};

    use super::*;

    fn create_test_config() -> CSVProtocolConfig {
        let mut cfg = CSVProtocolConfig {
            tag: TagId::new(PROTOCOL_TAG_SCOPE, "csv").into(),
            delimiter: ',',
            has_header: true,
            num_fields: 3,
            fields: vec![
                CSVField {
                    index: 0,
                    name: Symbol::new("name"),
                    r#type: DataType::String,
                    optional: false,
                },
                CSVField {
                    index: 1,
                    name: Symbol::new("age"),
                    r#type: DataType::Int,
                    optional: false,
                },
                CSVField {
                    index: 2,
                    name: Symbol::new("active"),
                    r#type: DataType::Bool,
                    optional: false,
                },
            ],
        };

        cfg.verify().expect("Invalid config");
        cfg
    }

    #[tokio::test]
    async fn test_basic_csv_parsing() {
        let data = "name,age,active\nAlice,30,true\nBob,25,false\n";
        let reader = Cursor::new(data);

        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        // First record
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("name")).unwrap(),
            &Value::String(intern("Alice"))
        );
        assert_eq!(
            record
                .get(&Symbol::new("age"))
                .unwrap()
                .int()
                .unwrap()
                .value(),
            30
        );
        assert_eq!(
            record
                .get(&Symbol::new("active"))
                .unwrap()
                .bool()
                .unwrap()
                .value(),
            true
        );

        // Second record
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("name")).unwrap(),
            &Value::String(intern("Bob"))
        );
        assert_eq!(
            record
                .get(&Symbol::new("age"))
                .unwrap()
                .int()
                .unwrap()
                .value(),
            25
        );
        assert_eq!(
            record
                .get(&Symbol::new("active"))
                .unwrap()
                .bool()
                .unwrap()
                .value(),
            false
        );

        // No more records
        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EOF));
    }

    #[tokio::test]
    async fn test_no_header() {
        let data = "Alice,30,true\nBob,25,false\n";
        let mut config = create_test_config();
        config.has_header = false;

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, config).unwrap();

        // First record
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("name")).unwrap(),
            &Value::String(intern("Alice"))
        );
    }

    #[tokio::test]
    async fn test_optional_fields() {
        let data = "name,age,active\nAlice,30\nBob\n";
        let mut config = create_test_config();
        config.fields[1].optional = true; // age is optional
        config.fields[2].optional = true; // active is optional

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, config).unwrap();

        // First record (missing active)
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 2);
        assert!(record.contains_key(&Symbol::new("name")));
        assert!(record.contains_key(&Symbol::new("age")));
        assert!(!record.contains_key(&Symbol::new("active")));

        // Second record (missing active and age)
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 1);
        assert!(record.contains_key(&Symbol::new("name")));
        assert!(!record.contains_key(&Symbol::new("age")));
        assert!(!record.contains_key(&Symbol::new("active")));

        // No more records
        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EOF));
    }

    #[tokio::test]
    async fn test_different_delimiter() {
        let data = "name;age;active\nAlice;30;true\nBob;25;false\n";
        let mut config = create_test_config();
        config.delimiter = ';';

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, config).unwrap();

        // First record
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("name")).unwrap(),
            &Value::String(intern("Alice"))
        );
    }

    #[tokio::test]
    async fn test_parse_errors() {
        let data = "name,age,active\nAlice,not_a_number,true\n";
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        // Parsing should fail because "not_a_number" is not an integer
        let result = parser.read_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_large_input() {
        // Generate a large CSV with 1000 rows
        let mut data = String::from("name,age,active\n");
        for i in 0..580 {
            data.push_str(&format!("Person{},{},{}\n", i, i, i % 2 == 0));
        }

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        // Read all records
        let mut count = 0;
        while let Ok(record) = parser.read_next().await {
            assert_eq!(record.len(), 3);
            count += 1;
        }

        assert_eq!(count, 580);
    }

    #[tokio::test]
    async fn test_empty_fields() {
        let data = "name,age,active\n,30,true\nBob,,false\nAlice,25,\n";
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        // First record (empty name)
        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MismatchedFormat(_)));

        // Second record (empty age)
        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MismatchedFormat(_)));

        // Third record (empty active)
        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MismatchedFormat(_)));
    }

    #[tokio::test]
    async fn test_trim_whitespace() {
        let data = "name,age,active\n Alice ,30, true \n";
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("name")).unwrap(),
            &Value::String(intern("Alice"))
        );
        assert_eq!(
            record
                .get(&Symbol::new("active"))
                .unwrap()
                .bool()
                .unwrap()
                .value(),
            true
        );
    }

    #[tokio::test]
    async fn test_different_data_types() {
        let data = "string,int,float,bool,date\ntext,42,3.14,true,2023-01-01\n";

        let mut cfg = CSVProtocolConfig {
            tag: TagId::new(PROTOCOL_TAG_SCOPE, "csv").into(),
            delimiter: ',',
            has_header: true,
            num_fields: 5,
            fields: vec![
                CSVField {
                    index: 0,
                    name: Symbol::new("string"),
                    r#type: DataType::String,
                    optional: false,
                },
                CSVField {
                    index: 1,
                    name: Symbol::new("int"),
                    r#type: DataType::Int,
                    optional: false,
                },
                CSVField {
                    index: 2,
                    name: Symbol::new("float"),
                    r#type: DataType::Float,
                    optional: false,
                },
                CSVField {
                    index: 3,
                    name: Symbol::new("bool"),
                    r#type: DataType::Bool,
                    optional: false,
                },
                CSVField {
                    index: 4,
                    name: Symbol::new("date"),
                    r#type: DataType::String, // Using string for date in this test
                    optional: false,
                },
            ],
        };

        cfg.verify().expect("Invalid config");

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, cfg).unwrap();

        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 5);
        assert_eq!(
            record.get(&Symbol::new("string")).unwrap(),
            &Value::String(intern("text"))
        );
        assert_eq!(
            record
                .get(&Symbol::new("int"))
                .unwrap()
                .int()
                .unwrap()
                .value(),
            42
        );
        assert!(
            (record
                .get(&Symbol::new("float"))
                .unwrap()
                .float()
                .unwrap()
                .value()
                - 3.14)
                .abs()
                < 0.001
        );
        assert_eq!(
            record
                .get(&Symbol::new("bool"))
                .unwrap()
                .bool()
                .unwrap()
                .value(),
            true
        );
    }

    #[tokio::test]
    async fn test_malformed_data() {
        // Test with wrong number of fields
        let data = "name,age,active\nAlice,30\n"; // Missing one field
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        let result = parser.read_next().await;
        assert!(result.is_err());

        // Test with too many fields
        let data = "name,age,active\nAlice,30,true,extra\n"; // Extra field
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        let result = parser.read_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_zero_length_csv() {
        let data = "";
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EOF));
    }

    #[tokio::test]
    async fn test_only_header() {
        let data = "name,age,active\n";
        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, create_test_config()).unwrap();

        let result = parser.read_next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EOF));
    }

    #[tokio::test]
    async fn test_mixed_field_types() {
        // Testing with some fields and without strict typing
        let data = "id,name,score\n1,Alice,100\n2,Bob,ninety\n";

        let mut cfg = CSVProtocolConfig {
            tag: TagId::new(PROTOCOL_TAG_SCOPE, "csv").into(),
            delimiter: ',',
            has_header: true,
            num_fields: 3,
            fields: vec![
                CSVField {
                    index: 0,
                    name: Symbol::new("id"),
                    r#type: DataType::Int,
                    optional: false,
                },
                CSVField {
                    index: 1,
                    name: Symbol::new("name"),
                    r#type: DataType::String,
                    optional: false,
                },
                CSVField {
                    index: 2,
                    name: Symbol::new("score"),
                    r#type: DataType::String,
                    optional: false,
                },
            ],
        };

        cfg.verify().expect("Invalid config");

        let reader = Cursor::new(data);
        let mut parser = CSVProtocolParser::try_create_from(reader, cfg).unwrap();

        // First record should parse correctly
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record
                .get(&Symbol::new("id"))
                .unwrap()
                .int()
                .unwrap()
                .value(),
            1
        );
        assert_eq!(
            record.get(&Symbol::new("score")).unwrap(),
            &Value::String(intern("100"))
        );

        // Second record should also parse because score is a string type
        let record = parser.read_next().await.unwrap();
        assert_eq!(record.len(), 3);
        assert_eq!(
            record.get(&Symbol::new("score")).unwrap(),
            &Value::String(intern("ninety"))
        );
    }
}
