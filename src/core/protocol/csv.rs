use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use csv_core::ReadRecordResult;
use tokio::io::AsyncReadExt;

use crate::{
    config::protocol::csv::CSVProtocolConfig,
    core::types::{parse_value, DataType, Record, Symbol, SymbolMap, Value},
    utils::tracing::TracingContext,
};

const BUFFER_SIZE: usize = 16 * 1024;

pub struct CSVProtocolParser<R> {
    reader: R,
    csv_reader: csv_core::Reader,

    has_header: bool,
    header_skipped: bool,

    fields: HashMap<usize, (Symbol, DataType, bool)>,

    num_required_fields: usize,
    num_optional_fields: usize,
    num_fields: usize,

    input_buf: BytesMut,
    output_buf: BytesMut,
    end_buf: Vec<usize>,
}

impl<R> CSVProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    pub fn try_create_from(reader: R, cfg: CSVProtocolConfig) -> super::Result<Self> {
        let fields = cfg
            .fields
            .into_iter()
            .map(|c| (c.index, (c.name, c.r#type, c.optional)))
            .collect::<HashMap<usize, _>>();

        let csv_reader = csv_core::ReaderBuilder::new()
            .delimiter(cfg.delimiter as u8)
            .build();

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
            csv_reader,
            has_header: cfg.has_header,
            header_skipped: !cfg.has_header,
            num_required_fields,
            num_optional_fields,
            num_fields: num_optional_fields + num_required_fields,
            fields,
            input_buf: BytesMut::with_capacity(BUFFER_SIZE),
            output_buf: BytesMut::zeroed(BUFFER_SIZE),
            // end_buf[0] is a sentinel
            // end_buf[1..] is the end positions of each field
            end_buf: vec![0; cfg.num_fields + 1],
        })
    }

    async fn skip_header(&mut self) -> super::Result<()> {
        if self.has_header && !self.header_skipped {
            loop {
                match self.reader.read_u8().await {
                    Ok(0) => {
                        // EOF reached
                        return Err(super::Error::EOF);
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
                            return Err(super::Error::EOF);
                        }
                        std::io::ErrorKind::WouldBlock => {
                            // Would block, continue reading
                            continue;
                        }
                        _ => {
                            // Other IO error
                            return Err(super::Error::Io(e));
                        }
                    },
                }
            }
        }
        Ok(())
    }

    fn ensure_input_capacity(&mut self) {
        self.input_buf.reserve(BUFFER_SIZE);
    }

    fn ensure_output_capacity(&mut self) {
        self.output_buf.reserve(BUFFER_SIZE);
        unsafe {
            // make csv-core happy since they use is_empty to check if the buffer is empty
            self.output_buf.set_len(self.output_buf.capacity());
        }
    }

    fn parse_record(&self, ends: &[usize], end_pos: usize) -> super::Result<Record> {
        // Only check if we have enough fields if end_pos is smaller than expected
        // This is to handle the case where optional fields are at the end
        let max_field_index = self
            .fields
            .keys()
            .max()
            .copied()
            .unwrap_or(self.num_required_fields.saturating_sub(1));
        if end_pos < max_field_index {
            // Check if all missing fields are optional
            for i in end_pos + 1..=max_field_index {
                if let Some((name, _, optional)) = self.fields.get(&i) {
                    if !*optional {
                        return Err(super::Error::MismatchedFormat(format!(
                            "Too few fields in CSV record. Expected at least {}, got {}",
                            i + 1,
                            end_pos + 1
                        )));
                    }
                } else {
                    // If the field is not defined in the config, it's an error
                    return Err(super::Error::MismatchedFormat(format!(
                        "Field index {} is not defined in the config",
                        i
                    )));
                }
            }
        }

        let map = ends[0..end_pos + 1]
            .windows(2)
            .enumerate()
            .filter_map(|(i, range)| {
                // Optional fields will be skipped if empty
                self.fields.get(&i).map(|(name, data_type, optional)| {
                    let start = range[0];
                    let end = range[1];
                    let field = &self.output_buf[start..end];
                    let field_str = unsafe { std::str::from_utf8_unchecked(field).trim() };

                    // Return early for empty fields
                    if field_str.is_empty() {
                        if *optional {
                            return Ok(None);
                        } else {
                            // Return error for empty required fields
                            return Err(super::Error::MismatchedFormat(format!(
                                "Required field {} cannot be empty",
                                name
                            )));
                        }
                    }

                    let parsed_value = parse_value(field_str, data_type).map_err(|_| {
                        super::Error::MismatchedFormat(format!(
                            "Failed to parse field {}: {}, expected {}",
                            name, field_str, data_type
                        ))
                    })?;

                    Ok(Some((name.clone(), parsed_value)))
                })
            })
            .filter_map(|r| match r {
                Ok(Some(pair)) => Some(Ok(pair)),
                Ok(None) => None, // Skip optional empty fields
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<SymbolMap, super::Error>>()?;

        if map.len() < self.num_required_fields {
            return Err(super::Error::MismatchedFormat(format!(
                "Too few fields in CSV record. Expected at least {}, got {}",
                self.num_required_fields,
                map.len()
            )));
        }

        let record = Record::new_with_values(map, TracingContext::new_root());
        Ok(record)
    }
}

#[async_trait]
impl<R> super::ProtocolParser for CSVProtocolParser<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    async fn read_next(&mut self) -> super::Result<Record> {
        self.skip_header().await?;

        loop {
            // Try to read more data if buffer is empty or we need more
            if self.input_buf.is_empty() || self.input_buf.len() < BUFFER_SIZE / 2 {
                match self.reader.read_buf(&mut self.input_buf).await {
                    Ok(0) => {
                        // Handle EOF condition
                        if self.input_buf.is_empty() {
                            return Err(super::Error::EOF);
                        }
                        // If we have some data left, continue processing it
                    }
                    Ok(_) => {
                        // Successfully read more data
                    }
                    Err(e) => return Err(e)?,
                }
            }

            let (state, input_pos, _, end_pos) = self.csv_reader.read_record(
                &self.input_buf,
                &mut self.output_buf,
                &mut self.end_buf[1..],
            );

            match state {
                ReadRecordResult::InputEmpty => {
                    if self.input_buf.is_empty() {
                        // We've processed all input and need more, but there might not be more
                        match self.reader.read_buf(&mut self.input_buf).await {
                            Ok(0) => {
                                // No more data to read
                                return Err(super::Error::EOF);
                            }
                            Ok(_) => {
                                // Got more data, continue processing
                                self.ensure_input_capacity();
                            }
                            Err(e) => return Err(e)?,
                        }
                    } else {
                        self.ensure_input_capacity();
                    }
                    continue;
                }
                ReadRecordResult::OutputFull => {
                    self.ensure_output_capacity();
                    continue;
                }
                ReadRecordResult::OutputEndsFull => {
                    // We can't store more end positions, this implies too many fields
                    return Err(super::Error::MismatchedFormat(
                        "Too many fields in CSV record".to_string(),
                    ));
                }
                ReadRecordResult::Record => {
                    let record = self.parse_record(&self.end_buf, end_pos)?;
                    self.input_buf.advance(input_pos);
                    return Ok(record);
                }
                ReadRecordResult::End => return Err(super::Error::EOF),
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
        assert!(parser.read_next().await.is_err());
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
        for i in 0..1000 {
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

        assert_eq!(count, 1000);
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
        // The parser should now properly detect too many fields
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MismatchedFormat(_)));
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
        assert_eq!(
            record.get(&Symbol::new("score")).unwrap(),
            &Value::String(intern("ninety"))
        );
    }
}
