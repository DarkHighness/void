use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use csv_core::ReadRecordResult;
use tokio::io::AsyncReadExt;

use crate::{
    config::protocol::csv::CSVProtocolConfig,
    core::types::{parse_value, DataType, Record, Symbol},
};

const BUFFER_SIZE: usize = 8192;

pub struct CSVProtocolParser<R> {
    reader: R,
    csv_reader: csv_core::Reader,

    has_header: bool,
    header_skipped: bool,

    fields: HashMap<usize, (Symbol, DataType, bool)>,
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

        Ok(Self {
            reader,
            csv_reader,
            has_header: cfg.has_header,
            header_skipped: !cfg.has_header,
            num_fields: cfg.num_fields,
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
                let c = self.reader.read_u8().await?;
                if c == b'\n' {
                    self.header_skipped = true;
                    break;
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
        ends[0..end_pos + 1]
            .windows(2)
            .enumerate()
            .filter_map(|(i, range)| {
                // Optional fields will be skipped if (end_pos + 1) is less than the field index
                self.fields.get(&i).map(|(name, data_type, _)| {
                    let start = range[0];
                    let end = range[1];
                    let field = &self.output_buf[start..end];
                    let field_str = unsafe { std::str::from_utf8_unchecked(field).trim() };

                    parse_value(field_str, data_type)
                        .map_err(|_| {
                            super::Error::MismatchedFormat(format!(
                                "Failed to parse field {}: {}, expected {}",
                                name, field_str, data_type
                            ))
                        })
                        .map(|v| (name.clone(), v))
                })
            })
            .collect::<Result<Record, super::Error>>()
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
            let bytes_read = self.reader.read_buf(&mut self.input_buf).await?;
            if bytes_read == 0 && self.input_buf.is_empty() {
                return Err(super::Error::EOF);
            }

            let (state, input_pos, _, end_pos) = self.csv_reader.read_record(
                &self.input_buf,
                &mut self.output_buf,
                &mut self.end_buf[1..],
            );

            match state {
                ReadRecordResult::InputEmpty => {
                    self.ensure_input_capacity();
                    continue;
                }
                ReadRecordResult::OutputFull => {
                    self.ensure_output_capacity();
                    continue;
                }
                ReadRecordResult::OutputEndsFull => {
                    return Err(super::Error::MismatchedFormat(
                        "Too many fields in CSV record".to_string(),
                    ))
                }
                ReadRecordResult::Record => {
                    let record = self.parse_record(&self.end_buf, end_pos)?;
                    self.input_buf = self.input_buf.split_off(input_pos);

                    return Ok(record);
                }
                ReadRecordResult::End => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::config::protocol::csv::{CSVField, CSVProtocolConfig};
    use crate::config::Verify;
    use crate::core::protocol::ProtocolParser;
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
}
