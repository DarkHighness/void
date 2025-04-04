use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use csv_core::ReadRecordResult;
use tokio::io::AsyncReadExt;

use crate::{
    config::protocol::csv::CSVProtocolConfig,
    core::types::{parse_value, DataType, Record, Symbol},
};

pub struct CSVProtocol<R> {
    reader: R,
    csv_reader: csv_core::Reader,

    has_header: bool,
    header_skipped: bool,

    fields: HashMap<usize, (Symbol, DataType)>,
    num_fields: usize,

    input_buf: bytes::BytesMut,
    output_buf: bytes::BytesMut,
    end_buf: Vec<usize>,
}

const BUFFER_SIZE: usize = 8192;

impl<R> CSVProtocol<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    pub fn try_create_from(reader: R, cfg: CSVProtocolConfig) -> super::Result<Self> {
        let fields = cfg
            .fields
            .into_iter()
            .map(|c| {
                let r#type = c.r#type;
                let index = c.index;
                (index, (c.name, r#type))
            })
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
            // end_buf[0] will always be 0 to act as a sentinel
            // end_buf[1..] will be used to store the end positions of each field
            end_buf: vec![0; cfg.num_fields + 1],
        })
    }

    pub fn grow_input_buf(&mut self) {
        self.input_buf.reserve(BUFFER_SIZE);
    }

    pub fn grow_output_buf(&mut self) {
        self.output_buf.reserve(BUFFER_SIZE);
        unsafe {
            // Make the csv-core happy since they use is_empty to determine if the buffer is empty
            self.output_buf.set_len(self.output_buf.capacity());
        }
    }

    pub fn reset_end_buf(&mut self) {
        // Do nothing
    }
}

#[async_trait]
impl<R> super::ProtocolParser for CSVProtocol<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    async fn read_next(&mut self) -> super::Result<Record> {
        if !self.header_skipped {
            loop {
                let c = self.reader.read_u8().await?;
                if c == b'\n' {
                    self.header_skipped = true;
                    break;
                }
            }
        }

        loop {
            let readed = self.reader.read_buf(&mut self.input_buf).await?;
            if readed == 0 {
                return Err(super::Error::EOF);
            }

            let (state, input_pos, output_pos, end_pos) = self.csv_reader.read_record(
                &self.input_buf[..self.input_buf.len()],
                &mut self.output_buf[..],
                &mut self.end_buf[1..],
            );

            match state {
                ReadRecordResult::InputEmpty => {
                    self.grow_input_buf();
                    continue;
                }
                ReadRecordResult::OutputEndsFull => {
                    return Err(super::Error::MismatchedFormat(
                        "The number of readed fields is greater than it defined in the config"
                            .to_string(),
                    ))
                }
                ReadRecordResult::OutputFull => {
                    self.grow_output_buf();
                    continue;
                }
                ReadRecordResult::End => unreachable!(),
                ReadRecordResult::Record => {
                    let record = self.end_buf[0..end_pos + 1]
                        .windows(2)
                        .enumerate()
                        .filter_map(|(i, range)| {
                            if !self.fields.contains_key(&i) {
                                return None;
                            }

                            let start = range[0];
                            let end = range[1];
                            let field = &self.output_buf[start..end];
                            let field = unsafe { std::str::from_utf8_unchecked(field) };
                            let field = field.trim();

                            let (name, r#type) = self.fields.get(&i).unwrap();
                            let field = parse_value(field, r#type)
                                .map_err(|_| {
                                    super::Error::MismatchedFormat(format!(
                                        "Failed to parse field {}: {}",
                                        name, field
                                    ))
                                })
                                .map(|v| (name.clone(), v));

                            Some(field)
                        })
                        .collect::<Result<Record, super::Error>>()?;

                    self.reset_end_buf();
                    let next_input_buf = self.input_buf.split_off(input_pos);
                    self.input_buf = next_input_buf;

                    return Ok(record);
                }
            }
        }
    }
}
