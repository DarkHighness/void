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

pub struct CSVProtocol<R> {
    reader: R,
    csv_reader: csv_core::Reader,

    has_header: bool,
    header_skipped: bool,

    fields: HashMap<usize, (Symbol, DataType)>,
    num_fields: usize,

    input_buf: BytesMut,
    output_buf: BytesMut,
    end_buf: Vec<usize>,
}

impl<R> CSVProtocol<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    pub fn try_create_from(reader: R, cfg: CSVProtocolConfig) -> super::Result<Self> {
        let fields = cfg
            .fields
            .into_iter()
            .map(|c| (c.index, (c.name, c.r#type)))
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
                self.fields.get(&i).map(|(name, data_type)| {
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
impl<R> super::ProtocolParser for CSVProtocol<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    async fn read_next(&mut self) -> super::Result<Record> {
        self.skip_header().await?;

        loop {
            let bytes_read = self.reader.read_buf(&mut self.input_buf).await?;
            if bytes_read == 0 {
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
