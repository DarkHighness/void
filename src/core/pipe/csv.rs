// use std::sync::Arc;

// use log::debug;

// use crate::{
//     config::pipe::csv::CSVPipeConfig,
//     core::{
//         tag::{HasTag, TagId},
//         types::{parse_value, DataType, Record},
//     },
// };

// use super::Result;

// #[derive(Debug)]
// pub struct CSVPipe {
//     tag: TagId,

//     delimiter: char,
//     index_name_types: Vec<(usize, Arc<str>, DataType)>,
// }

// impl CSVPipe {
//     pub fn try_create_from_config(cfg: CSVPipeConfig) -> Result<Self> {
//         let index_name_types = cfg
//             .columns
//             .into_iter()
//             .map(|c| {
//                 let name = c.name.into();
//                 let r#type = c.r#type;
//                 let index = c.index;
//                 (index, name, r#type)
//             })
//             .collect::<Vec<_>>();

//         Ok(Self {
//             tag: cfg.tag,
//             delimiter: cfg.delimiter,
//             index_name_types,
//         })
//     }
// }

// impl HasTag for CSVPipe {
//     fn tag(&self) -> TagId {
//         self.tag.clone()
//     }
// }

// impl super::base::Pipe for CSVPipe {
//     fn pipe(&self, data: String) -> super::Result<Vec<Record>> {
//         let mut reader = csv::ReaderBuilder::new()
//             .delimiter(self.delimiter as u8)
//             .has_headers(false)
//             .from_reader(data.as_bytes());

//         let records = reader
//             .records()
//             .collect::<std::result::Result<Vec<csv::StringRecord>, csv::Error>>()?;

//         let parsed_records = records
//             .into_iter()
//             .map(|record| {
//                 let mut map = Record::new();

//                 if self.index_name_types.len() > record.len() {
//                     return Err(super::Error::InvalidRecord(format!(
//                         "Record has fewer fields than expected. Expected: {}, Found: {}",
//                         self.index_name_types.len(),
//                         record.len()
//                     )));
//                 }

//                 if self.index_name_types.len() < record.len() {
//                     debug!(
//                         "Record has more fields than expected. Expected: {}, Found: {}",
//                         self.index_name_types.len(),
//                         record.len()
//                     );
//                 }

//                 for (i, name, r#type) in &self.index_name_types {
//                     if *i >= record.len() {
//                         return Err(super::Error::InvalidRecord(format!(
//                             "Index {} out of bounds for record with length {}",
//                             i,
//                             record.len()
//                         )));
//                     }

//                     let raw_value = &record[*i];
//                     let value = parse_value(raw_value, r#type)?;

//                     map.insert(name.clone(), value);
//                 }

//                 Ok(map)
//             })
//             .collect::<Result<Vec<_>>>()?;

//         Ok(parsed_records)
//     }
// }
