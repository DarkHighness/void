// mod base;
// mod csv;
// mod error;

// pub use base::Pipe;
// pub use error::{Error, Result};

// use crate::config::pipe::PipeConfig;

// pub fn try_create_from_config(cfg: PipeConfig) -> Result<Box<dyn Pipe>> {
//     let parser: Box<dyn Pipe> = match cfg {
//         PipeConfig::CSV(cfg) => {
//             let parser = csv::CSVPipe::try_create_from_config(cfg)?;
//             Box::new(parser)
//         }
//     };

//     Ok(parser)
// }
