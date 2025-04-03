mod base;
mod csv;
mod error;

pub use base::{ParsedRecord, Parser};
pub use error::{Error, Result};

use crate::config::inbound::parser::ParserConfig;

pub fn try_create_from_config(cfg: ParserConfig) -> Result<Box<dyn Parser>> {
    let parser: Box<dyn Parser> = match cfg {
        ParserConfig::CSV(cfg) => {
            let parser = csv::Parser::try_create_from_config(cfg)?;
            Box::new(parser)
        }
    };

    Ok(parser)
}
