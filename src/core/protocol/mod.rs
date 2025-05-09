mod base;
// mod csv;
mod csv_nom;
mod error;
mod graphite_nom;

pub use base::ProtocolParser;
pub use error::{Error, Result};

use crate::config::ProtocolConfig;

pub fn try_create_from<R>(reader: R, cfg: ProtocolConfig) -> Result<Box<dyn ProtocolParser>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    match cfg {
        ProtocolConfig::CSV(cfg) => Ok(Box::new(csv_nom::CSVProtocolParser::try_create_from(
            reader, cfg,
        )?)),
        ProtocolConfig::Graphite(cfg) => Ok(Box::new(
            graphite_nom::GraphiteProtocolParser::try_create_from(reader, cfg)?,
        )),
    }
}
