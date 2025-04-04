mod base;
mod csv;
mod error;

pub use base::Protocol;
pub use error::{Error, Result};

use crate::config::ProtocolConfig;

pub fn try_create_from<R>(reader: R, cfg: ProtocolConfig) -> Result<Box<dyn Protocol>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    match cfg {
        ProtocolConfig::CSV(cfg) => Ok(Box::new(csv::CSVProtocol::try_create_from(reader, cfg)?)),
    }
}
