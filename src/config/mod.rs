pub mod env;
pub mod error;
pub mod inbound;
pub mod outbound;
pub mod pipe;
pub mod protocol;

use std::path::PathBuf;

pub use error::{Error, Result};
pub use outbound::OutboundConfig;
use pipe::PipeConfig;
pub use protocol::ProtocolConfig;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::BufReader};

use crate::{config::inbound::InboundConfig, core::tag::find_duplicate_tags};

pub trait Verify {
    fn verify(&mut self) -> error::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub inbounds: Vec<InboundConfig>,
    pub outbounds: Vec<OutboundConfig>,
    pub protocols: Vec<ProtocolConfig>,
    pub pipes: Vec<PipeConfig>,
}

impl Config {
    pub fn load_from_file(path: &PathBuf) -> error::Result<Self> {
        if !path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Config file not found: {}", path.display()),
            )));
        }

        if !path.is_file() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Given config file path is not a file: {}", path.display()),
            )));
        }

        let ext = path
            .extension()
            .expect("Invalid extension")
            .to_str()
            .expect("Invalid encoding");

        let mut config: Config = match ext {
            "json" => serde_json::from_reader(BufReader::new(File::open(path)?))?,
            "toml" => {
                let text = std::fs::read_to_string(path)?;
                toml::de::from_str(&text)?
            }
            _ => return Err(Error::InvalidConfigFileFormat(ext.to_string())),
        };

        config.verify()?;

        Ok(config)
    }
}

impl Verify for Config {
    fn verify(&mut self) -> error::Result<()> {
        if self.inbounds.is_empty() {
            return Err(Error::InvalidConfig("inbounds is empty".into()));
        }

        if self.outbounds.is_empty() {
            return Err(Error::InvalidConfig("outbounds is empty".into()));
        }

        if self.protocols.is_empty() {
            return Err(Error::InvalidConfig("protocols is empty".into()));
        }

        if self.pipes.is_empty() {
            return Err(Error::InvalidConfig("pipes is empty".into()));
        }

        if let Some(duplicates) = find_duplicate_tags(&self.inbounds) {
            return Err(Error::DuplicateTags(duplicates));
        }

        if let Some(duplicates) = find_duplicate_tags(&self.outbounds) {
            return Err(Error::DuplicateTags(duplicates));
        }

        if let Some(duplicates) = find_duplicate_tags(&self.protocols) {
            return Err(Error::DuplicateTags(duplicates));
        }

        if let Some(duplicates) = find_duplicate_tags(&self.pipes) {
            return Err(Error::DuplicateTags(duplicates));
        }

        for inbound in &mut self.inbounds {
            inbound.verify()?;
        }

        for protocol in &mut self.protocols {
            protocol.verify()?;
        }

        for pipe in &mut self.pipes {
            pipe.verify()?;
        }

        for outbound in &mut self.outbounds {
            outbound.verify()?;
        }

        Ok(())
    }
}
