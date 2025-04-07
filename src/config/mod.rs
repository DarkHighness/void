pub mod env;
pub mod error;
pub mod global;
pub mod inbound;
pub mod outbound;
pub mod pipe;
pub mod protocol;

use std::path::PathBuf;

pub use error::{Error, Result};
use global::{GlobalConfig, GLOBAL_CONFIG};
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
    #[serde(default)]
    pub global: GlobalConfig,
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

        GLOBAL_CONFIG
            .set(config.global.clone())
            .expect("Failed to set global config");

        Ok(config)
    }
}

macro_rules! check_empty {
    ($self:ident, $field:ident, $msg:expr) => {
        if $self.$field.is_empty() {
            return Err(Error::InvalidConfig($msg.into()));
        }
    };
}

macro_rules! check_duplicates {
    ($self:ident, $field:ident) => {
        if let Some(duplicates) = find_duplicate_tags(&$self.$field) {
            return Err(Error::DuplicateTags(
                duplicates.into_iter().cloned().collect(),
            ));
        }
    };
}

macro_rules! verify_all {
    ($self:ident, $field:ident) => {
        for item in &mut $self.$field {
            item.verify()?;
        }
    };
}

impl Verify for Config {
    fn verify(&mut self) -> error::Result<()> {
        self.global.verify()?;
        // skip disabled items
        self.inbounds.retain(|cfg| !cfg.disabled());
        self.outbounds.retain(|cfg| !cfg.disabled());
        self.pipes.retain(|cfg| !cfg.disabled());

        check_empty!(self, inbounds, "inbounds is empty");
        check_empty!(self, outbounds, "outbounds is empty");
        check_empty!(self, protocols, "protocols is empty");
        check_empty!(self, pipes, "pipes is empty");

        check_duplicates!(self, inbounds);
        check_duplicates!(self, outbounds);
        check_duplicates!(self, protocols);
        check_duplicates!(self, pipes);

        verify_all!(self, inbounds);
        verify_all!(self, protocols);
        verify_all!(self, pipes);
        verify_all!(self, outbounds);

        Ok(())
    }
}
