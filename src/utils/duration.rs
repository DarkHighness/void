use std::time::Duration;

use serde::Deserialize;

pub fn parse_duration<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match go_parse_duration::parse_duration(&s) {
        Ok(duration) => Ok(Duration::from_nanos(duration as u64)),
        Err(_) => Err(serde::de::Error::custom(format!(
            "failed to parse duration: {}",
            s
        ))),
    }
}
