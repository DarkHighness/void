/*
An EnvString can be used to represent a string that is read from an environment variable when deserializing.
Format:
    - If the string starts with "env:", it will be treated as an environment variable.
    - If the string starts with "file:", it will be treated as a file path.
    - The default value can be placed in the end of the string, separated by a ":".
    - Otherwise, it will be treated as a normal string.
If no environment variable or file is found, the default value will be used.
*/

use std::{ops::Deref, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
enum EnvStringType {
    Env,
    File,
    String,
}

#[derive(Debug)]
pub struct Env<T> {
    r#type: EnvStringType,
    key: String,
    value: T,
}

impl<T> Env<T> {
    pub fn is_env(&self) -> bool {
        matches!(self.r#type, EnvStringType::Env)
    }

    pub fn is_file(&self) -> bool {
        matches!(self.r#type, EnvStringType::File)
    }

    pub fn is_string(&self) -> bool {
        matches!(self.r#type, EnvStringType::String)
    }

    pub fn take(self) -> T {
        self.value
    }

    pub fn get(&self) -> &T {
        &self.value
    }
}

impl<T> Deref for Env<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Serialize for Env<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let key = match self.r#type {
            EnvStringType::Env => format!("env:{}", self.key),
            EnvStringType::File => format!("file:{}", self.key),
            EnvStringType::String => self.key.clone(),
        };

        serializer.serialize_str(&key)
    }
}

impl<'de, T> Deserialize<'de> for Env<T>
where
    T: FromStr,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let (key, r#type) = if s.starts_with("env:") {
            (s[4..].to_string(), EnvStringType::Env)
        } else if s.starts_with("file:") {
            (s[5..].to_string(), EnvStringType::File)
        } else {
            (s.clone(), EnvStringType::String)
        };

        let value = match r#type {
            EnvStringType::Env => match std::env::var(&key) {
                Ok(v) => v,
                Err(_) => {
                    return Err(serde::de::Error::custom(format!(
                        "Environment variable not found: {}",
                        key
                    )))
                }
            },
            EnvStringType::File => match std::fs::read_to_string(&key) {
                Ok(v) => v,
                Err(_) => {
                    return Err(serde::de::Error::custom(format!(
                        "File not found or cannot be read: {}",
                        key
                    )))
                }
            },
            EnvStringType::String => key.clone(),
        };

        let value = T::from_str(&value).map_err(|_| {
            serde::de::Error::custom(format!(
                "Failed to parse value from string: {}:{}",
                &key, value
            ))
        })?;

        Ok(Env { r#type, key, value })
    }
}

impl<T> std::fmt::Display for Env<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.r#type {
            EnvStringType::Env => write!(f, "env:{}", self.key),
            EnvStringType::File => write!(f, "file:{}", self.key),
            EnvStringType::String => write!(f, "{}", self.key),
        }
    }
}
