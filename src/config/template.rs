/*
  Templates:
  - `{{cwd}}` - Current working directory
  - `{{home}}` - User's home directory
  - `{{user}}` - User's username
  - `{{group}}` - User's group name
  - `{{date}}` - Current date
  - `{{time}}` - Current time
  - `{{timestamp}}` - Current timestamp
  - `{{uuid}}` - Random UUID
  - `{{random}}` - Random string
  - `{{random:10}}` - Random string of length 10
  - `{{hostname}}` - Hostname
  - `{{env:VAR}}` - Environment variable
*/

use std::{ops::Deref, str::FromStr};

use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Template<T>(T);

impl<T> Template<T> {
    pub fn take(self) -> T {
        self.0
    }
}

impl<T> Deref for Template<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Clone for Template<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Template(self.0.clone())
    }
}

impl<'de, T> Deserialize<'de> for Template<T>
where
    T: FromStr,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let filled = fill(&s);
        let value = filled.ok_or_else(|| serde::de::Error::custom("Failed to fill template"))?;

        let value = T::from_str(&value).map_err(|_| {
            serde::de::Error::custom(format!(
                "Failed to parse value from string: {}:{}",
                &s, value
            ))
        })?;

        Ok(Template(value))
    }
}

impl<T> Serialize for Template<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

fn fill(st: &str) -> Option<String> {
    let mut filled_string = st.to_string();

    // 创建正则表达式来匹配模板标记
    let re = Regex::new(r"\{\{([^{}]+)\}\}").ok()?;

    // 循环查找和替换所有模板标记
    while let Some(caps) = re.captures(&filled_string) {
        let full_match = caps.get(0).unwrap().as_str();
        let template_name = caps.get(1).unwrap().as_str();

        // 根据不同的模板名称进行替换
        let replacement = match template_name {
            "cwd" => std::env::current_dir().ok()?.to_string_lossy().to_string(),
            "home" => std::env::var("HOME").ok()?,
            "user" => std::env::var("USER").ok()?,
            "group" => std::env::var("GROUP").ok()?,
            "date" => chrono::Local::now().format("%Y-%m-%d").to_string(),
            "time" => chrono::Local::now().format("%H:%M:%S").to_string(),
            "timestamp" => chrono::Local::now().timestamp().to_string(),
            "uuid" => uuid::Uuid::new_v4().to_string(),
            "random" => rand::random::<u32>().to_string(),
            "hostname" => hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .ok()?,
            // 处理带参数的模板，如 random:10
            template if template.starts_with("random:") => {
                if let Some(len_str) = template.split(':').nth(1) {
                    if let Ok(len) = len_str.parse::<usize>() {
                        (0..len).map(|_| rand::random::<char>()).collect()
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            // 处理环境变量
            template if template.starts_with("env:") => {
                if let Some(var_name) = template.split(':').nth(1) {
                    std::env::var(var_name).ok()?
                } else {
                    continue;
                }
            }
            // 不支持的模板标记，保持原样
            _ => continue,
        };

        // 替换找到的模板标记
        filled_string = filled_string.replace(full_match, &replacement);
    }

    Some(filled_string)
}
