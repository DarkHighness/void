use reqwest::Client;

use crate::{
    config::outbound::auth::AuthConfig,
    core::{
        pipe::{
            LABELS_FIELD, METRIC_TYPE_FIELD, NAME_FIELD, RECORD_TYPE_TIMESERIES_VALUE,
            TIMESTAMP_FIELD, VALUE_FIELD,
        },
        types::Record,
    },
};
use std::{collections::HashMap, ops::Deref};

/// A label.
///
/// .proto:
/// ```protobuf
/// message Label {
///   string name  = 1;
///   string value = 2;
/// }
/// ```
#[derive(prost::Message, Clone, Hash, PartialEq, Eq)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A sample.
///
/// .proto:
/// ```protobuf
/// message Sample {
///   double value    = 1;
///   int64 timestamp = 2;
/// }
/// ```
#[derive(prost::Message, Clone, PartialEq)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

pub enum ExtraLabel {
    LessThan(f64),
    Quantile(f64),
}

/// A time series.
///
/// .proto:
/// ```protobuf
/// message TimeSeries {
///   repeated Label labels   = 1;
///   repeated Sample samples = 2;
/// }
/// ```
#[derive(prost::Message, Clone, PartialEq)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

impl TimeSeries {
    pub fn sort_labels_and_samples(&mut self) {
        self.sort_labels();
        self.sort_samples();
    }

    pub fn sort_labels(&mut self) {
        self.labels.sort_by(|a, b| a.name.cmp(&b.name));
    }

    pub fn sort_samples(&mut self) {
        self.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }

    pub fn sorted(mut self) -> Self {
        self.sort_labels_and_samples();
        self
    }
}

/// A write request.
///
/// .proto:
/// ```protobuf
/// message WriteRequest {
///   repeated TimeSeries timeseries = 1;
///   // Cortex uses this field to determine the source of the write request.
///   // We reserve it to avoid any compatibility issues.
///   reserved  2;

///   // Prometheus uses this field to send metadata, but this is
///   // omitted from v1 of the spec as it is experimental.
///   reserved  3;
/// }
/// ```
#[derive(prost::Message, Clone, PartialEq)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

impl WriteRequest {
    pub fn sort(&mut self) {
        for ts in &mut self.timeseries {
            ts.sort_labels_and_samples();
        }
    }

    pub fn sorted(mut self) -> Self {
        self.sort();
        self
    }

    /// Encode this write request as a protobuf message.
    ///
    /// NOTE: The API requires snappy compression, not a raw protobuf message.
    pub fn encode_proto3(self) -> Vec<u8> {
        prost::Message::encode_to_vec(&self.sorted())
    }

    pub fn encode_compressed(self) -> Result<Vec<u8>, snap::Error> {
        snap::raw::Encoder::new().compress_vec(&self.encode_proto3())
    }

    pub fn build_request(
        self,
        client: &Client,
        auth: &AuthConfig,
        endpoint: &str,
        useragent: &str,
    ) -> Result<reqwest::Request, super::Error> {
        let url = format!("{}/api/v1/write", endpoint);
        let builder = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .header(reqwest::header::CONTENT_ENCODING, "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .header(reqwest::header::USER_AGENT, useragent);

        let builder = match auth {
            AuthConfig::None => builder,
            AuthConfig::Basic { username, password } => {
                builder.basic_auth(username, Some(password.as_str()))
            }
            AuthConfig::Bearer { token } => builder.bearer_auth(token),
        };

        let request = builder.body(self.encode_compressed()?).build()?;

        Ok(request)
    }
}

impl TryFrom<Record> for TimeSeries {
    type Error = super::Error;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        let r#type = record
            .get_type()
            .ok_or_else(|| super::Error::InvalidRecord("No type found".into()))?;

        if r#type != RECORD_TYPE_TIMESERIES_VALUE.deref() {
            return Err(super::Error::InvalidRecord(format!(
                "Invalid type: {}",
                r#type
            )));
        }

        let name = record
            .get(&NAME_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("No name found".into()))?;

        let name = Label {
            name: "__name__".to_string(),
            value: name.to_string(),
        };

        let metric_type = record
            .get(&METRIC_TYPE_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("No metric type found".into()))?
            .ensure_string()?
            .as_ref();

        let mut labels = record
            .get(&LABELS_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("No labels found".into()))?
            .ensure_map()?
            .iter()
            .map(|(key, value)| {
                let label = Label {
                    name: key.ensure_string()?.as_ref().to_string(),
                    value: value.to_string(),
                };
                Ok::<_, super::Error>(label)
            })
            .collect::<Result<Vec<_>, _>>()?;
        labels.push(name);

        let timestamp = record
            .get(&TIMESTAMP_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("No timestamp found".into()))?
            .ensure_datetime()?
            .timestamp_millis();

        let value = record
            .get(&VALUE_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("No value found".into()))?
            .ensure_float()?
            .as_ref();

        let sample = Sample {
            value: *value,
            timestamp,
        };

        let samples = vec![sample];
        let timeseries = TimeSeries { labels, samples };

        Ok(timeseries)
    }
}

fn combine_timeseries(tss: Vec<TimeSeries>) -> Result<Vec<TimeSeries>, super::Error> {
    if tss.is_empty() {
        return Err(super::Error::InvalidRecord("No timeseries found".into()));
    }

    // Merge by two timeseries have identical labels.
    // This is a naive implementation, but it should be fast enough for our use case.
    let tss = tss
        .into_iter()
        .map(|mut ts| {
            ts.sort_labels();
            ts
        })
        .fold(HashMap::new(), |mut acc, ts| {
            let entry = acc.entry(ts.labels).or_insert_with(Vec::new);
            entry.extend(ts.samples);

            acc
        })
        .into_iter()
        .map(|(labels, samples)| TimeSeries { labels, samples })
        .map(|mut ts| {
            ts.sort_labels_and_samples();
            ts
        })
        .collect::<Vec<_>>();

    Ok(tss)
}

pub fn transform_timeseries(records: Vec<Record>) -> Result<Vec<TimeSeries>, super::Error> {
    let mut tss = Vec::new();
    for record in records {
        let ts = TimeSeries::try_from(record)?;
        tss.push(ts);
    }

    combine_timeseries(tss)
}

impl From<Vec<TimeSeries>> for WriteRequest {
    fn from(tss: Vec<TimeSeries>) -> Self {
        WriteRequest { timeseries: tss }
    }
}
