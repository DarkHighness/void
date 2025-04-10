use miette::Diagnostic;
use reqwest::Client;
use thiserror::Error;

use crate::{
    config::outbound::auth::AuthConfig,
    core::{
        pipe::{LABELS_FIELD, METRIC_TYPE_FIELD, NAME_FIELD, TIMESTAMP_FIELD, VALUE_FIELD},
        types::Record,
    },
};
use std::collections::HashMap;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),
    #[error("Empty record")]
    EmptyRecord,
    #[error("Field not found: {0}")]
    FieldNotFound(&'static str),
    #[error("Invalid type: {0}")]
    Type(#[from] crate::core::types::Error),
    #[error("Failed to compress: {0}")]
    Snap(#[from] snap::Error),
}

pub const NAME_FIELD_STR: &str = "name";
pub const TIMESTAMP_FIELD_STR: &str = "timestamp";
pub const TYPE_FIELD_STR: &str = "type";
pub const METRIC_TYPE_FIELD_STR: &str = "metric_type";
pub const LABELS_FIELD_STR: &str = "labels";
pub const VALUE_FIELD_STR: &str = "value";
pub const UNIT_FIELD_STR: &str = "unit";

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
    ) -> Result<reqwest::RequestBuilder, Error> {
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

        let request = builder.body(self.encode_compressed()?);

        Ok(request)
    }
}

impl TryFrom<Record> for TimeSeries {
    type Error = Error;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        let name = record
            .get(&NAME_FIELD)
            .ok_or_else(|| Error::FieldNotFound(NAME_FIELD_STR))?;

        let name = Label {
            name: "__name__".to_string(),
            value: name.to_string(),
        };

        let metric_type = record
            .get(&METRIC_TYPE_FIELD)
            .ok_or_else(|| Error::FieldNotFound(METRIC_TYPE_FIELD_STR))?
            .string()?
            .as_str();

        let mut labels = record
            .get(&LABELS_FIELD)
            .ok_or_else(|| Error::FieldNotFound(LABELS_FIELD_STR))?
            .map()?
            .iter()
            .map(|(key, value)| {
                let label = Label {
                    name: key.string()?.to_string(),
                    value: value.to_string(),
                };
                Ok::<_, Error>(label)
            })
            .collect::<Result<Vec<_>, _>>()?;
        labels.push(name);

        let timestamp = record
            .get(&TIMESTAMP_FIELD)
            .ok_or_else(|| Error::FieldNotFound(TIMESTAMP_FIELD_STR))?
            .datetime()?
            .timestamp_millis();

        let value = record
            .get(&VALUE_FIELD)
            .ok_or_else(|| Error::FieldNotFound(VALUE_FIELD_STR))?
            .float()?
            .value();

        let sample = Sample { value, timestamp };

        let samples = vec![sample];
        let timeseries = TimeSeries { labels, samples };

        Ok(timeseries)
    }
}

fn combine_timeseries(tss: Vec<TimeSeries>) -> Result<Vec<TimeSeries>, Error> {
    if tss.is_empty() {
        return Err(Error::EmptyRecord);
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

pub fn transform_timeseries(records: Vec<Record>) -> Result<Vec<TimeSeries>, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::pipe::RECORD_TYPE_TIMESERIES_VALUE;
    use crate::core::types::Value;

    fn create_test_record() -> Record {
        let mut record = Record::new_root();

        // Add basic fields
        record.set(NAME_FIELD.clone(), Value::from("test_metric"));
        record.set(METRIC_TYPE_FIELD.clone(), Value::from("gauge"));
        record.set(VALUE_FIELD.clone(), Value::from(42.0));
        record.set(TIMESTAMP_FIELD.clone(), Value::from(chrono::Utc::now()));

        // Add labels
        let mut labels = HashMap::new();
        labels.insert(Value::from("env"), Value::from("test"));
        labels.insert(Value::from("host"), Value::from("localhost"));
        record.set(LABELS_FIELD.clone(), Value::from(labels));

        // Set record type
        record.set_type(RECORD_TYPE_TIMESERIES_VALUE.clone());

        record
    }

    #[test]
    fn test_timeseries_try_from_record() {
        let record = create_test_record();

        let result = TimeSeries::try_from(record.clone());
        assert!(result.is_ok());

        let ts = result.unwrap();

        // Verify labels
        assert_eq!(ts.labels.len(), 3); // env, host, __name__

        // Find and verify __name__ label
        let name_label = ts.labels.iter().find(|l| l.name == "__name__").unwrap();
        assert_eq!(name_label.value, "test_metric");

        // Verify samples
        assert_eq!(ts.samples.len(), 1);
        assert_eq!(ts.samples[0].value, 42.0);
    }

    #[test]
    fn test_combine_timeseries() {
        // Create two time series with identical labels
        let mut labels1 = Vec::new();
        labels1.push(Label {
            name: "__name__".to_string(),
            value: "test_metric".to_string(),
        });
        labels1.push(Label {
            name: "env".to_string(),
            value: "test".to_string(),
        });

        let labels2 = labels1.clone();

        let sample1 = Sample {
            value: 42.0,
            timestamp: 1000,
        };
        let sample2 = Sample {
            value: 43.0,
            timestamp: 2000,
        };

        let ts1 = TimeSeries {
            labels: labels1,
            samples: vec![sample1],
        };
        let ts2 = TimeSeries {
            labels: labels2,
            samples: vec![sample2],
        };

        let result = combine_timeseries(vec![ts1, ts2]);
        assert!(result.is_ok());

        let combined = result.unwrap();
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].samples.len(), 2);
        assert_eq!(combined[0].samples[0].value, 42.0);
        assert_eq!(combined[0].samples[1].value, 43.0);
    }

    #[test]
    fn test_transform_timeseries() {
        // Create two records
        let mut record1 = create_test_record();
        record1.set(NAME_FIELD.clone(), Value::from("metric1"));
        record1.set(VALUE_FIELD.clone(), Value::from(10.0));

        let mut record2 = create_test_record();
        record2.set(NAME_FIELD.clone(), Value::from("metric2"));
        record2.set(VALUE_FIELD.clone(), Value::from(20.0));

        let result = transform_timeseries(vec![record1, record2]);
        assert!(result.is_ok());

        let transformed = result.unwrap();
        assert_eq!(transformed.len(), 2);
    }

    #[test]
    fn test_write_request_from_timeseries() {
        let labels = vec![Label {
            name: "__name__".to_string(),
            value: "test_metric".to_string(),
        }];

        let sample = Sample {
            value: 42.0,
            timestamp: 1000,
        };
        let ts = TimeSeries {
            labels,
            samples: vec![sample],
        };

        let write_request: WriteRequest = vec![ts].into();
        assert_eq!(write_request.timeseries.len(), 1);
    }

    #[test]
    fn test_encode_proto3() {
        let labels = vec![Label {
            name: "__name__".to_string(),
            value: "test_metric".to_string(),
        }];

        let sample = Sample {
            value: 42.0,
            timestamp: 1000,
        };
        let ts = TimeSeries {
            labels,
            samples: vec![sample],
        };

        let write_request: WriteRequest = vec![ts].into();
        let encoded = write_request.encode_proto3();

        // Check if encoding is successful (not empty)
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_empty_timeseries() {
        let result = combine_timeseries(Vec::new());
        assert!(matches!(result, Err(Error::EmptyRecord)));
    }
}
