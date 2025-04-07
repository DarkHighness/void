pub mod annotate;

pub use annotate::TimeseriesAnnotatePipe;
use tokio::task::JoinHandle;

pub use super::{Error, Result};
use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use log::{debug, warn};
use once_cell::sync::Lazy;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{
        global::use_serial_mode,
        outbound,
        pipe::timeseries::{MetricType, TimeseriesPipeConfig},
    },
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver, TaggedSender},
        tag::{HasTag, TagId},
        types::{Attribute, Record, Symbol, Value},
    },
    utils::recv::recv_batch,
};

use super::Pipe;

#[derive(Debug)]
struct InnerState {
    tag: TagId,
    label_syms: Vec<Symbol>,
    value_syms: Option<HashMap<Symbol, MetricType>>,
    timestamp_sym: Option<Symbol>,
    extra_labels: HashMap<Symbol, String>,
    outbound: TaggedSender,
}

impl InnerState {
    fn new(
        tag: TagId,
        label_syms: Vec<Symbol>,
        value_syms: Option<HashMap<Symbol, MetricType>>,
        timestamp_sym: Option<Symbol>,
        extra_labels: HashMap<Symbol, String>,
        outbound: TaggedSender,
    ) -> Self {
        InnerState {
            tag,
            label_syms,
            value_syms,
            timestamp_sym,
            extra_labels,
            outbound,
        }
    }

    fn transform(&self, record: Record) -> super::Result<Vec<Record>> {
        let inbound = record
            .get_attribute(&Attribute::Inbound)
            .cloned()
            .unwrap_or_else(|| (&self.tag).into());

        // Transform the given record into a timeseries format.
        let timestamp = match self.timestamp_sym {
            // If specified, use the timestamp field from the record.
            Some(ref field) => record.get(field).cloned(),
            // Otherwise, use the current time.
            None => Some(chrono::Utc::now().into()),
        };

        let timestamp = match timestamp {
            Some(ts) => ts,
            None => {
                return Err(super::Error::FieldNotFound(TIMESTAMP_FIELD_STR));
            }
        };

        let (labels, values) = record
            .take()
            .into_iter()
            .partition::<Vec<_>, _>(|(sym, _)| self.label_syms.contains(sym));
        let labels: Value = labels
            .into_iter()
            .map(|(sym, value)| {
                let key = ensure_valid_label(sym.as_ref())?;
                Ok((Symbol::from(key), value))
            })
            .collect::<super::Result<_>>()?;

        let (values, _) =
            values
                .into_iter()
                .partition::<Vec<_>, _>(|(sym, _)| match self.value_syms {
                    Some(ref syms) => syms.contains_key(sym),
                    None => {
                        let in_labels = self.label_syms.contains(sym);
                        let in_timestamp =
                            self.timestamp_sym.as_ref().map_or(false, |ts| ts == sym);
                        !in_labels && !in_timestamp
                    }
                });

        if values.is_empty() {
            return Err(super::Error::FieldNotFound(VALUE_FIELD_STR));
        }

        let mut new_records = Vec::new();
        for (name, value) in values {
            let mut new_record = Record::empty();

            let metric_type = match self.value_syms {
                Some(ref syms) => {
                    if let Some(typ) = syms.get(&name) {
                        typ.clone()
                    } else {
                        return Err(super::Error::InvalidRecord(format!(
                            "Value {} not found in value syms",
                            name
                        )));
                    }
                }
                None => MetricType::default(),
            };
            let name = ensure_valid_name(name.as_ref())?;

            new_record.set(NAME_FIELD.clone(), name.into());
            new_record.set(METRIC_TYPE_FIELD.clone(), Value::String(metric_type.into()));
            new_record.set(TIMESTAMP_FIELD.clone(), timestamp.clone());

            let value = value.cast_float()?;
            let unit = value.ensure_float()?.unit.clone();
            new_record.set(VALUE_FIELD.clone(), value);

            let mut labels = labels.clone();
            match unit {
                Some(unit) => labels.map_set(UNIT_FIELD.clone().into(), unit.into())?,
                None => {}
            };

            for (key, value) in &self.extra_labels {
                labels.map_set(key.into(), value.as_str().into())?;
            }
            new_record.set(LABELS_FIELD.clone(), labels);

            new_record.set_attribute(Attribute::Type, RECORD_TYPE_TIMESERIES_VALUE.clone());
            new_record.set_attribute(Attribute::Inbound, inbound.clone());

            new_records.push(new_record);
        }

        if new_records.is_empty() {
            warn!("{}: no values found in record", self.tag);
            return Err(super::Error::FieldNotFound(VALUE_FIELD_STR));
        }

        Ok(new_records)
    }
}

pub struct TimeseriesPipe {
    tag: TagId,

    inner: Arc<InnerState>,

    inbounds: Vec<TaggedReceiver>,
    outbound: TaggedSender,
    last_handle: Option<JoinHandle<()>>,

    interval: Duration,
    buffer_size: usize,
}

pub static RECORD_TYPE_TIMESERIES: Lazy<Symbol> = Lazy::new(|| Symbol::intern("TimeseriesRecord"));

pub static RECORD_TYPE_TIMESERIES_VALUE: Lazy<Value> =
    Lazy::new(|| Value::from(RECORD_TYPE_TIMESERIES.as_ref()));

pub const NAME_FIELD_STR: &str = "name";
pub const TIMESTAMP_FIELD_STR: &str = "timestamp";
pub const METRIC_TYPE_FIELD_STR: &str = "metric_type";
pub const LABELS_FIELD_STR: &str = "labels";
pub const VALUE_FIELD_STR: &str = "value";
pub const UNIT_FIELD_STR: &str = "unit";

pub static NAME_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(NAME_FIELD_STR));
pub static TIMESTAMP_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(TIMESTAMP_FIELD_STR));
pub static METRIC_TYPE_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(METRIC_TYPE_FIELD_STR));
pub static LABELS_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(LABELS_FIELD_STR));
pub static VALUE_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(VALUE_FIELD_STR));
pub static UNIT_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(UNIT_FIELD_STR));

impl TimeseriesPipe {
    pub fn try_create_from(
        cfg: TimeseriesPipeConfig,
        channels: &mut ChannelGraph,
    ) -> super::Result<Self> {
        let tag = cfg.tag.into();
        let inbounds = cfg
            .inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &tag))
            .collect::<Vec<_>>();
        let outbound = channels.sender(&tag);

        let value_syms = if let Some(values) = cfg.values {
            let syms = values
                .into_iter()
                .map(|field| (field.name, field.r#type))
                .collect::<HashMap<_, _>>();

            Some(syms)
        } else {
            // If the values field is not set, all the fields except the labels will be treated as values.
            None
        };

        let label_syms = cfg
            .labels
            .into_iter()
            .map(|label| label.clone())
            .collect::<Vec<_>>();

        let inner = InnerState::new(
            tag.clone(),
            label_syms,
            value_syms,
            cfg.timestamp,
            cfg.extra_labels,
            outbound.clone(),
        );
        let inner = Arc::new(inner);

        Ok(TimeseriesPipe {
            tag,
            inner,
            inbounds,
            outbound,
            last_handle: None,
            interval: cfg.interval,
            buffer_size: cfg.buffer_size,
        })
    }

    fn transform_records(&self, record: Vec<Record>) -> super::Result<JoinHandle<()>> {
        let inner = self.inner.clone();

        let handle = tokio::task::Builder::new()
            .name(&format!("{}-transform", self.tag))
            .spawn(async move {
                record
                    .into_iter()
                    .map(|r| inner.transform(r))
                    .filter_map(|r| match r {
                        Ok(records) => Some(records),
                        Err(e) => {
                            warn!("{}: error transforming record: {}", inner.tag, e);
                            None
                        }
                    })
                    .flatten()
                    .for_each(|record| {
                        if let Err(e) = inner.outbound.send(record) {
                            warn!("{}: error sending record: {}", inner.tag, e);
                        }
                    });
            })?;

        Ok(handle)
    }
}

impl HasTag for TimeseriesPipe {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for TimeseriesPipe {
    type Error = super::Error;
    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()> {
        let tag = self.tag.clone();

        let records = match recv_batch(
            &tag,
            &mut self.inbounds,
            Some(self.interval),
            self.buffer_size,
            ctx,
        )
        .await
        {
            Ok(records) => records,
            Err(crate::utils::recv::Error::Timeout) => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        debug!("{}: received {} records", self.tag, records.len());

        match use_serial_mode() {
            true => {
                let handle = self.last_handle.take();
                if let Some(handle) = handle {
                    if let Err(e) = handle.await {
                        warn!("{}: error waiting for handle: {}", self.tag, e);
                    }
                }

                let this_handle = self.transform_records(records)?;
                self.last_handle = Some(this_handle);
            }
            false => {
                let _ = self.transform_records(records);
            }
        }

        Ok(())
    }
}

impl Pipe for TimeseriesPipe {}

// Make sure the label matches [a-zA-Z_]([a-zA-Z0-9_])*
pub fn ensure_valid_label(label: &str) -> super::Result<String> {
    let label = label.trim();
    // transform `.` to `_`
    let label = label.replace('.', "_");
    if label.is_empty() {
        return Err(super::Error::InvalidRecord("Label is empty".into()));
    }

    if !label
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(super::Error::InvalidRecord(format!(
            "Label {} contains invalid characters",
            label
        )));
    }

    if label.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(super::Error::InvalidRecord(format!(
            "Label {} starts with a digit",
            label
        )));
    }

    if label.contains("__") {
        return Err(super::Error::InvalidRecord(format!(
            "Label {} contains double underscore",
            label
        )));
    }

    Ok(label)
}

// Make sure the name matches [a-zA-Z_:]([a-zA-Z0-9_:])*
pub fn ensure_valid_name(name: &str) -> super::Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(super::Error::InvalidRecord("Name is empty".into()));
    }

    // transform `.` to `_`
    let name = name.replace('.', "_");

    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':' || c == '-')
    {
        return Err(super::Error::InvalidRecord(format!(
            "Name {} contains invalid characters",
            name
        )));
    }

    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(super::Error::InvalidRecord(format!(
            "Name {} starts with a digit",
            name
        )));
    }

    if name.contains("__") {
        return Err(super::Error::InvalidRecord(format!(
            "Name {} contains double underscore",
            name
        )));
    }

    Ok(name)
}
