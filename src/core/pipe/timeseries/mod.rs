use std::{cell::RefCell, collections::HashMap};

use async_trait::async_trait;
use dashmap::DashSet;
use futures::{stream::FuturesUnordered, StreamExt};
use log::{info, warn};
use once_cell::sync::Lazy;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::pipe::timeseries::TimeseriesPipeConfig,
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver, TaggedSender},
        tag::{HasTag, TagId},
        types::{Attribute, Record, Symbol, Value},
    },
};

use super::Pipe;

pub struct TimeseriesPipe {
    tag: TagId,

    label_fields: Vec<Symbol>,
    value_fields: Vec<Symbol>,
    timestamp_field: Option<Symbol>,
    extra_labels: HashMap<Symbol, String>,

    inbounds: Vec<TaggedReceiver>,
    outbound: TaggedSender,
}

pub static RECORD_TYPE_TIMESERIES: Lazy<Symbol> = Lazy::new(|| Symbol::from("TimeseriesRecord"));

static RECORD_TYPE_TIMESERIES_VALUE: Lazy<Value> =
    Lazy::new(|| Value::from(RECORD_TYPE_TIMESERIES.as_ref()));

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

        Ok(TimeseriesPipe {
            tag,
            label_fields: cfg.labels,
            value_fields: cfg.values,
            timestamp_field: cfg.timestamp,
            extra_labels: cfg.extra_labels,
            inbounds,
            outbound,
        })
    }

    fn transform(&self, record: Record) -> super::Result<Vec<Record>> {
        // Transform the given record into a timeseries format.
        let timestamp = match self.timestamp_field {
            // If specified, use the timestamp field from the record.
            Some(ref field) => record.get(field).cloned(),
            // Otherwise, use the current time.
            None => Some(chrono::Utc::now().into()),
        };

        let timestamp = match timestamp {
            Some(ts) => ts,
            None => {
                return Err(super::Error::InvalidRecord("No timestamp found".into()));
            }
        };

        let (labels, values) = record
            .take()
            .into_iter()
            .partition::<Vec<_>, _>(|(sym, _)| self.label_fields.contains(sym));
        let labels: Value = labels.into();

        let (values, _) = values
            .into_iter()
            .partition::<Vec<_>, _>(|(sym, _)| self.value_fields.contains(sym));

        if values.is_empty() {
            return Err(super::Error::InvalidRecord("No values found".into()));
        }

        let mut new_records = Vec::new();
        for (name, value) in values {
            let mut new_record = Record::empty();
            new_record.set("name".into(), name.into());
            new_record.set("labels".into(), labels.clone());
            new_record.set("value".into(), value);
            new_record.set("timestamp".into(), timestamp.clone());
            for (key, value) in &self.extra_labels {
                new_record.set(key.clone(), value.clone().into());
            }
            new_record.set_attribute(Attribute::Type, RECORD_TYPE_TIMESERIES_VALUE.clone());
            new_records.push(new_record);
        }

        if new_records.is_empty() {
            warn!("{}: no values found in record", self.tag);
            return Err(super::Error::InvalidRecord("No values found".into()));
        }

        for record in &new_records {
            info!("TimeSeries: {}", record);
        }

        Ok(new_records)
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
        let mut futs = self
            .inbounds
            .iter_mut()
            .map(|rx| Box::pin(rx.recv()))
            .collect::<Vec<_>>();

        loop {
            let (remaining, record) = tokio::select! {
                _ = ctx.cancelled() => return Ok(()),
                (record, _, remaining) = futures::future::select_all(futs.into_iter()) => {
                    match record {
                        Ok(record) => match remaining.len() {
                            0 => (None, record),
                            _ => (Some(remaining), record)
                        },
                        Err(e) => {
                            warn!("{}: error receiving record: {:?}", self.tag, e);
                            return Ok(());
                        }
                    }
                }
            };

            info!("{}: received record: {:?}", self.tag, record);

            match remaining {
                Some(remaining) => futs = remaining,
                None => break,
            }
        }

        Ok(())
    }
}

impl Pipe for TimeseriesPipe {}
