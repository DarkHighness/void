use std::collections::HashMap;

use async_trait::async_trait;
use futures::{stream::FuturesUnordered, StreamExt};
use log::warn;
use once_cell::sync::Lazy;
use tokio::sync::broadcast::error::RecvError;
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

    lagged_inbound_index: Option<(usize, u64)>,
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
            lagged_inbound_index: None,
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
            new_record.set("timestamp".into(), timestamp.clone());

            let (unit, value) = match value {
                Value::Float(mut n) => (n.unit.take(), n.value),
                Value::Int(mut n) => (n.unit.take(), n.value as f64),
                _ => return Err(super::Error::InvalidRecord("Value is not a number".into())),
            };

            let value = value.into();
            new_record.set("value".into(), value);

            let mut labels = labels.clone();
            match unit {
                Some(unit) => labels.map_set("unit".into(), unit.into())?,
                None => {}
            };
            new_record.set("labels".into(), labels);

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

        Ok(new_records)
    }

    async fn next_record(&mut self, ctx: CancellationToken) -> super::Result<Record> {
        // Try to receive from the lagged inbound channel first.
        loop {
            if let Some((index, count)) = self.lagged_inbound_index.take() {
                match self.try_receive_from_lagged(index, count).await {
                    Ok(Some(record)) => return Ok(record),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }

            let mut futures = FuturesUnordered::new();
            for (i, rx) in self.inbounds.iter_mut().enumerate() {
                let tag = rx.tag().clone();

                futures.push(async move {
                    match rx.recv().await {
                        Ok(record) => Ok((i, record)),
                        Err(RecvError::Closed) => Err(super::Error::InboundRecvError(format!(
                            "Inbound channel {} closed",
                            tag
                        ))),
                        Err(RecvError::Lagged(n)) => Err(super::Error::ChannelLagged(i, n)),
                    }
                });
            }

            let tag = self.tag.clone();
            if let Some(result) = futures.next().await {
                match result {
                    Ok((_, record)) => return Ok(record),
                    Err(super::Error::ChannelLagged(index, n)) => {
                        warn!("{}: inbound {} lagged {}", &tag, index, n);
                        self.lagged_inbound_index = Some((index, n));

                        // Retry
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                return Err(super::Error::AllInboundsClosed(tag));
            };
        }
    }

    async fn try_receive_from_lagged(
        &mut self,
        index: usize,
        count: u64,
    ) -> super::Result<Option<Record>> {
        let rx = self
            .inbounds
            .get_mut(index)
            .expect("inbound index out of bounds");

        let tag = rx.tag().clone();

        tokio::select! {
            record = rx.recv() => match record {
                Ok(record) => {
                    if count > 0 {
                        self.lagged_inbound_index = Some((index, count - 1));
                    }
                    Ok(Some(record))
                },
                Err(RecvError::Closed) => {
                    Err(super::Error::InboundRecvError(format!("Channel {} closed", tag)))
                },
                Err(RecvError::Lagged(n)) => {
                    warn!("{}: inbound {} lagged additional {}", self.tag, index, n);
                    self.lagged_inbound_index = Some((index, count + n));
                    Ok(None)
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                self.lagged_inbound_index = None;
                Ok(None)
            }
        }
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
        let record = self.next_record(ctx).await;

        let record = match record {
            Ok(record) => record,
            Err(e) => {
                warn!("{}: failed to transform record: {:?}", self.tag, e);
                return Ok(());
            }
        };

        let records = self.transform(record)?;
        for record in records {
            if let Err(e) = self.outbound.send(record) {
                warn!("{}: failed to send record: {:?}", self.tag, e);
            }
        }

        Ok(())
    }
}

impl Pipe for TimeseriesPipe {}
