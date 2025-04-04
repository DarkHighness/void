use std::collections::HashMap;

use async_trait::async_trait;
use log::{info, warn};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    config::pipe::timeseries::TimeseriesPipeConfig,
    core::{
        manager::ChannelGraph,
        tag::{HasTag, TagId},
        types::{Record, Symbol, Value},
    },
};

pub struct TimeseriesPipe {
    tag: TagId,

    label_fields: Vec<Symbol>,
    value_fields: Vec<Symbol>,
    timestamp_field: Option<Symbol>,
    extra_labels: HashMap<Symbol, String>,

    rx: broadcast::Receiver<Record>,
    tx: broadcast::Sender<Record>,
}

impl TimeseriesPipe {
    pub fn try_create_from(
        cfg: TimeseriesPipeConfig,
        channels: &ChannelGraph,
    ) -> super::Result<Self> {
        let tag = cfg.tag.into();
        let rx = channels.unsafe_pipe_inbound(&tag);
        let tx = channels.unsafe_pipe_outbound(&tag);

        Ok(TimeseriesPipe {
            tag,
            label_fields: cfg.labels,
            value_fields: cfg.values,
            timestamp_field: cfg.timestamp,
            extra_labels: cfg.extra_labels,
            rx,
            tx,
        })
    }

    fn transform(&self, record: Record) -> super::Result<Vec<Record>> {
        // Transform the given record into a timeseries format.
        let timestamp = match self.timestamp_field {
            Some(ref field) => record
                .get(field)
                .cloned()
                .ok_or(super::Error::InvalidRecord(format!(
                    "Missing timestamp field: {}",
                    field
                )))?,
            None => chrono::Utc::now().into(),
        };

        let (labels, values) = record
            .take()
            .into_iter()
            .partition::<Vec<_>, _>(|(sym, _)| self.label_fields.contains(sym));
        let labels: Value = labels.into();

        let (values, _) = values
            .into_iter()
            .partition::<Vec<_>, _>(|(sym, _)| self.value_fields.contains(sym));

        let mut transformed_records = Vec::new();
        for (name, value) in values {
            let mut new_record = Record::empty();
            new_record.set("name".into(), name.into());
            new_record.set("labels".into(), labels.clone());
            new_record.set("value".into(), value);
            new_record.set("timestamp".into(), timestamp.clone());
            for (key, value) in &self.extra_labels {
                new_record.set(key.clone(), value.clone().into());
            }
            transformed_records.push(new_record);
        }

        if transformed_records.is_empty() {
            warn!("{}: no values found in record", self.tag);
            return Err(super::Error::InvalidRecord("No values found".into()));
        }

        Ok(transformed_records)
    }
}

impl HasTag for TimeseriesPipe {
    fn tag(&self) -> TagId {
        self.tag.clone()
    }
}

#[async_trait]
impl super::Pipe for TimeseriesPipe {
    async fn poll(&mut self, ctx: CancellationToken) -> super::Result<()> {
        tokio::select! {
            _ = ctx.cancelled() => {
                return Ok(());
            }
            record = self.rx.recv() => match record {
                Ok(record) => {
                    let records = self.transform(record)?;
                    for record in records {
                        if let Err(err) = self.tx.send(record) {
                            warn!("{}: error sending record: {:?}", self.tag, err);
                            return Err(err.into());
                        }
                    }
                },
                Err(err) => {
                    warn!("{}: error receiving record: {:?}", self.tag, err);
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }
}
