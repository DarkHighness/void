use std::{collections::HashMap, ops::Deref};

use async_trait::async_trait;
use log::{error, info};
use once_cell::sync::Lazy;

use crate::{
    config::pipe::timeseries::TimeseriesActionPipeConfig,
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver, TaggedSender},
        pipe::Pipe,
        tag::{HasTag, TagId},
        types::{Record, Symbol, Value},
    },
    utils::recv::{recv, recv_batch},
};

#[derive(Debug)]
pub struct TimeseriesActionPipe {
    tag: TagId,

    data_inbounds: Vec<TaggedReceiver>,
    control_inbounds: Vec<TaggedReceiver>,
    outbound: TaggedSender,

    labels_to_add: HashMap<Symbol, Value>,
    labels_to_remove: Vec<Symbol>,
}

pub static ACTION_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern("action"));
pub static NAME_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern("name"));
pub static VALUE_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern("value"));

pub const ACTION_SET: &'static str = "set";
pub const ACTION_UNSET: &'static str = "unset";
pub const ACTION_DELETE: &'static str = "delete";
pub const ACTION_UNDELETE: &'static str = "undelete";
pub const ACTION_CLEAR: &'static str = "clear";

impl TimeseriesActionPipe {
    pub fn try_create_from(
        cfg: TimeseriesActionPipeConfig,
        channels: &mut ChannelGraph,
    ) -> super::Result<Self> {
        let data_inbounds = cfg
            .data_inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &cfg.tag))
            .collect::<Vec<_>>();
        let control_inbounds = cfg
            .control_inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &cfg.tag))
            .collect::<Vec<_>>();
        let outbound = channels.sender(&cfg.tag);

        let pipe = TimeseriesActionPipe {
            tag: cfg.tag.into(),
            data_inbounds,
            control_inbounds,
            outbound,
            labels_to_add: HashMap::new(),
            labels_to_remove: Vec::new(),
        };

        Ok(pipe)
    }

    fn handle_action_record(&mut self, record: Record) -> super::Result<()> {
        let action = record
            .get(ACTION_FIELD.deref())
            .ok_or_else(|| super::Error::InvalidRecord("Missing action field".to_string()))?;

        let action = action.ensure_string()?;
        let action = action.as_ref();

        match action {
            ACTION_SET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::InvalidRecord("Missing name field".to_string()))?;

                let name = name.ensure_string()?.clone();

                let value = record.get(VALUE_FIELD.deref()).ok_or_else(|| {
                    super::Error::InvalidRecord("Missing value field".to_string())
                })?;

                let value = value.cast_string()?;

                info!(
                    "Timeseries {} will set label: {} = {}",
                    self.tag(),
                    name,
                    value
                );

                self.labels_to_add.insert(name, value);
            }
            ACTION_UNSET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::InvalidRecord("Missing name field".to_string()))?;

                let name = name.ensure_string()?.clone();

                info!(
                    "Timeseries {} will no longer set label: {}",
                    self.tag(),
                    name
                );

                self.labels_to_remove.push(name);
            }
            ACTION_DELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::InvalidRecord("Missing name field".to_string()))?;

                let name = name.ensure_string()?.clone();

                info!("Timeseries {} will delete label: {}", self.tag(), name);

                self.labels_to_remove.push(name);
            }
            ACTION_UNDELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::InvalidRecord("Missing name field".to_string()))?;

                let name = name.ensure_string()?.clone();

                info!("Timeseries {} will undelete label: {}", self.tag(), name);

                self.labels_to_remove.retain(|label| label != &name);
            }
            ACTION_CLEAR => {
                info!("Timeseries {} will clear all actions", self.tag());

                self.labels_to_add.clear();
                self.labels_to_remove.clear();
            }
            _ => {
                return Err(super::Error::InvalidRecord(format!(
                    "Invalid action: {}",
                    action
                )));
            }
        }

        Ok(())
    }

    fn transform(&self, record: &mut Record) -> super::Result<()> {
        let labels = record
            .get_mut(&super::LABELS_FIELD)
            .ok_or_else(|| super::Error::InvalidRecord("Missing labels field".to_string()))?;

        for (label, value) in &self.labels_to_add {
            labels.map_set(label.clone().into(), value.clone())?;
        }

        for label in &self.labels_to_remove {
            let label: Value = label.clone().into();
            labels.map_remove(&label)?;
        }

        Ok(())
    }
}

impl HasTag for TimeseriesActionPipe {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for TimeseriesActionPipe {
    type Error = super::Error;

    async fn poll(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), super::Error> {
        let tag = self.tag().clone();

        tokio::select! {
            _ = ctx.cancelled() => return Ok(()),
            records = recv_batch(&tag, &mut self.data_inbounds, std::time::Duration::from_millis(500), 4096, ctx.clone()) => {
                match records {
                    Ok(records) => {
                        for mut record in records {
                            if let Err(e) = self.transform(&mut record) {
                                error!("{}: failed to transform record: {:?}", self.tag, e);
                            }

                            if let Err(e) = self.outbound.send(record) {
                                error!("{}: failed to send record: {:?}", self.tag, e);
                            }
                        }
                    }
                    Err(crate::utils::recv::Error::Timeout) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            record = recv(&tag, &mut self.control_inbounds, std::time::Duration::from_secs(999), ctx.clone()) => {
                match record {
                    Ok(record) => {
                        if let Err(e) = self.handle_action_record(record) {
                            error!("{}: failed to handle action record: {:?}", self.tag, e);
                        }
                    }
                    Err(crate::utils::recv::Error::Timeout) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(())
    }
}

impl Pipe for TimeseriesActionPipe {}
