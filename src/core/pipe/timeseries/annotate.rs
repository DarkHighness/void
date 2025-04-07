use std::{collections::HashMap, ops::Deref, time::Duration};

use async_trait::async_trait;
use log::{error, info};
use once_cell::sync::Lazy;

use crate::{
    config::pipe::timeseries::TimeseriesAnnotatePipeConfig,
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver, TaggedSender},
        pipe::Pipe,
        tag::{HasTag, TagId},
        types::{Record, Symbol, Value},
    },
    utils::recv::{recv, recv_batch},
};

use super::{LABELS_FIELD, LABELS_FIELD_STR};

#[derive(Debug)]
pub struct TimeseriesAnnotatePipe {
    tag: TagId,

    data_inbounds: Vec<TaggedReceiver>,
    control_inbounds: Vec<TaggedReceiver>,
    outbound: TaggedSender,

    labels_to_add: HashMap<Symbol, Value>,
    labels_to_remove: Vec<Symbol>,

    interval: Duration,
    buffer_size: usize,
}

pub const ACTION_FIELD_STR: &str = "action";
pub const NAME_FIELD_STR: &str = "name";
pub const VALUE_FIELD_STR: &str = "value";

pub static ACTION_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(ACTION_FIELD_STR));
pub static NAME_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(NAME_FIELD_STR));
pub static VALUE_FIELD: Lazy<Symbol> = Lazy::new(|| Symbol::intern(VALUE_FIELD_STR));

pub const ACTION_SET: &'static str = "set";
pub const ACTION_UNSET: &'static str = "unset";
pub const ACTION_DELETE: &'static str = "delete";
pub const ACTION_UNDELETE: &'static str = "undelete";
pub const ACTION_CLEAR: &'static str = "clear";

impl TimeseriesAnnotatePipe {
    pub fn try_create_from(
        cfg: TimeseriesAnnotatePipeConfig,
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

        let pipe = TimeseriesAnnotatePipe {
            tag: cfg.tag.into(),
            data_inbounds,
            control_inbounds,
            outbound,
            labels_to_add: HashMap::new(),
            labels_to_remove: Vec::new(),
            interval: cfg.interval,
            buffer_size: cfg.buffer_size,
        };

        Ok(pipe)
    }

    fn handle_action_record(&mut self, record: Record) -> super::Result<()> {
        let action = record
            .get(ACTION_FIELD.deref())
            .ok_or_else(|| super::Error::FieldNotFound(ACTION_FIELD_STR))?;

        let action = action.ensure_string()?;
        let action = action.as_ref();

        match action {
            ACTION_SET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.ensure_string()?.clone();

                let value = record
                    .get(VALUE_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(VALUE_FIELD_STR))?;

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
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

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
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.ensure_string()?.clone();

                info!("Timeseries {} will delete label: {}", self.tag(), name);

                self.labels_to_remove.push(name);
            }
            ACTION_UNDELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

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
                return Err(super::Error::InvalidAction(action.to_string()));
            }
        }

        Ok(())
    }

    fn transform(&self, record: &mut Record) -> super::Result<()> {
        let labels = record
            .get_mut(&super::LABELS_FIELD)
            .ok_or_else(|| super::Error::FieldNotFound(LABELS_FIELD_STR))?;

        for (label, value) in &self.labels_to_add {
            labels.map_set(label.into(), value.clone())?;
        }

        for label in &self.labels_to_remove {
            let label: Value = label.into();
            labels.map_remove(&label)?;
        }

        Ok(())
    }
}

impl HasTag for TimeseriesAnnotatePipe {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for TimeseriesAnnotatePipe {
    type Error = super::Error;

    async fn poll(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), super::Error> {
        let tag = self.tag().clone();

        tokio::select! {
            _ = ctx.cancelled() => return Ok(()),
            records = recv_batch(&tag, &mut self.data_inbounds, Some(self.interval), self.buffer_size, ctx.clone()) => {
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
            record = recv(&tag, &mut self.control_inbounds, None, ctx.clone()) => {
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

impl Pipe for TimeseriesAnnotatePipe {}
