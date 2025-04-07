use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use log::{error, info};
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;

use crate::{
    config::{global::use_serial_mode, pipe::timeseries::TimeseriesAnnotatePipeConfig},
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver, TaggedSender},
        pipe::Pipe,
        tag::{HasTag, TagId},
        types::{Record, Symbol, Value, STAGE_PIPE_PROCESSED, STAGE_PIPE_RECEIVED},
    },
    utils::{
        record_timing::mark_pipeline_stage,
        recv::{recv, recv_batch},
    },
};

use super::{LABELS_FIELD, LABELS_FIELD_STR};

#[derive(Debug)]
struct InnerState {
    tag: TagId,
    labels_to_add: DashMap<Symbol, Value>,
    labels_to_remove: DashSet<Symbol>,
}

impl InnerState {
    fn new(tag: TagId) -> Self {
        Self {
            tag,
            labels_to_add: DashMap::new(),
            labels_to_remove: DashSet::new(),
        }
    }

    fn transform(&self, mut record: Record) -> super::Result<Record> {
        let labels = record
            .get_mut(&LABELS_FIELD)
            .ok_or_else(|| super::Error::FieldNotFound(LABELS_FIELD_STR))?;

        for label in self.labels_to_add.iter() {
            let name = label.key().into();
            let value = label.value().clone();
            labels.map_set(name, value)?;
        }

        for label in self.labels_to_remove.iter() {
            let name = label.clone().into();
            labels.map_remove(&name)?;
        }

        Ok(record)
    }
}

#[derive(Debug)]
pub struct TimeseriesAnnotatePipe {
    tag: TagId,

    data_inbounds: Vec<TaggedReceiver>,
    control_inbounds: Vec<TaggedReceiver>,

    inner: Arc<InnerState>,

    outbound: TaggedSender,
    last_handle: Option<JoinHandle<()>>,

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
        let inner = Arc::new(InnerState::new((&cfg.tag).into()));

        let pipe = TimeseriesAnnotatePipe {
            tag: cfg.tag.into(),
            data_inbounds,
            control_inbounds,
            inner,
            outbound,
            last_handle: None,
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

                self.inner.labels_to_add.insert(name, value);
            }
            ACTION_UNSET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.ensure_string()?;

                info!(
                    "Timeseries {} will no longer set label: {}",
                    self.tag(),
                    name
                );

                self.inner.labels_to_add.remove(name);
            }
            ACTION_DELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.ensure_string()?.clone();

                info!("Timeseries {} will delete label: {}", self.tag(), name);

                self.inner.labels_to_remove.insert(name);
            }
            ACTION_UNDELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.ensure_string()?;

                info!("Timeseries {} will undelete label: {}", self.tag(), name);

                self.inner.labels_to_remove.remove(name);
            }
            ACTION_CLEAR => {
                info!("Timeseries {} will clear all actions", self.tag());

                self.inner.labels_to_add.clear();
                self.inner.labels_to_remove.clear();
            }
            _ => {
                return Err(super::Error::InvalidAction(action.to_string()));
            }
        }

        Ok(())
    }

    fn transform_records(&self, mut records: Vec<Record>) -> super::Result<JoinHandle<()>> {
        for record in records.iter_mut() {
            record.mark_timestamp(&format!("{}-{}", STAGE_PIPE_RECEIVED, self.tag));
        }

        let inner = self.inner.clone();
        let outbound = self.outbound.clone();

        // transform records
        let mut transformed_records: Vec<_> = records
            .into_iter()
            .filter_map(|record| match inner.transform(record) {
                Ok(record) => Some(record),
                Err(e) => {
                    error!("{}: failed to transform record: {:?}", inner.tag, e);
                    None
                }
            })
            .collect();

        for record in transformed_records.iter_mut() {
            record.mark_timestamp(&format!("{}-{}", STAGE_PIPE_PROCESSED, inner.tag));
        }

        let handle = tokio::task::Builder::new()
            .name(&format!("{}-send", self.tag))
            .spawn(async move {
                // send transformed records
                transformed_records.into_iter().for_each(|r| {
                    if let Err(e) = outbound.send(r) {
                        error!("{}: failed to send record: {:?}", inner.tag, e);
                    }
                });
            })?;

        Ok(handle)
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
            biased;
            records = recv_batch(&tag, &mut self.data_inbounds, Some(self.interval), self.buffer_size, ctx.clone()) => {
                match records {
                    Ok(records) => match use_serial_mode() {
                        true => {
                            if let Some(handle) = self.last_handle.take() {
                                handle.await?;
                            }
                            self.last_handle = Some(self.transform_records(records)?);
                        }
                        false => {
                            let _ = self.transform_records(records)?;
                        }
                    }
                    Err(crate::utils::recv::Error::Timeout) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            _ = ctx.cancelled() => return Ok(()),
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
