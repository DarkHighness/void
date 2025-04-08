use std::{ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
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
        let mut labels = record
            .get_mut(&LABELS_FIELD)
            .ok_or_else(|| super::Error::FieldNotFound(LABELS_FIELD_STR))?
            .map_mut()?;

        for label in self.labels_to_add.iter() {
            let name = label.key().into();
            let value = label.value().clone();

            labels.set(name, value);
        }

        for label in self.labels_to_remove.iter() {
            let name = label.clone().into();
            let _ = labels.remove(&name);
        }

        Ok(record)
    }

    fn handle_action(&self, record: Record) -> super::Result<()> {
        let action = record
            .get(ACTION_FIELD.deref())
            .ok_or_else(|| super::Error::FieldNotFound(ACTION_FIELD_STR))?;
        let action_guard = action.string()?;
        let action = action_guard.as_str();

        match action {
            ACTION_SET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.string()?.as_symbol().clone();
                let value = record
                    .get(VALUE_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(VALUE_FIELD_STR))?;

                let value = value.cast_string()?;

                info!(
                    "Timeseries {} will set label: {} = {}",
                    self.tag, name, value
                );

                self.labels_to_add.insert(name, value);
            }
            ACTION_UNSET => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name_guard = name.string()?;
                let name = name_guard.as_symbol();

                info!("Timeseries {} will no longer set label: {}", self.tag, name);

                self.labels_to_add.remove(name);
            }
            ACTION_DELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name = name.string()?.as_symbol().clone();

                info!("Timeseries {} will delete label: {}", self.tag, name);

                self.labels_to_remove.insert(name);
            }
            ACTION_UNDELETE => {
                let name = record
                    .get(NAME_FIELD.deref())
                    .ok_or_else(|| super::Error::FieldNotFound(NAME_FIELD_STR))?;

                let name_guard = name.string()?;
                let name = name_guard.as_symbol();

                info!("Timeseries {} will undelete label: {}", self.tag, name);

                self.labels_to_remove.remove(name);
            }
            ACTION_CLEAR => {
                info!("Timeseries {} will clear all actions", self.tag);

                self.labels_to_add.clear();
                self.labels_to_remove.clear();
            }
            _ => {
                return Err(super::Error::InvalidAction(action.to_string()));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct TimeseriesAnnotatePipe {
    tag: TagId,

    data_inbounds: Vec<TaggedReceiver>,
    control_inbounds: Vec<TaggedReceiver>,

    inner: Arc<InnerState>,

    outbound: TaggedSender,

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
            interval: cfg.recv_timeout,
            buffer_size: cfg.recv_buffer_size,
        };

        Ok(pipe)
    }

    fn transform_records(&mut self, records: Vec<Record>) -> super::Result<()> {
        let inner = self.inner.clone();
        let outbound = &mut self.outbound;

        // transform records
        let transformed_records: Vec<_> = records
            .into_iter()
            .filter_map(|record| match inner.transform(record) {
                Ok(record) => Some(record),
                Err(e) => {
                    error!("{}: failed to transform record: {:?}", inner.tag, e);
                    None
                }
            })
            .collect();

        transformed_records.into_iter().for_each(|r| {
            if let Err(e) = outbound.send(r) {
                error!("{}: failed to send record: {:?}", inner.tag, e);
            }
        });

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
        let control_inbounds = &mut self.control_inbounds;
        let data_inbounds = &mut self.data_inbounds;

        tokio::select! {
            biased;

            records = recv_batch(
                &tag,
                data_inbounds,
                Some(self.interval),
                self.buffer_size,
                ctx.clone(),
            ) => {
                match records {
                    Ok(records) => {
                        self.transform_records(records)?;
                    }
                    Err(crate::utils::recv::Error::Timeout) => {}
                    Err(e) => return Err(e.into()),
                }
            },
            control_record = recv(&tag, control_inbounds, None, ctx.clone()) => match control_record {
                Ok(record) => {
                    if let Err(e) = self.inner.handle_action(record) {
                        error!("{}: failed to handle action record: {:?}", tag, e);
                    }
                }
                Err(crate::utils::recv::Error::Timeout) => {}
                Err(e) => {
                    error!("{}: failed to receive control record: {:?}", tag, e);
                }
            },
            _ = ctx.cancelled() => {
                info!("{}: cancelled", tag);
                return Ok(());
            }
        }

        Ok(())
    }
}

impl Pipe for TimeseriesAnnotatePipe {}
