use std::ops::Deref;

use crate::{
    config::{
        global::use_time_tracing,
        outbound::{auth::AuthConfig, prometheus::PrometheusOutboundConfig},
    },
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        pipe::RECORD_TYPE_TIMESERIES_VALUE,
        tag::{HasTag, TagId},
        types::conv::prometheus::WriteRequest,
    },
    utils::recv::recv_batch,
};

pub mod error;

use async_trait::async_trait;
pub use error::{Error, Result};
use log::{error, info, warn};
use tokio_util::sync::CancellationToken;

use super::Outbound;
pub struct PrometheusOutbound {
    tag: TagId,
    address: String,

    recv_timeout: std::time::Duration,

    auth: AuthConfig,
    client: reqwest::Client,

    inbounds: Vec<TaggedReceiver>,

    recv_buffer_size: usize,
}

impl PrometheusOutbound {
    pub fn try_create_from(
        cfg: PrometheusOutboundConfig,
        channels: &mut ChannelGraph,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;

        let address = cfg.address.to_string();
        let auth = cfg.auth;
        let tag = cfg.tag.into();

        let inbounds = cfg
            .inbounds
            .iter()
            .map(|inbound| channels.recv_from(inbound, &tag))
            .collect::<Vec<_>>();

        Ok(PrometheusOutbound {
            tag,
            address,
            recv_timeout: cfg.recv_timeout,
            auth,
            client,
            inbounds,
            recv_buffer_size: cfg.recv_buffer_size,
        })
    }
}

impl HasTag for PrometheusOutbound {
    fn tag(&self) -> &TagId {
        &self.tag
    }
}

#[async_trait]
impl Actor for PrometheusOutbound {
    type Error = super::Error;

    async fn poll(&mut self, ctx: CancellationToken) -> std::result::Result<(), Self::Error> {
        let tag = self.tag.clone();
        let interval = (&self.recv_timeout).clone();
        let buffer_size = self.recv_buffer_size;

        let records =
            match recv_batch(&tag, self.inbounds(), Some(interval), buffer_size, ctx).await {
                Ok(records) => records,
                Err(crate::utils::recv::Error::Timeout) => {
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

        if records.is_empty() {
            return Ok(());
        }

        let before_len = records.len();
        let records = records
            .into_iter()
            .filter_map(|record| {
                let r#type = record.get_type()?;

                if r#type != RECORD_TYPE_TIMESERIES_VALUE.deref() {
                    return None;
                }

                Some(record)
            })
            .collect::<Vec<_>>();
        let after_len = records.len();
        if after_len != before_len {
            warn!(
                "{}: filtered {} records with wrong types, {} left",
                tag,
                before_len - after_len,
                after_len
            );
        }

        for record in &records {
            record.mark_record_release();
        }

        let client = self.client.clone();
        let auth = self.auth.clone();
        let address = self.address.clone();
        let tag = self.tag.clone();
        let transform_start_timestamp = std::time::Instant::now();

        let _ = tokio::task::spawn(async move {
            let tss = crate::core::types::conv::prometheus::transform_timeseries(records)
                .map_err(error::Error::from)?;

            let last_timestamp = tss
                .iter()
                .flat_map(|ts| ts.samples.iter().map(|s| s.timestamp))
                .max()
                .unwrap_or_default();
            let last_timestamp =
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(last_timestamp)
                    .expect("Invalid timestamp");
            let now = chrono::Utc::now();
            let time_diff = now.signed_duration_since(last_timestamp);
            let time_diff = time_diff.num_milliseconds();
            if time_diff > 1000 {
                warn!(
                    "{}: last timestamp is {:+4} seconds ago, lagging...",
                    tag,
                    (time_diff as f64) / 1000.0
                );
            }
            let request: WriteRequest = tss.into();
            let request = request
                .build_request(&client, &auth, &address, "void")
                .map_err(Error::from)?;

            // spawn a task to send the request
            let response = request.send().await;

            match response {
                Ok(response) => {
                    if response.status() != reqwest::StatusCode::NO_CONTENT {
                        error!(
                            "{}: request failed ({}): {}",
                            tag,
                            response.status(),
                            response.text().await.unwrap_or_default()
                        );
                    }
                }
                Err(e) => {
                    error!("{}: request failed: {}", tag, e);
                }
            }

            if use_time_tracing() {
                let elapsed = transform_start_timestamp.elapsed();
                info!("{}: prometheus request took {:?}", tag, elapsed);
            }

            Ok::<(), super::Error>(())
        });

        Ok(())
    }

    fn is_blocking(&self) -> bool {
        false
    }
}

impl Outbound for PrometheusOutbound {
    fn inbounds(&mut self) -> &mut [TaggedReceiver] {
        &mut self.inbounds
    }
}
