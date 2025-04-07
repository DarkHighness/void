use crate::{
    config::{
        global::{time_tracing_path, use_time_tracing},
        outbound::{auth::AuthConfig, prometheus::PrometheusOutboundConfig},
    },
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        tag::{HasTag, TagId},
        types::{STAGE_OUTBOUND_PROCESSED, STAGE_OUTBOUND_RECEIVED},
    },
    utils::{
        record_timing::{mark_pipeline_stage, summarize_record_timings},
        recv::recv_batch,
    },
};
use std::io::Write;

pub mod error;
pub mod r#type;

use async_trait::async_trait;
pub use error::{Error, Result};
use log::{error, info, warn};
use r#type::WriteRequest;
use tokio_util::sync::CancellationToken;

use super::Outbound;
pub struct PrometheusOutbound {
    tag: TagId,
    address: String,

    interval: std::time::Duration,

    auth: AuthConfig,
    client: reqwest::Client,

    inbounds: Vec<TaggedReceiver>,

    buffer_size: usize,
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
            interval: cfg.interval,
            auth,
            client,
            inbounds,
            buffer_size: cfg.buffer_size,
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
        let interval = (&self.interval).clone();
        let buffer_size = self.buffer_size;

        let mut records =
            match recv_batch(&tag, self.inbounds(), Some(interval), buffer_size, ctx).await {
                Ok(mut records) => {
                    mark_pipeline_stage(&mut records, STAGE_OUTBOUND_RECEIVED);
                    records
                }
                Err(crate::utils::recv::Error::Timeout) => {
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

        mark_pipeline_stage(&mut records, STAGE_OUTBOUND_PROCESSED);
        if use_time_tracing() {
            let file = std::fs::File::options()
                .append(true)
                .create(true)
                .open(time_tracing_path())
                .unwrap();
            let mut writer = std::io::BufWriter::new(file);
            for record in records.iter() {
                writeln!(writer, "{}", summarize_record_timings(record)).unwrap();
            }
        }

        let tss = r#type::transform_timeseries(records)?;

        let last_timestamp = tss
            .iter()
            .flat_map(|ts| ts.samples.iter().map(|s| s.timestamp))
            .max()
            .unwrap_or_default();
        let last_timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(last_timestamp)
            .expect("Invalid timestamp");
        let now = chrono::Utc::now();
        let time_diff = now.signed_duration_since(last_timestamp);
        let time_diff_seconds = time_diff.num_seconds();
        if time_diff_seconds > 5 {
            warn!(
                "{}: last timestamp is {} seconds ago, lagging...",
                self.tag, time_diff_seconds
            );
        }
        let request: WriteRequest = tss.into();
        let request = request.build_request(&self.client, &self.auth, &self.address, "void")?;

        let client = self.client.clone();

        // spawn a task to send the request
        let _ = tokio::task::Builder::new()
            .name(&format!("{}-request", self.tag))
            .spawn(async move {
                let response = client.execute(request).await;

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
            });

        Ok(())
    }
}

impl Outbound for PrometheusOutbound {
    fn inbounds(&mut self) -> &mut [TaggedReceiver] {
        &mut self.inbounds
    }
}
