use crate::{
    config::{
        global::use_time_tracing,
        outbound::{auth::AuthConfig, prometheus::PrometheusOutboundConfig},
    },
    core::{
        actor::Actor,
        manager::{ChannelGraph, TaggedReceiver},
        tag::{HasTag, TagId},
    },
    utils::recv::recv_batch,
};

pub mod error;
pub mod r#type;

use async_trait::async_trait;
pub use error::{Error, Result};
use log::{debug, error, info, warn};
use r#type::WriteRequest;
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

        for record in &records {
            record.mark_record_release();
        }

        let client = self.client.clone();
        let auth = self.auth.clone();
        let address = self.address.clone();
        let tag = self.tag.clone();
        let transform_start_timestamp = std::time::Instant::now();

        let _ = tokio::task::spawn(async move {
            let tss = r#type::transform_timeseries(records)?;

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
            let request = request.build_request(&client, &auth, &address, "void")?;

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
