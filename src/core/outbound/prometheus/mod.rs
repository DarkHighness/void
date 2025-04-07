use crate::{
    config::outbound::{auth::AuthConfig, prometheus::PrometheusOutboundConfig},
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
use log::{info, warn};
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

        let records =
            match recv_batch(&tag, self.inbounds(), Some(interval), buffer_size, ctx).await {
                Ok(records) => records,
                Err(crate::utils::recv::Error::Timeout) => {
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

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

        let response = self
            .client
            .execute(request)
            .await
            .map_err(|e| Error::from(e))?;

        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(Error::RequestError(format!(
                "{}: request failed ({}): {}",
                self.tag,
                response.status(),
                response.text().await.unwrap_or_default()
            ))
            .into());
        }

        Ok(())
    }
}

impl Outbound for PrometheusOutbound {
    fn inbounds(&mut self) -> &mut [TaggedReceiver] {
        &mut self.inbounds
    }
}
