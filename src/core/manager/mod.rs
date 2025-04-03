pub mod error;

use async_trait::async_trait;
use error::Result;

use crate::{
    config::Config,
    core::inbound::{self, Inbound},
};

use super::component::Component;

pub struct Manager {
    inbounds: Vec<Box<dyn Inbound>>,
}

#[async_trait]
impl Component for Manager {
    type T = ();
    type Error = error::Error;

    async fn poll_async(
        &mut self,
        ctx: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<Self::T, Self::Error> {
        let futs = self
            .inbounds
            .iter_mut()
            .map(|inbound| {
                let ctx = ctx.clone();
                async move { inbound.poll_async(ctx).await }
            })
            .collect::<Vec<_>>();

        let _ = futures::future::join_all(futs)
            .await
            .into_iter()
            .map(|r| r.map_err(Self::Error::from))
            .collect::<Result<Vec<_>>>()?;

        if ctx.is_cancelled() {
            return Err(Self::Error::Cancelled);
        }

        Ok(())
    }
}

pub fn try_create_from_config(cfg: Config) -> Result<Manager> {
    let inbounds = cfg
        .inbounds
        .into_iter()
        .map(|c| inbound::try_create_from_config(c).map_err(error::Error::from))
        .collect::<Result<Vec<_>>>()?;

    let mgr = Manager { inbounds };

    Ok(mgr)
}
