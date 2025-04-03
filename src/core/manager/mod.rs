pub mod error;

use error::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    core::inbound::{self, Inbound},
};
use log::error;

use error::Error;

pub struct Manager {
    inbounds: Vec<Box<dyn Inbound>>,
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

impl Manager {
    pub async fn run(&mut self, ctx: CancellationToken) -> Result<()> {
        loop {
            let futs = self
                .inbounds
                .iter_mut()
                .map(|inbound| {
                    let ctx = ctx.clone();
                    async move { inbound.poll_async(ctx).await }
                })
                .collect::<Vec<_>>();

            tokio::select! {
                futs = futures::future::join_all(futs) => {
                    futs.into_iter()
                        .filter_map(|r| r.err())
                        .map(Error::from)
                        .for_each(|err| {
                            error!("Inbound error: {}", err);
                        });
                }
                _ = ctx.cancelled() => {
                    // Cancellation token was triggered
                    return Err(Error::Cancelled);
                }
            }
        }
    }
}
