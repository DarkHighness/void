pub mod error;

use std::{collections::HashMap, sync::Arc};

use error::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    core::inbound::{self, Inbound},
};
use log::error;

use error::Error;

use super::tag::HasTag;

pub struct Manager {
    inbounds: Vec<Box<dyn Inbound>>,
}

pub fn try_create_from_config(cfg: Config) -> Result<Manager> {
    let protocols = cfg
        .protocols
        .into_iter()
        .map(|p| (p.tag(), p))
        .collect::<HashMap<_, _>>();

    let inbounds = cfg
        .inbounds
        .into_iter()
        .map(|cfg| {
            let protocol_id = cfg.protocol();
            let protocol_cfg = protocols
                .get(&protocol_id)
                .cloned()
                .ok_or_else(|| Error::ProtocolNotFound(protocol_id))?;
            inbound::try_create_from(cfg, protocol_cfg).map_err(error::Error::from)
        })
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
