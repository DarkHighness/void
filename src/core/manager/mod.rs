pub mod error;
mod graph;

use std::collections::HashMap;

use futures::TryFutureExt;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    core::{
        inbound::{self, Inbound},
        pipe::{self},
    },
    timeit,
};
use log::{error, info};

pub use error::{Error, Result};
pub use graph::ChannelGraph;

use super::{pipe::Pipe, tag::HasTag};

pub struct Manager {
    inbounds: Vec<Box<dyn Inbound>>,
    pipes: Vec<Box<dyn Pipe>>,
    // We hold the channels here to prevent them from being dropped
    // before the pipes are done using them.
    channel_graph: ChannelGraph,
}

pub fn try_create_from_config(cfg: Config) -> Result<Manager> {
    info!("Creating manager from config...");

    let channel_graph = timeit! { "Creating channel graph", {
            ChannelGraph::try_create_from(&cfg.pipes, &cfg.inbounds, &cfg.outbounds)?
    }};

    let inbounds = timeit! { "Creating inbounds", {
        let protocols = cfg
            .protocols
            .into_iter()
            .map(|p| (p.tag().clone(), p))
            .collect::<HashMap<_, _>>();

        cfg.inbounds
            .into_iter()
            .map(|cfg| {
                let protocol_id = cfg.protocol();
                let protocol_cfg = protocols
                    .get(&protocol_id)
                    .cloned()
                    .ok_or_else(|| Error::ProtocolNotFound(protocol_id))?;
                    inbound::try_create_from(cfg, protocol_cfg, &channel_graph).map_err(error::Error::from)
            })
            .collect::<Result<Vec<_>>>()?
        }
    };

    let pipes = timeit! { "Creating pipes", {
        cfg.pipes
            .into_iter()
            .map(|cfg| {
                let pipe = pipe::try_create_from(cfg, &channel_graph)?;
                Ok(pipe)
            })
            .collect::<Result<Vec<_>>>()?
    }};

    let mgr = Manager {
        inbounds,
        pipes,
        channel_graph,
    };

    info!(
        "Total interned strings: {}",
        crate::core::types::num_interned_strings()
    );

    // let inbounds = ",".join(
    //     &mgr.inbounds
    //         .iter()
    //         .map(|e| format!("{}", e.tag()))
    //         .collect::<Vec<_>>(),
    // );
    // info!("Inbounds: [{}]", inbounds);

    // let pipes = ",".join(
    //     &mgr.pipes
    //         .iter()
    //         .map(|e| format!("{}", e.tag()))
    //         .collect::<Vec<_>>(),
    // );
    // info!("Pipes: [{}]", pipes);

    Ok(mgr)
}

impl Manager {
    pub async fn run(&mut self, ctx: CancellationToken) -> Result<()> {
        loop {
            let inbound_futs = self
                .inbounds
                .iter_mut()
                .map(|actor| actor.poll(ctx.clone()).map_err(Error::from));
            let pipe_futs = self
                .pipes
                .iter_mut()
                .map(|actor| actor.poll(ctx.clone()).map_err(Error::from));

            tokio::select! {
                inbounds = futures::future::join_all(inbound_futs) => {
                    inbounds.into_iter()
                        .filter_map(|r| r.err())
                        .for_each(|err| {
                            error!("Inbound error: {}", err);
                        });
                }
                pipes = futures::future::join_all(pipe_futs) => {
                    pipes.into_iter()
                        .filter_map(|r| r.err())
                        .for_each(|err| {
                            error!("Pipe error: {}", err);
                        });
                }
                _ = ctx.cancelled() => {
                    return Err(Error::Cancelled);
                }
            }
        }
    }
}
