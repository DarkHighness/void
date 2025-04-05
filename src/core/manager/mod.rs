pub mod error;
mod graph;

use std::collections::HashMap;

use futures::TryFutureExt;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    core::{
        inbound::{self, Inbound},
        outbound,
        pipe::{self},
    },
    timeit,
};
use log::{error, info};

pub use error::{Error, Result};
pub use graph::{ChannelGraph, TaggedReceiver, TaggedSender};

use super::{outbound::Outbound, pipe::Pipe, tag::HasTag};

pub struct Manager {
    inbounds: Vec<Box<dyn Inbound>>,
    pipes: Vec<Box<dyn Pipe>>,
    outbounds: Vec<Box<dyn Outbound>>,
    // We hold the channels here to prevent them from being dropped
    // before the pipes are done using them.
    channel_graph: ChannelGraph,
}

pub fn try_create_from_config(cfg: Config) -> Result<Manager> {
    info!("Creating manager from config...");

    let mut channel_graph = timeit! { "Creating channel graph", {
            ChannelGraph::try_create_from(&cfg.inbounds,&cfg.pipes, &cfg.outbounds)?
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
                    inbound::try_create_from(cfg, protocol_cfg, &mut channel_graph).map_err(error::Error::from)
            })
            .collect::<Result<Vec<_>>>()?
        }
    };

    let pipes = timeit! { "Creating pipes", {
        cfg.pipes
            .into_iter()
            .map(|cfg| {
                let pipe = pipe::try_create_from(cfg, &mut channel_graph)?;
                Ok(pipe)
            })
            .collect::<Result<Vec<_>>>()?
    }};

    let outbounds = timeit! { "Creating outbounds", {
        cfg.outbounds
            .into_iter()
            .map(|cfg| {
                let outbound = outbound::try_create_from(cfg, &mut channel_graph)?;
                Ok(outbound)
            })
            .collect::<Result<Vec<_>>>()?
    }};

    channel_graph.dump_to_dot();

    let mgr = Manager {
        inbounds,
        pipes,
        outbounds,
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
    pub async fn run(self, ctx: CancellationToken) -> Result<()> {
        info!("Starting manager...");

        let mut inbounds = self.inbounds;
        let inbound_ctx = ctx.child_token();
        let inbound_handle = tokio::spawn(async move {
            loop {
                let inbounds = inbounds
                    .iter_mut()
                    .map(|actor| actor.poll(inbound_ctx.clone()).map_err(Error::from));

                let out = futures::future::try_join_all(inbounds);
                tokio::select! {
                    out = out => {
                        match out {
                            Ok(_) => {},
                            Err(err) => {
                                error!("Inbound error: {}", err);
                            }
                        }
                    }
                    _ = inbound_ctx.cancelled() => {
                        break;
                    }
                }
            }

            info!("Inbound handle finished");
        });

        let mut pipes = self.pipes;
        let pipe_ctx = ctx.child_token();
        let pipe_handle = tokio::spawn(async move {
            loop {
                let pipes = pipes
                    .iter_mut()
                    .map(|actor| actor.poll(pipe_ctx.clone()).map_err(Error::from));

                let out = futures::future::try_join_all(pipes);
                tokio::select! {
                    out = out => {
                        match out {
                            Ok(_) => {},
                            Err(err) => {
                                error!("Pipe error: {}", err);
                            }
                        }
                    }
                    _ = pipe_ctx.cancelled() => {
                        break;
                    }
                }
            }

            info!("Pipe handle finished");
        });

        let mut outbounds = self.outbounds;
        let outbound_ctx = ctx.child_token();
        let outbound_handle = tokio::spawn(async move {
            loop {
                let outbounds = outbounds
                    .iter_mut()
                    .map(|actor| actor.poll(outbound_ctx.clone()).map_err(Error::from));

                let out = futures::future::try_join_all(outbounds);
                tokio::select! {
                    out = out => {
                        match out {
                            Ok(_) => {},
                            Err(err) => {
                                error!("Outbound error: {}", err);
                            }
                        }
                    }
                    _ = outbound_ctx.cancelled() => {
                        return;
                    }
                }
            }
        });

        info!("Manager started");
        let _ = tokio::try_join!(inbound_handle, pipe_handle, outbound_handle);

        Ok(())
    }
}
