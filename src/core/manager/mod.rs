pub mod error;
mod graph;

use std::collections::HashMap;

use futures::{StreamExt, TryFutureExt};
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    core::{
        actor,
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
    inbounds: Vec<Box<dyn Inbound + 'static>>,
    pipes: Vec<Box<dyn Pipe + 'static>>,
    outbounds: Vec<Box<dyn Outbound + 'static>>,
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
                    let inbound = inbound::try_create_from(cfg, protocol_cfg, &mut channel_graph).map_err(actor::Error::from)?;

                Ok(inbound)
            })
            .collect::<Result<Vec<_>>>()?
        }
    };

    let pipes = timeit! { "Creating pipes", {
        cfg.pipes
            .into_iter()
            .map(|cfg| {
                let pipe = pipe::try_create_from(cfg, &mut channel_graph).map_err(actor::Error::from)?;
                Ok(pipe)
            })
            .collect::<Result<Vec<_>>>()?
    }};

    let outbounds = timeit! { "Creating outbounds", {
        cfg.outbounds
            .into_iter()
            .map(|cfg| {
                let outbound = outbound::try_create_from(cfg, &mut channel_graph).map_err(actor::Error::from)?;
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

    Ok(mgr)
}

impl Manager {
    pub async fn run(self, ctx: CancellationToken) -> Result<()> {
        info!("Starting manager...");

        let mut handles = vec![];

        let outbounds = self.outbounds;
        for outbound in outbounds {
            let handle = actor::spawn(outbound, ctx.child_token());
            handles.push(handle);
        }

        let pipes = self.pipes;
        for pipe in pipes {
            let handle = actor::spawn(pipe, ctx.child_token());
            handles.push(handle);
        }

        let inbounds = self.inbounds;
        for inbound in inbounds {
            let handle = actor::spawn(inbound, ctx.child_token());
            handles.push(handle);
        }

        // Wait for all handles to finish
        futures::future::try_join_all(handles)
            .await?
            .iter()
            .for_each(|result| {
                if let Err(err) = result {
                    error!("Error in actor: {:?}", err);
                }
            });

        Ok(())
    }
}
