use std::collections::HashMap;

use log::{debug, info};
use petgraph::{csr::DefaultIx, graph::NodeIndex, Direction, Graph};
use tokio::sync::broadcast;

use crate::{
    config::{inbound::InboundConfig, pipe::PipeConfig, OutboundConfig},
    core::{
        tag::{HasTag, TagId},
        types::Record,
    },
};

pub const PIPE_CHANNEL_BUFFER_SIZE: usize = 64;

#[derive(Debug)]
pub struct PipeChannel {
    tag: TagId,

    // The inbound channel is used to receive data from the previous pipe in the chain.
    inbound_tx: broadcast::Sender<Record>,
    inbound_rx: broadcast::Receiver<Record>,
    /// The outbound channel is used to send data to the next pipe in the chain.
    outbound_tx: broadcast::Sender<Record>,
    outbound_rx: broadcast::Receiver<Record>,
}

pub struct ChannelGraph {
    pipe_channels: HashMap<TagId, PipeChannel>,

    inbound_channels: HashMap<TagId, Vec<broadcast::Sender<Record>>>,
    outbound_channels: HashMap<TagId, Vec<broadcast::Receiver<Record>>>,
}

impl ChannelGraph {
    pub fn try_create_from(
        pipes: &[PipeConfig],
        inbounds: &[InboundConfig],
        outbonds: &[OutboundConfig],
    ) -> super::Result<Self> {
        let (nodes, dag) = Self::try_create_dag_from(pipes, inbounds, outbonds)?;

        let mut pipe_channels = HashMap::new();
        for pipe in pipes {
            let tag = pipe.tag();

            if pipe_channels.contains_key(&tag) {
                panic!(
                    "Duplicate pipe tag: {}, panic should not be happen here",
                    tag
                );
            }

            let (inbound_tx, inbound_rx) = broadcast::channel(PIPE_CHANNEL_BUFFER_SIZE);
            let (outbound_tx, outbound_rx) = broadcast::channel(PIPE_CHANNEL_BUFFER_SIZE);

            let channel = PipeChannel {
                tag: tag.clone(),
                inbound_tx,
                inbound_rx,
                outbound_tx,
                outbound_rx,
            };

            pipe_channels.insert(tag, channel);
        }

        let mut inbound_channels = HashMap::new();

        // Collecting channels for inbounds and outbounds
        for inbound in inbounds {
            let tag = inbound.tag();
            let mut outbounds = vec![];

            let node = nodes.get(&tag).expect("Node not found in DAG for inbound");
            for neighbour in dag.neighbors_directed(*node, Direction::Outgoing) {
                let neighbour_tag = dag[neighbour].clone();
                let neighbour_channels = pipe_channels
                    .get_mut(&neighbour_tag)
                    .expect("Channel not found in DAG for neighbour");
                outbounds.push(neighbour_channels.inbound_tx.clone());
            }

            inbound_channels.insert(tag, outbounds);
        }

        let mut outbound_channels = HashMap::new();
        for outbound in outbonds {
            let tag = outbound.tag();
            let mut inbounds = vec![];

            let node = nodes.get(&tag).expect("Node not found in DAG for outbound");
            for neighbour in dag.neighbors_directed(*node, Direction::Incoming) {
                let neighbour_tag = dag[neighbour].clone();
                let neighbour_channels = pipe_channels
                    .get_mut(&neighbour_tag)
                    .expect("Channel not found in DAG for neighbour");
                inbounds.push(neighbour_channels.outbound_rx.resubscribe());
            }

            outbound_channels.insert(tag, inbounds);
        }

        Ok(ChannelGraph {
            pipe_channels,
            inbound_channels,
            outbound_channels,
        })
    }

    pub fn pipe_outbound(&self, tag: &TagId) -> Option<broadcast::Sender<Record>> {
        self.pipe_channels
            .get(tag)
            .map(|channel| channel.outbound_tx.clone())
    }

    pub fn pipe_inbound(&self, tag: &TagId) -> Option<broadcast::Receiver<Record>> {
        self.pipe_channels
            .get(tag)
            .map(|channel| channel.inbound_rx.resubscribe())
    }

    pub fn unsafe_pipe_outbound(&self, tag: &TagId) -> broadcast::Sender<Record> {
        self.pipe_channels
            .get(tag)
            .map(|channel| channel.outbound_tx.clone())
            .unwrap_or_else(|| panic!("Pipe channel not found for tag: {}", tag))
    }

    pub fn unsafe_pipe_inbound(&self, tag: &TagId) -> broadcast::Receiver<Record> {
        self.pipe_channels
            .get(tag)
            .map(|channel| channel.inbound_rx.resubscribe())
            .unwrap_or_else(|| panic!("Pipe channel not found for tag: {}", tag))
    }

    pub fn inbound_outputs(&self, tag: &TagId) -> Option<Vec<broadcast::Sender<Record>>> {
        self.inbound_channels.get(tag).cloned()
    }

    pub fn outbound_inputs(&self, tag: &TagId) -> Option<Vec<broadcast::Receiver<Record>>> {
        self.outbound_channels
            .get(tag)
            .map(|c| c.iter().map(|c| c.resubscribe()).collect())
    }

    pub fn unsafe_inbound_outputs(&self, tag: &TagId) -> Vec<broadcast::Sender<Record>> {
        self.inbound_channels
            .get(tag)
            .map(|channels| channels.clone())
            .unwrap_or_else(|| panic!("Inbound channel not found for tag: {}", tag))
    }

    pub fn unsafe_outbound_inputs(&self, tag: &TagId) -> Vec<broadcast::Receiver<Record>> {
        self.outbound_channels
            .get(tag)
            .map(|c| c.iter().map(|c| c.resubscribe()).collect())
            .unwrap_or_else(|| panic!("Outbound channel not found for tag: {}", tag))
    }

    fn try_create_dag_from(
        pipes: &[PipeConfig],
        inbounds: &[InboundConfig],
        outbonds: &[OutboundConfig],
    ) -> super::Result<(
        HashMap<TagId, NodeIndex<DefaultIx>>,
        Graph<TagId, (), petgraph::Directed>,
    )> {
        let mut dag = Graph::<TagId, (), petgraph::Directed, DefaultIx>::new();

        // Collect all tags from pipes, inbounds, and outbounds
        let mut nodes = HashMap::new();
        for tag in pipes
            .iter()
            .map(HasTag::tag)
            .chain(inbounds.iter().map(HasTag::tag))
            .chain(outbonds.iter().map(HasTag::tag))
        {
            if nodes.contains_key(&tag) {
                panic!(
                    "Duplicate tag found: {}, panic should not be happen here",
                    tag
                );
            }

            nodes.insert(tag.clone(), dag.add_node(tag));
        }

        for (tag, inbounds) in pipes
            .iter()
            .map(|e| (e.tag().clone(), e.inbounds()))
            .chain(outbonds.iter().map(|e| (e.tag().clone(), e.inbounds())))
        {
            let node = nodes.get(&tag).expect("Node not found in DAG");
            for inbound in inbounds {
                if let Some(inbound_node) = nodes.get(&inbound) {
                    info!("Found data flow: {} -> {}", inbound, tag);
                    dag.add_edge(*inbound_node, *node, ());
                } else {
                    return Err(super::Error::UnknownTagRequired(inbound, tag));
                }
            }
        }

        Ok((nodes, dag))
    }
}
