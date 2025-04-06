use log::info;
use petgraph::csr::DefaultIx;
use std::hash::{Hash, Hasher};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};
use tokio::sync::broadcast;

use crate::{
    config::{inbound::InboundConfig, pipe::PipeConfig, OutboundConfig},
    core::{
        tag::{HasTag, TagId},
        types::Record,
    },
};

#[derive(Debug)]
pub struct ActorChannel {
    tag: TagId,

    sender: Option<broadcast::Sender<Record>>,
    receiver: broadcast::Receiver<Record>,
}

pub struct TaggedSender {
    tag: TagId,
    sender: broadcast::Sender<Record>,
}

impl TaggedSender {
    pub fn new(tag: TagId, sender: broadcast::Sender<Record>) -> Self {
        TaggedSender { tag, sender }
    }

    pub fn tag(&self) -> &TagId {
        &self.tag
    }

    pub fn sender(&self) -> &broadcast::Sender<Record> {
        &self.sender
    }
}

pub struct TaggedReceiver {
    tag: TagId,
    receiver: broadcast::Receiver<Record>,
}

impl TaggedReceiver {
    pub fn new(tag: TagId, receiver: broadcast::Receiver<Record>) -> Self {
        TaggedReceiver { tag, receiver }
    }

    pub fn tag(&self) -> &TagId {
        &self.tag
    }

    pub fn receiver(&self) -> &broadcast::Receiver<Record> {
        &self.receiver
    }
}

impl Deref for TaggedSender {
    type Target = broadcast::Sender<Record>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl Deref for TaggedReceiver {
    type Target = broadcast::Receiver<Record>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl DerefMut for TaggedReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

impl DerefMut for TaggedSender {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

impl PartialEq for TaggedSender {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl PartialEq for TaggedReceiver {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl Eq for TaggedSender {}
impl Eq for TaggedReceiver {}

impl Hash for TaggedSender {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
    }
}

impl Hash for TaggedReceiver {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
    }
}

pub const CHANNEL_BUFFER_SIZE: usize = 1024;

impl ActorChannel {
    pub fn new(tag: TagId) -> Self {
        let (sender, receiver) = broadcast::channel(CHANNEL_BUFFER_SIZE);
        ActorChannel {
            tag,
            sender: Some(sender),
            receiver,
        }
    }

    pub fn tag(&self) -> &TagId {
        &self.tag
    }

    pub fn sender(&mut self) -> TaggedSender {
        let sender = self.sender.take().expect("Sender already taken");
        TaggedSender {
            tag: self.tag.clone(),
            sender,
        }
    }

    pub fn receiver(&self) -> TaggedReceiver {
        let receiver = self.receiver.resubscribe();
        TaggedReceiver {
            tag: self.tag.clone(),
            receiver,
        }
    }
}

#[derive(Debug)]
pub struct ChannelGraph {
    channels: HashMap<TagId, ActorChannel>,

    graph: petgraph::Graph<TagId, (), petgraph::Directed, DefaultIx>,
    tag_2_idx: HashMap<TagId, petgraph::graph::NodeIndex<DefaultIx>>,
}

impl ChannelGraph {
    pub fn try_create_from(
        inbounds: &[InboundConfig],
        pipes: &[PipeConfig],
        outbounds: &[OutboundConfig],
    ) -> super::Result<Self> {
        let tags = inbounds
            .iter()
            .map(HasTag::tag)
            .chain(pipes.iter().map(HasTag::tag))
            .chain(outbounds.iter().map(HasTag::tag))
            .cloned()
            .collect::<Vec<_>>();

        let mut graph = petgraph::Graph::<TagId, (), petgraph::Directed, DefaultIx>::new();
        let mut tag_to_idx = HashMap::new();

        let mut channels = HashMap::new();
        for tag in tags {
            if channels.contains_key(&tag) {
                return Err(super::Error::DuplicateTag(tag));
            }

            let node = graph.add_node(tag.clone());
            tag_to_idx.insert(tag.clone(), node);

            let channel = ActorChannel::new(tag.clone());
            channels.insert(tag, channel);
        }

        let graph = ChannelGraph {
            channels,
            graph,
            tag_2_idx: tag_to_idx,
        };

        Ok(graph)
    }

    pub fn sender(&mut self, tag: &TagId) -> TaggedSender {
        let channel = self
            .channels
            .get_mut(tag)
            .expect("Channel not found in DAG");

        info!("Sender {} has been taken", tag);

        channel.sender()
    }

    pub fn recv_from(&mut self, tag: &TagId, who: &TagId) -> TaggedReceiver {
        let channel = self.channels.get(tag).expect("Channel not found in DAG");
        let receiver = channel.receiver();

        let src = self.tag_2_idx.get(tag).expect("Tag not found in DAG");
        let dst = self.tag_2_idx.get(who).expect("Tag not found in DAG");

        self.graph.add_edge(*src, *dst, ());
        info!("Receiver of {} has been taken by {}", tag, who);

        receiver
    }

    pub fn query_inbounds(&self, tag: &TagId) -> Vec<TagId> {
        let node = self.tag_2_idx.get(tag).expect("Tag not found in DAG");
        let mut inbounds = vec![];

        for inbound in self
            .graph
            .neighbors_directed(*node, petgraph::Direction::Incoming)
        {
            let inbound_tag = &self.graph[inbound];
            inbounds.push(inbound_tag.clone());
        }

        inbounds
    }

    pub fn query_outbounds(&self, tag: &TagId) -> Vec<TagId> {
        let node = self.tag_2_idx.get(tag).expect("Tag not found in DAG");
        let mut outbounds = vec![];

        for outbound in self
            .graph
            .neighbors_directed(*node, petgraph::Direction::Outgoing)
        {
            let outbound_tag = &self.graph[outbound];
            outbounds.push(outbound_tag.clone());
        }

        outbounds
    }

    pub fn dump_to_dot(&self) {
        let graph = petgraph::dot::Dot::with_config(&self.graph, &[]);
        let graph = format!("{:?}", graph);
        std::fs::write("graph.dot", graph).expect("Unable to write file");
    }
}

// use std::collections::HashMap;

// use dashmap::DashMap;
// use log::{info, warn};
// use petgraph::{csr::DefaultIx, graph::NodeIndex, Direction, Graph};
// use tokio::sync::mpsc;

// use crate::{
//     config::{inbound::InboundConfig, pipe::PipeConfig, OutboundConfig},
//     core::{
//         tag::{HasTag, TagId},
//         types::Record,
//     },
// };

// pub const PIPE_CHANNEL_BUFFER_SIZE: usize = 64;

// #[derive(Debug)]
// pub struct ActorChannelGroup {
//     tag: TagId,

//     // The inbound channel is used to receive data from the previous pipe in the chain.
//     inbound_tx: mpsc::Sender<Record>,

//     // The outbound channel is used to send data to the next pipe in the chain.
//     outbound_tx: mpsc::Sender<Record>,

//     // There can only one receiver for the inbound channel
//     // Once taken, it will be None
//     outbound_rx: Option<mpsc::Receiver<Record>>,
//     inbound_rx: Option<mpsc::Receiver<Record>>,
// }

// pub struct ChannelGraph {
//     channels: DashMap<TagId, ActorChannelGroup>,

//     pipe_outputs: DashMap<TagId, Vec<mpsc::Sender<Record>>>,

//     inbound_outputs: DashMap<TagId, Vec<mpsc::Sender<Record>>>,

//     outbound_inputs: DashMap<TagId, Vec<mpsc::Receiver<Record>>>,
// }

// impl ChannelGraph {
//     pub fn try_create_from(
//         pipes: &[PipeConfig],
//         inbounds: &[InboundConfig],
//         outbonds: &[OutboundConfig],
//     ) -> super::Result<Self> {
//         let (nodes, dag) = Self::try_create_dag_from(pipes, inbounds, outbonds)?;

//         let channel_groups = DashMap::new();
//         for tag in pipes
//             .iter()
//             .map(HasTag::tag)
//             .chain(outbonds.iter().map(HasTag::tag))
//         {
//             if channel_groups.contains_key(tag) {
//                 panic!(
//                     "Duplicate pipe tag: {}, panic should not be happen here",
//                     tag
//                 );
//             }

//             let (inbound_tx, inbound_rx) = mpsc::channel(PIPE_CHANNEL_BUFFER_SIZE);
//             let (outbound_tx, outbound_rx) = mpsc::channel(PIPE_CHANNEL_BUFFER_SIZE);

//             let channel = ActorChannelGroup {
//                 tag: tag.clone(),
//                 inbound_tx,
//                 inbound_rx: Some(inbound_rx),
//                 outbound_tx,
//                 outbound_rx: Some(outbound_rx),
//             };

//             channel_groups.insert(tag.clone(), channel);
//         }

//         // Collecting output channels for pipes
//         let pipe_outputs = DashMap::new();
//         for pipe in pipes {
//             let mut out = vec![];
//             let tag = pipe.tag().clone();

//             let node = nodes.get(&tag).expect("Node not found in DAG for pipe");
//             for neighbour in dag.neighbors_directed(*node, Direction::Outgoing) {
//                 let neighbour_tag = &dag[neighbour];
//                 let neighbour_channels = channel_groups
//                     .get_mut(neighbour_tag)
//                     .expect("Channel not found in DAG for neighbour");
//                 out.push(neighbour_channels.inbound_tx.clone());
//             }

//             pipe_outputs.insert(tag, out);
//         }

//         // Collecting output channels for inbounds
//         let inbound_outputs = DashMap::new();
//         for inbound in inbounds {
//             let mut out = vec![];
//             let tag = inbound.tag().clone();

//             let node = nodes.get(&tag).expect("Node not found in DAG for inbound");
//             for neighbour in dag.neighbors_directed(*node, Direction::Outgoing) {
//                 let neighbour_tag = &dag[neighbour];
//                 let neighbour_channels = channel_groups
//                     .get_mut(neighbour_tag)
//                     .expect("Channel not found in DAG for neighbour");
//                 out.push(neighbour_channels.inbound_tx.clone());
//             }

//             inbound_outputs.insert(tag, out);
//         }

//         let outbound_inputs = DashMap::new();
//         // Collecting input channels for outbounds
//         for outbound in outbonds {
//             let mut in_ = vec![];
//             let tag = outbound.tag().clone();

//             let node = nodes.get(&tag).expect("Node not found in DAG for outbound");
//             for neighbour in dag.neighbors_directed(*node, Direction::Incoming) {
//                 let neighbour_tag = &dag[neighbour];
//                 let mut neighbour_channels = channel_groups
//                     .get_mut(neighbour_tag)
//                     .expect("Channel not found in DAG for neighbour");
//                 in_.push(
//                     neighbour_channels
//                         .outbound_rx
//                         .take()
//                         .expect("Channel already taken, this should never happen"),
//                 );
//             }

//             outbound_inputs.insert(tag, in_);
//         }

//         // Find unused actors
//         let mut unused_actors = vec![];
//         for (tag, node) in nodes.iter() {
//             if dag.neighbors_directed(*node, Direction::Incoming).count() == 0
//                 && dag.neighbors_directed(*node, Direction::Outgoing).count() == 0
//             {
//                 unused_actors.push(tag.clone());
//             }
//         }
//         warn!(
//             "Found unused actors: {:?}",
//             unused_actors
//                 .iter()
//                 .map(|tag| tag.to_string())
//                 .collect::<Vec<_>>()
//         );

//         Ok(ChannelGraph {
//             channels: channel_groups,
//             inbound_outputs,
//             pipe_outputs,
//             outbound_inputs,
//         })
//     }

//     pub fn recv_from(&self, tag: &TagId) -> mpsc::Receiver<Record> {
//         if !tag.is_inbound() && !tag.is_pipe() {
//             panic!("We can only recv from inbound or pipe, but got: {}", tag);
//         }

//         let mut channel = self
//             .channels
//             .get_mut(tag)
//             .expect("Channel not found in DAG");

//         channel
//             .inbound_rx
//             .take()
//             .expect("Channel already taken, this should never happen")
//     }

//     pub fn pipe_outputs(&self, tag: &TagId) -> Vec<mpsc::Sender<Record>> {
//         if !tag.is_pipe() {
//             panic!("Only pipe can take a pipe sender channel, but got: {}", tag);
//         }

//         let (_, outputs) = self
//             .pipe_outputs
//             .remove(tag)
//             .expect("Pipe outputs not found");

//         outputs
//     }

//     pub fn inbound_outputs(&self, tag: &TagId) -> Vec<mpsc::Sender<Record>> {
//         if !tag.is_inbound() {
//             panic!(
//                 "Only inbound can take a inbound sender channel, but got: {}",
//                 tag
//             );
//         }

//         let (_, outputs) = self
//             .inbound_outputs
//             .remove(tag)
//             .expect("Inbound outputs not found");

//         outputs
//     }

//     pub fn outbound_inputs(&self, tag: &TagId) -> Vec<mpsc::Receiver<Record>> {
//         if !tag.is_outbound() {
//             panic!(
//                 "Only outbound can take a outbound receiver channel, but got: {}",
//                 tag
//             );
//         }

//         let (_, outputs) = self
//             .outbound_inputs
//             .remove(tag)
//             .expect("Outbound inputs not found");

//         outputs
//     }

//     fn try_create_dag_from(
//         pipes: &[PipeConfig],
//         inbounds: &[InboundConfig],
//         outbonds: &[OutboundConfig],
//     ) -> super::Result<(
//         HashMap<TagId, NodeIndex<DefaultIx>>,
//         Graph<TagId, (), petgraph::Directed>,
//     )> {
//         let mut dag = Graph::<TagId, (), petgraph::Directed, DefaultIx>::new();

//         // Collect all tags from pipes, inbounds, and outbounds
//         let mut nodes = HashMap::new();
//         for tag in pipes
//             .iter()
//             .map(HasTag::tag)
//             .chain(inbounds.iter().map(HasTag::tag))
//             .chain(outbonds.iter().map(HasTag::tag))
//         {
//             if nodes.contains_key(tag) {
//                 panic!(
//                     "Duplicate tag found: {}, panic should not be happen here",
//                     tag
//                 );
//             }

//             nodes.insert(tag.clone(), dag.add_node(tag.clone()));
//         }

//         for (tag, inbounds) in pipes
//             .iter()
//             .map(|e| (e.tag().clone(), e.inbounds()))
//             .chain(outbonds.iter().map(|e| (e.tag().clone(), e.inbounds())))
//         {
//             let node = nodes.get(&tag).expect("Node not found in DAG");
//             for inbound in inbounds {
//                 if let Some(inbound_node) = nodes.get(&inbound) {
//                     info!("Found flow: {} -> {}", inbound, tag);
//                     dag.add_edge(*inbound_node, *node, ());
//                 } else {
//                     return Err(super::Error::UnknownTagRequired(inbound, tag));
//                 }
//             }
//         }

//         Ok((nodes, dag))
//     }
// }
