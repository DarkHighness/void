use log::info;
use petgraph::csr::DefaultIx;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use tokio::sync::broadcast;

use crate::config::global::{self};
use crate::utils::tracing::Direction;
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

#[derive(Debug, Clone)]
pub struct TaggedSender {
    tag: TagId,
    sender: broadcast::Sender<Record>,
}

impl TaggedSender {
    pub fn send(&mut self, record: Record) -> Result<usize, broadcast::error::SendError<Record>> {
        record.mark_timestamp(&self.tag, Direction::Outgoing);
        self.sender.send(record)
    }
}

#[derive(Debug)]
pub struct TaggedReceiver {
    tag: TagId,
    who: TagId,
    receiver: broadcast::Receiver<Record>,
}

impl TaggedReceiver {
    pub fn tag(&self) -> &TagId {
        &self.tag
    }

    pub async fn recv(&mut self) -> Result<Record, broadcast::error::RecvError> {
        let record = self.receiver.recv().await?;
        record.mark_timestamp(&self.who, Direction::Incoming);
        Ok(record)
    }

    pub fn try_recv(&mut self) -> Result<Record, broadcast::error::TryRecvError> {
        let record = self.receiver.try_recv()?;
        record.mark_timestamp(&self.who, Direction::Incoming);
        Ok(record)
    }
}

// impl Deref for TaggedSender {
//     type Target = broadcast::Sender<Record>;

//     fn deref(&self) -> &Self::Target {
//         &self.sender
//     }
// }

// impl Deref for TaggedReceiver {
//     type Target = broadcast::Receiver<Record>;

//     fn deref(&self) -> &Self::Target {
//         &self.receiver
//     }
// }

// impl DerefMut for TaggedReceiver {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.receiver
//     }
// }

// impl DerefMut for TaggedSender {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.sender
//     }
// }

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

impl ActorChannel {
    pub fn new(tag: TagId, factor: usize) -> Self {
        let cap = global::channel_buffer_size() * factor;
        let (sender, receiver) = broadcast::channel(cap);
        info!("Created channel {} with buffer size {}", tag, cap);

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

    pub fn receiver(&self, who: &TagId) -> TaggedReceiver {
        let receiver = self.receiver.resubscribe();
        TaggedReceiver {
            tag: self.tag.clone(),
            who: who.clone(),
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
            .filter(|e| !e.disabled())
            .map(|e| (e.tag().clone(), 1))
            .chain(
                pipes
                    .iter()
                    .filter(|e| !e.disabled())
                    .map(|e| (e.tag().clone(), e.channel_scale_factor())),
            )
            .chain(
                outbounds
                    .iter()
                    .filter(|e| !e.disabled())
                    .map(|e| (e.tag().clone(), e.channel_scale_factor())),
            )
            .collect::<Vec<_>>();

        let mut graph = petgraph::Graph::<TagId, (), petgraph::Directed, DefaultIx>::new();
        let mut tag_to_idx = HashMap::new();

        let mut channels = HashMap::new();
        for (tag, factor) in tags {
            if channels.contains_key(&tag) {
                return Err(super::Error::DuplicateTag(tag));
            }

            let node = graph.add_node(tag.clone());
            tag_to_idx.insert(tag.clone(), node);

            let channel = ActorChannel::new(tag.clone(), factor);
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

        channel.sender()
    }

    pub fn recv_from(&mut self, tag: &TagId, who: &TagId) -> TaggedReceiver {
        let channel = self.channels.get(tag).expect(&format!(
            "Channel not found in DAG, {} wants to receive from {}",
            who, tag,
        ));
        let receiver = channel.receiver(who);

        let src = self.tag_2_idx.get(tag).expect("Tag not found in DAG");
        let dst = self.tag_2_idx.get(who).expect("Tag not found in DAG");

        self.graph.add_edge(*src, *dst, ());
        info!("Found dataflow {} --> {}", tag, who);

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
