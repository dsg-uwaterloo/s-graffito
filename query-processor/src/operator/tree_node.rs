use std::hash::BuildHasherDefault;

use hashbrown::HashSet;
use hashers::fx_hash::FxHasher;

use super::super::util::types::{HalfOpenTimeInterval, VertexStatePair};

/// Helper struct to represents `SpanningTree` nodes
/// Each node contains a validity interval, a parent point and a list of chilren pointers
#[derive(Clone, Debug)]
pub struct TreeNode {
    node: VertexStatePair,
    timestamp: HalfOpenTimeInterval,
    incoming_edge_ts: HalfOpenTimeInterval,
    parent: Option<VertexStatePair>,
    children: HashSet<VertexStatePair, BuildHasherDefault<FxHasher>>,
}

impl TreeNode {
    pub fn new(vertex: u64, state: u8, timestamp: HalfOpenTimeInterval, incoming_edge_ts: HalfOpenTimeInterval, parent: Option<VertexStatePair>) -> Self {
        match parent {
            Some(parent_node) => {
                Self { node: (vertex, state), timestamp, incoming_edge_ts, parent: Some(parent_node), children: HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default()) }
            }
            None => {
                Self { node: (vertex, state), timestamp, incoming_edge_ts, parent: None, children: HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default()) }
            }
        }
    }

    /// insert a new child as a leaf node
    pub fn add_child(&mut self, child: VertexStatePair) {
        self.children.insert(child);
    }

    /// remove the children pointer for the given node
    pub fn remove_child(&mut self, child: VertexStatePair) {
        self.children.remove(&child);
    }

    /// returns an iterator over node's children
    pub fn get_children<'a>(&'a self) -> impl Iterator<Item=&VertexStatePair> + 'a {
        self.children.iter()
    }

    pub fn get_vertex(&self) -> u64 {
        self.node.0
    }

    pub fn get_state(&self) -> u8 {
        self.node.1
    }

    pub fn get_interval(&self) -> HalfOpenTimeInterval {
        self.timestamp
    }

    pub fn set_interval(&mut self, new_timestamp: HalfOpenTimeInterval) {
        self.timestamp = new_timestamp;
    }

    pub fn get_expiry_timestamp(&self) -> u64 {
        self.timestamp.end
    }

    pub fn get_parent(&self) -> Option<VertexStatePair> {
        self.parent
    }

    pub fn get_incoming_edge_ts(&self) -> HalfOpenTimeInterval {
        self.incoming_edge_ts
    }

    /// Update parent pointer and the incoming edge timestamp
    pub fn set_parent(&mut self, parent: VertexStatePair, edge_ts: HalfOpenTimeInterval) {
        self.parent = Some(parent);
        self.incoming_edge_ts = edge_ts;
    }
}
