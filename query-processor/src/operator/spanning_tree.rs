use std::fmt::Debug;
use hashbrown::HashMap;

use crate::operator::{MinPQIndex};

use super::super::util::types::{HalfOpenInterval, HalfOpenTimeInterval, VertexStatePair};

use self::super::tree_node::TreeNode;

/// SpanningTree implementation based on the `S-PATH` algorithm in PVLDB Submission
/// Each tree stores all reachable vertices from a given root vertex and the associated automata state
/// It is backed by a MinPQIndex that stores each node and their expiry timestamp for efficient expiry processing
#[derive(Clone, Debug)]
pub struct SpanningTree {
    root_vertex: VertexStatePair,
    root_node: TreeNode,
    node_queue: MinPQIndex<VertexStatePair, TreeNode>,
}

impl SpanningTree {
    pub fn new(root: u64) -> Self {
        Self {
            root_vertex: (root, 0),
            root_node: TreeNode::new(root, 0, HalfOpenTimeInterval::ZERO, HalfOpenTimeInterval::ZERO, None),
            node_queue: MinPQIndex::default(),
        }
    }

    /// insert a new leaf node (vertex-state) pair
    pub fn add_vertex(&mut self, vertex: u64, state: u8, timestamp: HalfOpenTimeInterval, parent: VertexStatePair) -> &TreeNode {
        let node_timestamp = if self.root_vertex == parent {
            timestamp
        } else {
            HalfOpenTimeInterval::intersect(&timestamp, &self.get_vertex(parent).unwrap().get_interval())
        };

        // timestamp of the new vertex is the min between edge timestamp and parent timestamp
        let new_vertex = TreeNode::new(vertex, state, node_timestamp, timestamp, Some(parent));

        // insert into priority queue
        self.node_queue.push((vertex, state), new_vertex, node_timestamp.end);

        // update
        self.add_chilren(parent, (vertex, state));

        // return newly create node
        self.node_queue.get(&(vertex, state)).map(|(entry, _)| entry).unwrap()
    }

    /// helper function to add a new child to a given node
    fn add_chilren(&mut self, parent: VertexStatePair, child: VertexStatePair) {
        if parent == self.root_vertex {
            self.root_node.add_child(child)
        } else {
            self.node_queue.get_mut(&parent).map(|(entry, _)| entry).unwrap().add_child(child)
        }
    }

    /// Updates the parent node of an existing node. It first removes the node from the list of children of its old parent
    pub fn update_parent(&mut self, node: VertexStatePair, new_parent: VertexStatePair, edge_ts: HalfOpenTimeInterval) {
        // remove parent relationship with old parent
        let old_parent_node = self.node_queue.get_mut(&node).map(|(entry, _)| entry).unwrap().get_parent().unwrap();

        if old_parent_node == self.root_vertex {
            self.root_node.remove_child(node)
        } else {
            self.node_queue.get_mut(&old_parent_node).map(|(entry, _)| entry).unwrap().remove_child(node);
        }

        // create new parent relationship
        let new_parent_node = if new_parent == self.root_vertex {
            &mut self.root_node
        } else {
            self.node_queue.get_mut(&new_parent).map(|(entry, _)| entry).unwrap()
        };

        new_parent_node.add_child(node);
        self.node_queue.get_mut(&node).map(|(entry, _)| entry).unwrap().set_parent(new_parent, edge_ts);
    }

    /// returns a reference to tree node for the given vertex-state pair
    pub fn get_vertex(&self, pair: VertexStatePair) -> Option<&TreeNode> {
        self.node_queue.get(&pair).map(|(entry, _)| entry)
    }

    /// returns a mutable reference to the tree node for the given vertex-state pair
    pub fn get_vertex_mut(&mut self, pair: VertexStatePair) -> Option<&mut TreeNode> {
        self.node_queue.get_mut(&pair).map(|(entry, _)| entry)
    }

    /// return the root vertex of the tree
    pub fn get_root_vertex(&self) -> u64 {
        self.root_vertex.0
    }

    /// return the root vertex as `TreeNode`
    pub fn get_root_node(&self) -> &TreeNode {
        &self.root_node
    }

    /// returns true if given vertex-state pair is a node in the tree
    pub fn contains(&self, node: VertexStatePair) -> bool {
        self.node_queue.get(&node).is_some()
    }

    /// return true if tree does not have any nodes
    pub fn is_empty(&self) -> bool {
        self.node_queue.is_empty()
    }


    /// update the priority of a node in the expiry queue
    pub fn update_node_expiry(&mut self, node: VertexStatePair, new_timestamp: u64) {
        self.node_queue.change_priority(&node, new_timestamp);
    }

    /// return the current minimum timestamp living on this tree
    pub fn get_min_timestamp(&self) -> u64 {
        self.node_queue.peek().map_or(u64::MAX, |(_, _, expiry_timestamp)| expiry_timestamp)
    }

    /// removes a single node from the tree
    pub fn remove_node(&mut self, node: VertexStatePair) -> (VertexStatePair, HalfOpenTimeInterval) {
        // remove the node from the tree
        let tree_node = self.node_queue.remove(&node).map(|(entry, _)| entry).unwrap();

        // remove the node from its parent's children
        let parent_pair = tree_node.get_parent().unwrap();
        if let Some(parent) = self.node_queue.get_mut(&parent_pair).map(|(entry, _)| entry) {
            parent.remove_child(node);
        } else {
            self.root_node.remove_child(node);
        }

        (node, tree_node.get_interval())
    }

    /// performs expiry given a low_watermark
    /// return a Vec that contains all removed nodes and their timestamps as pairs of (VertexStatePair, u64)
    /// expiry relies on the underlying MinPQIndex to locate expired trees
    pub fn expiry(&mut self, low_watermark: u64) -> Vec<(VertexStatePair, HalfOpenTimeInterval)> {
        let mut removed_results = Vec::new();

        let mut expiry_candidates: HashMap<VertexStatePair, TreeNode> = HashMap::new();

        // check expired nodes until all expired nodes are traversed
        while let Some((_, tree_node, expiry_ts)) = self.node_queue.peek() {
            if expiry_ts > low_watermark {
                break;
            }

            // tree_entry to be removed
            let node: VertexStatePair = (tree_node.get_vertex(), tree_node.get_state());

            // remove the node from its parent's children
            if tree_node.get_parent().unwrap() == self.root_vertex {
                self.root_node.remove_child(node)
            } else {
                // first check previously if the parent is already in the expiry list
                if let Some(parent) = expiry_candidates.get_mut(&tree_node.get_parent().unwrap()) {
                    parent.remove_child(node);
                } else {
                    let parent_pair = tree_node.get_parent().unwrap();
                    self.node_queue.get_mut(&parent_pair).map(|(entry, _)| entry).unwrap().remove_child(node)
                }
            }

            // finally remove the tree node from node_queue
            if let Some((_, expired_node, _timestamp)) = self.node_queue.pop() {
                expiry_candidates.insert((expired_node.get_vertex(), expired_node.get_state()), expired_node);
            }
        }


        // remove each node from the index
        for (pair, tree_node) in expiry_candidates.into_iter() {
            removed_results.push((pair, tree_node.get_interval()))
        }

        removed_results
    }
}

