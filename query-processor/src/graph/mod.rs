use std::cmp::{min};
use std::hash::BuildHasherDefault;

use hashbrown::HashMap;
use hashers::fx_hash::FxHasher;



use crate::operator::{MinPQIndex};
use crate::query::automata::dfa::DFA;

use self::super::util::types::{HalfOpenInterval, HalfOpenTimeInterval, StateType, VertexStatePair, VertexType};

/// Helper struct to store forward/backward adjacency list of each graph node
#[derive(Clone, Debug)]
struct GraphNode {
    node: VertexType,
    outgoing_edges: HashMap<String, MinPQIndex<VertexType, u64>, BuildHasherDefault<FxHasher>>,
    incoming_edges: HashMap<String, MinPQIndex<VertexType, u64>, BuildHasherDefault<FxHasher>>,
}


impl GraphNode {
    fn new(vertex: VertexType) -> Self {
        Self { node: vertex, outgoing_edges: HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()), incoming_edges: HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()) }
    }

    fn get_outgoing_edges(&self, label: &str) -> impl Iterator<Item=(u64, u64, u64)> + '_ {
        self.outgoing_edges.get(label)
            .into_iter()
            .flat_map(|t| t.iter())
            .map(|(v, start, end)| (v, *start, end))
    }

    fn get_incoming_edges(&self, label: &str) -> impl Iterator<Item=(u64, u64, u64)> + '_ {
        self.incoming_edges.get(label)
            .into_iter()
            .flat_map(|t| t.iter())
            .map(|(v, start, end)| (v, *start, end))
    }

    fn get_outgoing_edges_larger_than(&self, label: &str, low_watermark: u64) -> impl Iterator<Item=(u64, u64, u64)> + '_ {
        self.outgoing_edges.get(label)
            .into_iter()
            .flat_map(|t| t.iter())
            .filter(move |(_, _start_ts, expiry_ts)| *expiry_ts > low_watermark)
            .map(|(v, start, end)| (v, *start, end))
    }

    fn add_incoming_neighbour(&mut self, label: String, neighbour: VertexType, interval: HalfOpenTimeInterval) -> bool {
        let edges = self.incoming_edges.entry(label).or_insert(MinPQIndex::default());
        let mut has_larger_expiry = true;
        if let Some((_start, expiry)) = edges.get(&neighbour) {
            if expiry >= interval.get_end() {
                has_larger_expiry = false;
            } else {
                edges.push(neighbour, interval.start, interval.end);
            }
        } else {
            edges.push(neighbour, interval.start, interval.end);
        }

        has_larger_expiry
    }

    fn add_outgoing_neighbour(&mut self, label: String, neighbour: VertexType, interval: HalfOpenTimeInterval) -> bool {
        let edges = self.outgoing_edges.entry(label).or_insert(MinPQIndex::default());
        let mut has_larger_expiry = true;
        if let Some((_start, expiry)) = edges.get(&neighbour) {
            if expiry >= interval.get_end() {
                has_larger_expiry = false;
            } else {
                edges.push(neighbour, interval.start, interval.end);
            }
        } else {
            edges.push(neighbour, interval.start, interval.end);
        }
        has_larger_expiry
    }

    /// removes all expired inedges of the given vertex
    fn remove_expired_inedges(&mut self, low_watermark: u64) -> u64 {
        // retain an entry if there are still edges after expiry
        self.incoming_edges.retain(move |_, sources| {
            // find expired edges
            while let Some((_, _, expiry_ts)) = sources.peek() {
                if expiry_ts > low_watermark {
                    break;
                }
                sources.pop();
            }

            //return false if the edge list empty so that it will be deleted from the hashmap
            !sources.is_empty()
        });

        // return the min timestamp
        self.incoming_edges.iter()
            .map(|(_, values)| values.peek().map_or(u64::MAX, |(_, _, expiry_ts)| expiry_ts))
            .min().unwrap_or(u64::MAX)
    }

    /// removes all expired outedges of the vertex
    fn remove_expired_outedges(&mut self, low_watermark: u64) -> u64 {
        // retain an entry if there are still edges after expiry
        self.outgoing_edges.retain(move |_, targets| {
            // find expired edges
            while let Some((_, _, expiry_ts)) = targets.peek() {
                if expiry_ts > low_watermark {
                    break;
                }
                targets.pop();
            }

            //return false if the edge list empty so that it will be deleted from the hashmap
            !targets.is_empty()
        });

        // return the min timestamp
        self.outgoing_edges.iter()
            .map(|(_, values)| values.peek().map_or(u64::MAX, |(_, _, expiry_ts)| expiry_ts))
            .min().unwrap_or(u64::MAX)
    }

    fn is_isolated(&self) -> bool {
        self.incoming_edges.is_empty() && self.outgoing_edges.is_empty()
    }
}

/// MinPQIndex backed adjacency list implementation to store the product graph
/// It transparently stores the structure of the product graph based on the given DFA
/// Each edge is associated with a validity interval, whose upper-end is used the priority in MinPQIndex
/// It allows quick look-ups to retrieve all neighbours of a given node and to traversel all expired edges
#[derive(Clone, Debug)]
pub struct Graph {
    node_index: MinPQIndex<VertexType, GraphNode>,
    query_automata: DFA,
}

impl Graph {
    pub fn new(query_automata: DFA) -> Self {
        Self {
            node_index: MinPQIndex::default(),
            query_automata: query_automata,
        }
    }

    pub fn get_query_automata(&self) -> &DFA {
        &self.query_automata
    }

    fn get_node(&self, vertex: VertexType) -> Option<&GraphNode> {
        self.node_index.get(&vertex).map(|(entry, _)| entry)
    }

    /// Get outgoing edges of a given vertex as (vertex-state) pairs
    pub fn get_outgoing_edges(&self, vertex: VertexType, state: StateType) -> impl Iterator<Item=(VertexStatePair, HalfOpenTimeInterval)> + '_ {
        // get all outdoing edges of given source state
        self.query_automata.get_outgoing_transitions(state).into_iter()
            .flat_map(move |(label, target_state)| {
                self.get_node(vertex).into_iter()
                    .flat_map(move |graph_node| graph_node.get_outgoing_edges(&label))
                    .map(move |(target_vertex, start, end)| ((target_vertex, target_state), HalfOpenTimeInterval::new(start, end)))
            })
    }

    /// get outgoing edges of a given vertex with expiry timestamp larger than the `low_watermark`
    pub fn get_outgoing_edges_larger_than(&self, vertex: VertexType, state: StateType, low_watermark: u64) -> impl Iterator<Item=(VertexStatePair, HalfOpenTimeInterval)> + '_ {
        // get all outdoing edges of given source state
        self.query_automata.get_outgoing_transitions(state).into_iter()
            .flat_map(move |(label, target_state)| {
                self.get_node(vertex).into_iter()
                    .flat_map(move |graph_node| graph_node.get_outgoing_edges_larger_than(&label, low_watermark))
                    .map(move |(target_vertex, start, end)| ((target_vertex, target_state), HalfOpenTimeInterval::new(start, end)))
            })
    }

    /// get incoming edges of a given vertex with expiry timestamp larger then the `low_watermark`
    pub fn get_incoming_edges(&self, vertex: VertexType, state: StateType) -> impl Iterator<Item=(VertexStatePair, HalfOpenTimeInterval)> + '_ {
        // get all outdoing edges of given source state
        self.query_automata.get_incoming_transitions(state).into_iter()
            .flat_map(move |(label, target_state)| {
                self.get_node(vertex).into_iter()
                    .flat_map(move |graph_node| graph_node.get_incoming_edges(&label))
                    .map(move |(target_vertex, start, end)| ((target_vertex, target_state), HalfOpenTimeInterval::new(start, end)))
            })
    }

    /// update underlying graph with given graph edge
    /// return true if it replaces an existing edge with a lower timestamp
    pub fn insert_edge(&mut self, source: VertexType, label: String, target: VertexType, interval: HalfOpenTimeInterval) -> bool {
        // set to false only if there is an existing edge with a lower existing timestamp
        let mut has_larger_expiry = true;

        // first obtain or create the source
        let new_expiry_ts = interval.get_end();

        // index lookup by source vertex
        if let Some((entry, _)) = self.node_index.get_mut(&source) {
            // simply return mutable entry if the node already exists
            has_larger_expiry = entry.add_outgoing_neighbour(label.clone(), target, interval);
            // update priority if new edge has a lower expiry timestamp
        } else {
            // otherwise create the entry
            let mut source_node = GraphNode::new(source);
            source_node.add_outgoing_neighbour(label.clone(), target, interval);
            self.node_index.push(source, source_node, interval.get_end());
        }
        // update priority only if it gets smaller
        self.node_index.try_decrease_priority(&source, new_expiry_ts);

        // index lookup by target vertex
        if let Some((entry, _)) = self.node_index.get_mut(&target) {
            // simply return mutable entry if the node already exists
            entry.add_incoming_neighbour(label.clone(), source, interval);
            // update priority if new edge has a lower expiry timestamp
        } else {
            // otherwise create the entry
            let mut target_node = GraphNode::new(target);
            target_node.add_incoming_neighbour(label.clone(), source, interval);
            self.node_index.push(target, target_node, interval.get_end());
        };
        // update priority only if it gets smaller
        self.node_index.try_decrease_priority(&target, new_expiry_ts);

        // indicate whether incoming edge has increased expiry timestamp of an existing edge
        has_larger_expiry
    }

    /// removes all edges that are older than the provided timestamp
    /// it does not require linear scan due to underlying MinPQIndex
    pub fn remove_edges(&mut self, low_watermark: u64) {
        // iterate over edges and update adjacency lists

        let mut expiry_candidates = Vec::new();

        while let Some((_, _, priority)) = self.node_index.peek() {
            if priority > low_watermark {
                break;
            }

            let (key, entry, _) = self.node_index.pop().unwrap();
            expiry_candidates.push((key, entry));
        }

        for (key, mut node) in expiry_candidates.into_iter() {
            let min_incoming_ts = node.remove_expired_inedges(low_watermark);
            let min_outgoing_ts = node.remove_expired_outedges(low_watermark);

            // if node still has neighbours, update the node index
            if !node.is_isolated() {
                self.node_index.push(key, node, min(min_outgoing_ts, min_incoming_ts));
            }
        }
    }
}
