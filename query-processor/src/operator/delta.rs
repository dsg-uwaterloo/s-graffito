

use std::hash::BuildHasherDefault;

use hashbrown::{HashMap, HashSet};
use hashers::fx_hash::FxHasher;
use log::trace;


use crate::operator::MinPQIndex;
use crate::operator::spanning_tree::SpanningTree;
use crate::util::types::VertexType;

use self::super::super::util::types::VertexStatePair;

/// Implementation of Delta Index from PVLDB Submission
/// It organizes a collection of spanning trees in a MinPQIndex based on
/// the lowest expiry timestamp of modes in a given tree.
/// It enables fast access to expired trees
/// Additionally, it uses a hash-based invertex index from nodes to trees for fast membership test
#[derive(Clone, Debug)]
pub struct Delta;

impl Delta {
    /// return `true` if there exists a tree rooted at the given vertex
    pub fn contains(tree_queue: &MinPQIndex<VertexType, SpanningTree>, vertex: &VertexType) -> bool {
        tree_queue.get(vertex).is_some()
    }

    /// inserts a new spanning tree rooted at the given vertex
    pub fn add_spanning_tree(node_index: &mut HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, tree_queue: &mut MinPQIndex<VertexType, SpanningTree>, vertex: u64) {
        tree_queue.push(vertex, SpanningTree::new(vertex), u64::MAX);
        Delta::insert_into_node_index(node_index, vertex, 0, vertex);
    }

    /// return a reference to the spanning tree rooted at the given vertex
    pub fn get_tree<'a>(tree_queue: &'a MinPQIndex<VertexType, SpanningTree>, vertex: &VertexType) -> Option<&'a SpanningTree> {
        tree_queue.get(vertex).map(|(entry, _)| entry)
    }

    /// returns a mutable refernce to the spanning tree rooted at the given vertex
    pub fn get_tree_mut<'a>(tree_queue: &'a mut MinPQIndex<VertexType, SpanningTree>, vertex: &VertexType) -> Option<&'a mut SpanningTree> {
        tree_queue.get_mut(vertex).map(|(entry, _)| entry)
    }

    /// removes a spanning tree from indices
    /// assumes that tree exists and only has root node (single node), panics otherwise
    pub fn remove_spanning_tree(node_index: &mut HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, tree: SpanningTree) {
        let tree_root = tree.get_root_vertex();
        if tree.is_empty() {
            Delta::remove_from_node_index(node_index, tree_root, 0, tree_root);
            trace!("Spanning tree rooted at {} is removed during window management", tree_root)
        } else {
            panic!("Spanning tree rooted at {} should not be removed as it is not empty", tree_root)
        }
    }


    /// updates the priority of the given tree in MinPQIndex.
    /// CAUTION: It does not check if the supplied min_timestamp is indeed the min expiry of the given tree.
    pub fn update_tree_expiry(tree_queue: &mut MinPQIndex<VertexType, SpanningTree>, vertex: &VertexType, min_timestamp: u64) {
        tree_queue.change_priority(vertex, min_timestamp);
    }


    /// Returns trees that are updatable by the given edge, satisfying conditions
    /// 1. The source node exists in the tree
    /// 2. Target node may or may not exists
    pub fn get_updatable_trees(node_index: &HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, source_vertex: u64, source_state: u8) -> impl Iterator<Item=u64> + '_ {
        Delta::get_containing_trees(node_index, source_vertex, source_state)
    }

    /// returns trees that have expired node, requiring window maintenance
    /// requires re-insertion if trees still have valid nodes
    pub fn get_expired_trees<'a>(tree_queue: &'a mut MinPQIndex<VertexType, SpanningTree>, low_watermark: u64) -> impl Iterator<Item=SpanningTree> + 'a {
        let mut expired_trees = Vec::new();

        // check expired trees until all expired trees are traversed
        while let Some((_, _tree_root, timestamp)) = tree_queue.peek() {
            if timestamp > low_watermark {
                break;
            }

            if let Some((_, tree_entry, _timestamp)) = tree_queue.pop() {
                expired_trees.push(tree_entry);
            }
        }

        // push back the last element as it was not supposed to be removed
        expired_trees.into_iter()
    }

    /// Uses the inverted index to look-up trees that contains the given vertex-state pair
    /// Returns an iterator of root vertices
    fn get_containing_trees(node_index: &HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, vertex: u64, state: u8) -> impl Iterator<Item=u64> + '_ {
        node_index.get(&(vertex, state)).into_iter().cloned().flat_map(|v| v)
    }

    /// update the inverted index for newly added nodes of a given tree
    pub fn insert_into_node_index(node_index: &mut HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, vertex: u64, state: u8, tree_root: u64) {
        let containing_trees = node_index.entry((vertex, state)).or_insert(HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default()));
        containing_trees.insert(tree_root);
    }

    /// update the invertex index for a node that is removed from a tree
    pub fn remove_from_node_index(node_index: &mut HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>, vertex: u64, state: u8, tree_root: u64) {
        let containing_trees = node_index.entry((vertex, state)).or_insert(HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default()));
        containing_trees.remove(&tree_root);

        // check whether if there is any containing tree for the node
        if containing_trees.is_empty() {
            node_index.remove(&(vertex, state));
        }
    }
}
