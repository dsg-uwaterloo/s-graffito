extern crate timely;

use std::collections::VecDeque;
use std::hash::BuildHasherDefault;

use hashbrown::{HashMap, HashSet};
use hashers::fx_hash::FxHasher;
use log::{debug, trace};

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;

use crate::graph::Graph;
use crate::input::{GraphEdge, SGT, StreamingGraphEdge};
use crate::input::tuple::StreamingGraphTuple;
use crate::operator::{delta::Delta, MinPQIndex, spanning_tree::SpanningTree};

use crate::query::parser::RPQParser;

use self::super::super::util::types::{HalfOpenInterval, HalfOpenTimeInterval, VertexStatePair, VertexType};

/// Implementation of the `S-PATH` algorithm from PVLDB submission asa TD operator
/// It creates the minimal DFA for the given RPQ
/// It uses TD progress tracking mechanism to be notified about completed timestamps
pub trait RegularPathQuery<G: Scope<Timestamp=u64>, D: Data + SGT<HalfOpenTimeInterval, StreamingGraphEdge>> {
    /// Incremental RPQ evaluation on the given streams based on the provided RPQ `query_str`
    /// Resulting tuples carry the provided label `output_label`
    fn regular_path_query(&self, query_str: &str, output_label: String) -> Stream<G, StreamingGraphTuple>;
}

impl<G: Scope<Timestamp=u64>> RegularPathQuery<G, StreamingGraphTuple> for Stream<G, StreamingGraphTuple> {
    fn regular_path_query(&self, query_str: &str, output_label: String) -> Stream<G, StreamingGraphTuple> {
        let mut vector = Vec::new();

        // Min PQ based index to store spanning trees organized by their expiry timestamp
        let mut delta_node_index: HashMap<VertexStatePair, HashSet<u64, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>> = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());
        // invertex index for fast lookups
        let mut delta_tree_queue: MinPQIndex<VertexType, SpanningTree> = MinPQIndex::default();

        //create minimal DFA for the given regular expression
        let rpq_parser = RPQParser::new();
        let minimized_dfa = rpq_parser.parse_rpq(query_str);

        if minimized_dfa.is_err() {
            panic!("CANNOT create DFA from given RPQ {}", query_str);
        }

        // adjacency list index to store tuples in the window (i.e., snapshot graph)
        let mut graph = Graph::new(minimized_dfa.unwrap());

        // stash to collect tuples until progress notification
        let mut stash = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());

        // TODO: change communication pact for distributed setup
        self.unary_notify(Pipeline, "WindowedReachability", vec![], move |input, output, notificator| {
            // stash incoming tuples for processing after expiry

            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                let time_index = stash.entry(time.time().clone()).or_insert_with(|| HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()));

                for sgt in vector.drain(..) {
                    let tuple_key = (sgt.get_source(), sgt.get_target(), sgt.get_label().to_string());
                    let tuple_interval = sgt.get_interval();

                    // simply stash the tuple, keep max expiry for each value equivelant tuple
                    time_index.entry(tuple_key).and_modify(|current_interval: &mut HalfOpenTimeInterval| {
                        if current_interval.get_end() < tuple_interval.get_end() {
                            *current_interval = tuple_interval;
                        }
                    }).or_insert(tuple_interval);
                }

                notificator.notify_at(time.retain());
            }

            // process tuples once TD notifies about a completion of a timestamp
            // first clean-up the expired state based on the completed time
            // then retrieve the data from stash, update graph and perform expansion
            notificator.for_each(|time, _, _| {
                let mut session = output.session(&time);
                // perform expiry based on the completed timestamp
                let low_watermark = *time.time();
                debug!("Expiry for timestamp <= {:?}", low_watermark);

                // update the graph
                graph.remove_edges(low_watermark);

                // collect all expired tree based on the low watermark
                let expired_trees: Vec<SpanningTree> = Delta::get_expired_trees(&mut delta_tree_queue, low_watermark).collect();

                // process expired trees:
                // 1. delete all expired nodes
                // 2. compute the min expiry timestamp of remaining nodes
                // 3. update trees expiry timestamp, or remove if there is no node remaining
                expired_trees.into_iter().for_each(|mut tree| {
                    let tree_root = tree.get_root_vertex();
                    let removed_nodes = tree.expiry(low_watermark);
                    // expiry requires differentiated treatment for NT approach
                    //     match approach {
                    //     OperationType::Direct => tree.expiry(low_watermark),
                    //     OperationType::NegativeTuple => {
                    //         if tree_expiry_derivation(&mut tree, &mut graph, low_watermark) {
                    //             tree.expiry(low_watermark)
                    //         } else {
                    //             Vec::new()
                    //         }
                    //     }
                    // };

                    removed_nodes.iter().for_each(|(to, _expiry_ts)| {
                        // clear up node index
                        Delta::remove_from_node_index(&mut delta_node_index, to.0, to.1, tree_root);
                    });

                    if tree.is_empty() {
                        // tree needs to be removed from Delta indexes
                        Delta::remove_spanning_tree(&mut delta_node_index, tree);
                    } else {
                        // get updated min timestamp
                        let tree_min_ts = tree.get_min_timestamp();

                        // tree need to be placed back into the pq index if it is not empty, so create a new key
                        delta_tree_queue.push(tree_root, tree, tree_min_ts);
                    }
                });

                // temp data structure to maintain tuples that will be used for expansion
                let mut tuple_to_process = Vec::new();
                // get input data from stash based on completed timestamp
                if let Some(mut time_index) = stash.remove(&time.time()) {
                    // update the graph and flag it for processing in they create larger expiry
                    for ((source, target, label), interval) in time_index.drain() {
                        let has_larger_expiry = graph.insert_edge(source, label.clone(), target, interval);
                        // no need to process the tuple it maps to an existing tuple with already higher expiry timestamp
                        if has_larger_expiry {
                            tuple_to_process.push(((source, target, label), interval));
                        }
                    }
                }

                // finally perform expansion on Delta for tuples who either are new in the graph, or increase expiry timestamp of existing tuples
                for ((source, target, label), interval) in tuple_to_process.drain(..) {
                    debug!("Processing sgt {:?}", (source, target, &label, interval));
                    // iterate over each transition with the given label
                    let transitions: Vec<(u8, u8)> = graph.get_query_automata().get_transitions(&label);
                    transitions.into_iter().for_each(|(source_state, target_state)| {
                        debug!("Transition from {}-{} to {}-{} @ {}", source, source_state, target, target_state, interval);

                        // create a spanning tree rooted at source if it does not exists
                        if source_state == 0 && !Delta::contains(&delta_tree_queue, &source) {
                            Delta::add_spanning_tree(&mut delta_node_index, &mut delta_tree_queue, source);
                            debug!("Adding spanning tree rooted @ {:?}", source)
                        }

                        // invertex-index look-up to find trees that contains the source target-state pair
                        let updateable_trees: Vec<u64> = Delta::get_updatable_trees(&delta_node_index, source, source_state).collect();

                        // expand trees that have the source vertex,  but not the target vertex
                        updateable_trees.into_iter().for_each(|tree_root| {
                            // then insert the target node as a new leaf
                            let mut tree = Delta::get_tree_mut(&mut delta_tree_queue, &tree_root).unwrap();

                            let reachability_results = tree_expand(&mut tree, &mut graph, source, source_state, target, target_state, interval);
                            for (to, node_interval) in reachability_results {
                                if graph.get_query_automata().is_final_state(to.1) {
                                    // construct a resulting sgt
                                    session.give(
                                        StreamingGraphTuple::new(tree_root, to.0, output_label.clone(), node_interval)
                                    );
                                }
                                Delta::insert_into_node_index(&mut delta_node_index, to.0, to.1, tree_root);
                            }
                            // get trees updated min timestamp
                            let tree_min_ts = tree.get_min_timestamp();
                            // update tree's priority based on the new timestamp
                            Delta::update_tree_expiry(&mut delta_tree_queue, &tree_root, tree_min_ts);
                        });
                    });
                }
            });
        })
    }
}


/// Performs expansion on a given SpanningTree by traversing the graph
/// returns new reachability results in form of a vector of triples (to, from, ts)
/// If the target node (vertex-state pair) is not in the tree, create new leaf
/// If the target node already exists, check its expiry timestamp. If the new path leading to larger expiry
/// propagate changes. Otherwise, stop traversal
fn tree_expand(tree: &mut SpanningTree, graph: &mut Graph, source_vertex: u64, source_state: u8, target_vertex: u64, target_state: u8, edge_ts: HalfOpenTimeInterval) -> Vec<(VertexStatePair, HalfOpenTimeInterval)> {
    // collect results
    let mut reachability_results = Vec::new();

    let root_vertex = tree.get_root_vertex();

    let mut queue = VecDeque::new();
    queue.push_back(((source_vertex, source_state), (target_vertex, target_state), edge_ts));

    while !queue.is_empty() {
        let (node, child, child_ts) = queue.pop_front().unwrap();
        // expand only of parent interval overlaps with incoming edge, or it is a root
        // interval of a node that is direct child of the root is simply the edge (sgt) interval
        if ((tree.get_root_vertex(), 0) == node) | tree.get_vertex(node).map_or(false, |v| v.get_interval().overlaps(&child_ts)) {
            // expand child nodes that are not reachable and have overlapping timestamp
            if !tree.contains(child) {
                // it does not exists, so add it to the tree
                // decide expiry timestamp for the new node
                let child_node = tree.add_vertex(child.0, child.1, child_ts, node);
                reachability_results.push((child, child_node.get_interval()));

                trace!("Node {:?} created at tree {} with parent {:?} @ {}", child, root_vertex, node, child_node.get_interval());


                // add children of this node as potential extensions
                trace!("Check all outgoing edges of {:?} for expansion of tree {}", child, root_vertex);
                let neighbours = graph.get_outgoing_edges(child.0, child.1);
                neighbours.filter(|(_, interval)| child_node.get_interval().overlaps(interval)).for_each(|((v, s), interval)| queue.push_back((child, (v, s), interval)));
            } else {
                // TODO: following block is not necessary for NT, only Direct approach should check whether an incoming tuple increase expiry
                // the target node is already in the tree, check its expiry timestamp
                // parent node might be the root node

                let new_interval = if (root_vertex, 0) == node {
                    child_ts
                } else {
                    HalfOpenTimeInterval::intersect(&child_ts, &tree.get_vertex(node).unwrap().get_interval())
                };

                let child_node = tree.get_vertex(child).unwrap();

                trace!("Tree {} node {:?} is alternative parent for {:?} with timestamp {} compared to existing one {}", root_vertex, node, child, new_interval, child_node.get_interval());

                // propagate if the the new path has a larger expiry timestamp, and the target is not root
                if child_node.get_expiry_timestamp() < new_interval.end {
                    trace!("Tree {} node {} is set as parent for {}", root_vertex, node.0, child.0);

                    let old_expiry_timestamp = child_node.get_expiry_timestamp();

                    // update its parent
                    tree.update_parent(child, node, child_ts);

                    // update its timestamp
                    tree.get_vertex_mut(child).unwrap().set_interval(new_interval);
                    tree.update_node_expiry(child, new_interval.end);


                    trace!("New timestamp for node {:?} at tree {} is {}", child, root_vertex, new_interval);

                    // finally push back to queue so that its children are processed as well
                    reachability_results.push((child, new_interval));

                    // add children of this node as potential extensions
                    //                let neighbours = graph.get_outgoing_edges(child.0, child.1);
                    let neighbours = graph.get_outgoing_edges_larger_than(child.0, child.1, old_expiry_timestamp);
                    neighbours.for_each(|((v, s), interval)| queue.push_back((child, (v, s), interval)));
                }
            }
        }
    }

    return reachability_results;
}

/// Finds expired nodes in the spanning tree, and searches for alternative derivation
/// if alternative derivation exists, update its parent and its timestamp
/// return true if not all expired nodes have alternative derivations, i.e., there are nodes requiring clean-up
/// * it assumes that all edges in the product graph are valid
/// This function lazily maintains tree invariant: after each invocation, a node that does not have valid derivation is guarenteed to be expired
#[allow(dead_code)]
fn tree_expiry_derivation(tree: &mut SpanningTree, graph: &mut Graph, low_watermark: u64) -> bool {
    let children = tree.get_root_node().get_children();

    let mut expiry_candidates = Vec::new();
    // let mut updated_candidates = HashSet::new();

    let mut queue = VecDeque::new();
    children.for_each(|node| queue.push_back(*node));

    // traverse the tree to find expired nodes
    while let Some(node) = queue.pop_front() {
        let tree_node = tree.get_vertex(node).unwrap();
        // visit children
        tree_node.get_children().for_each(|child| queue.push_back(*child));

        // mark if node is expired
        if tree_node.get_expiry_timestamp() <= low_watermark {
            expiry_candidates.push(node);
        }
    }

    let mut updated_nodes = Vec::new();

    // find a valid incoming edge for each expired node, if found
    // 1. update parent pointer and timestamp based on the new valid incoming link
    // 2. traverse down the tree and propagate timestamp changes to nodes in the subtree
    expiry_candidates.iter().for_each(|expired_node| {
        // find altenrative derivation only if no other search finds
        if tree.get_vertex(*expired_node).unwrap().get_expiry_timestamp() <= low_watermark {
            // get all valid incoming edges to expired node
            let mut backward_edges = graph.get_incoming_edges(expired_node.0, expired_node.1);

            // find if there is an incoming edge where the source is in the tree and not expired
            let candidate_parent = backward_edges
                .find(|&((source_vertex, source_state), _)| {
                    tree.contains((source_vertex, source_state)) && tree.get_vertex((source_vertex, source_state)).unwrap().get_expiry_timestamp() > low_watermark
                });

            // traverse the subtree if there is a valid parent
            if let Some((new_parent, incoming_edge_interval)) = candidate_parent {
                let new_parent_node = tree.get_vertex(new_parent).unwrap();
                // find min timestamp for the node
                let new_timestamp = HalfOpenTimeInterval::intersect(&new_parent_node.get_interval(), &incoming_edge_interval);
                trace!("Candidate found for {} in {} @ {} with parent {}", expired_node.0, tree.get_root_vertex(), new_timestamp, new_parent.0);

                //update its parent
                tree.update_parent(*expired_node, new_parent, incoming_edge_interval);

                // update its timestamp
                tree.get_vertex_mut(*expired_node).unwrap().set_interval(new_timestamp);
                tree.update_node_expiry(*expired_node, new_timestamp.end);
                updated_nodes.push(*expired_node);

                // traverse its subtree to update timestamps
                let mut child_queue = VecDeque::new();
                tree.get_vertex(*expired_node).unwrap().get_children().for_each(|child_node| child_queue.push_back((*child_node, new_timestamp)));

                while let Some((child_node, parent_timestamp)) = child_queue.pop_front() {
                    let child_tree_node = tree.get_vertex(child_node).unwrap();
                    let child_timestamp = child_tree_node.get_interval();
                    let incoming_edge_ts = child_tree_node.get_incoming_edge_ts();

                    let new_child_timestamp = HalfOpenTimeInterval::intersect(&incoming_edge_ts, &parent_timestamp);

                    // continue propoagation if child node timestamp is updated
                    if child_timestamp.end < new_child_timestamp.end {
                        tree.get_vertex_mut(child_node).unwrap().set_interval(new_child_timestamp);
                        tree.update_node_expiry(child_node, new_child_timestamp.end);
                        updated_nodes.push(child_node);

                        tree.get_vertex(child_node).unwrap().get_children().for_each(|child_node| child_queue.push_back((*child_node, new_child_timestamp)));
                    }
                }
            }
        }
    });


    // if there is still nodes to be deleted, return true so that tree.expiry can handle it
    expiry_candidates.len() > updated_nodes.len()
}

