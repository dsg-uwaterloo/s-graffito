use std::collections::HashSet;

use differential_dataflow::{AsCollection, Collection};
use differential_dataflow::operators::*;
use log::trace;
use timely::communication::allocator::Generic;
use timely::dataflow::operators::{Concat, Partition, Filter, Inspect};
use timely::dataflow::scopes::Child;
use timely::dataflow::Stream;
use timely::worker::Worker;

use crate::input::{GraphEdge, SGT, StreamingGraphEdge};
use crate::input::tuple::StreamingGraphTuple;
use crate::operator::hash_join::{HashJoinAttributePair, SymmetricHashJoin};
use crate::operator::rpq::RegularPathQuery;
use crate::util::types::HalfOpenTimeInterval;

use self::super::automata::nfa::NFA;

pub struct RPQLibrary;

impl RPQLibrary {
    pub fn tc_one_or_more(label: String) -> NFA {
        let mut final_states = HashSet::new();
        final_states.insert(1);
        let mut automata = NFA::new(2, final_states);
        automata.add_transition(0, 1, label.clone());
        automata.add_transition(1, 1, label);

        trace!("Constructed automata {:?}", automata);

        automata
    }

    pub fn tc_zero_or_more(label: String) -> NFA {
        let mut final_states = HashSet::new();
        final_states.insert(0);
        let mut automata = NFA::new(1, final_states);
        automata.add_transition(0, 0, label);

        trace!("Constructed automata {:?}", automata);

        automata
    }

    pub fn maze_query7(label1: String, label2: String, label3: String) -> NFA {
        let mut final_states = HashSet::new();
        final_states.insert(2);
        let mut automata = NFA::new(3, final_states);
        automata.add_transition(0, 1, label1);
        automata.add_transition(1, 2, label2);
        automata.add_transition(2, 2, label3);

        automata
    }
}

/// Pre-constructed DD dataflows for queries in SGA paper (Table 1)
pub struct DDQueryLibrary;

impl DDQueryLibrary {
    pub fn hash_join<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 2);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(2, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else {
                    (1, sgt)
                }
            });

        let s1 = streams[0].as_collection();
        let s2 = streams[1].as_collection();

        // Create SGT from results with default interval
        s1.map(|sgt| (sgt.target, sgt.source))
            .join(&s2.map(|sgt| (sgt.source, sgt.target)))
            .map(|(_key, (s1, t2))| (s1, t2)).distinct()
            .map(move |(s1, t2)| StreamingGraphTuple::new(s1, t2, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query1<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 1);
        let edges = input.map(|sge| (sge.get_source(), sge.get_target()));
        let reachability = edges
            .iterate(|transitive| {
                let edges = edges.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&edges)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&edges)
                    .distinct()
            });

        // construct sgts from reachable pairs
        reachability.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query2<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 2);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(2, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else {
                    (1, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_target(), sgt.get_source()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let s1_closure = s1
            .iterate(|transitive| {
                let s1 = s1.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&s1)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&s1)
                    .distinct()
            });

        let results = s0
            .join(&s1_closure)
            .map(|(_key, (s1, t2))| (s1, t2)).distinct();

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query3<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_target(), sgt.get_source()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let s1_closure = s1
            .iterate(|transitive| {
                let s1 = s1.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&s1)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&s1)
                    .distinct()
            });

        let s2_closure = s2
            .iterate(|transitive| {
                let s2 = s2.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&s2)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&s2)
                    .distinct()
            });

        let results = s0
            .join(&s1_closure)
            .map(|(_key, (s1, t2))| (t2, s1)).distinct()
            .join(&s2_closure)
            .map(|(_key, (s1, t3))| (s1, t3)).distinct();


        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query4<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_target(), sgt.get_source()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let cq = s0
            .join(&s1)
            .map(|(_key, (s1, t2))| (t2, s1)).distinct()
            .join(&s2)
            .map(|(_key, (s1, t3))| (s1, t3)).distinct();

        // obtain transitive closure over the subgraph pattern
        let results = cq
            .iterate(|transitive| {
                let cq = cq.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&cq)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&cq)
                    .distinct()
            });

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query5<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let stream0 = streams[0].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let stream1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let stream2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let results = stream1
            .map(|(s1, t1)| (t1, s1))
            .join(&stream0)
            .map(|(_key, (s1, t0))| (t0, s1)).distinct()
            .join(&stream1.map(|(s3, t3)| (t3, s3)))
            .map(|(_key, (s1, s3))| (s1, s3)).distinct()
            .join(&stream2.map(|(s2, t2)| (t2, s2)))
            .filter(|(_key, (s3, t2))| s3 == t2)
            .map(|(key, (s3, _t2))| (key, s3)).distinct();


        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query6<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        //get transitive closure of the first stream
        // get transive closure of knows
        let closure1 = s0
            .iterate(|transitive| {
                let edges = s0.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&edges)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&edges)
                    .distinct()
            });

        let results = s2
            .join(&s1.map(|(s, t)| (t, s)))
            .map(|(_key, (hc_t, l_s))| (l_s, hc_t)).distinct()
            .join(&closure1)
            .filter(|(_key, (hc_t, k_t))| hc_t == k_t)
            .map(|(k_s, (_, k_t))| (k_s, k_t)).distinct();

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query6_cq<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let results = s2
            .join(&s1.map(|(s, t)| (t, s)))
            .map(|(_key, (hc_t, l_s))| (l_s, hc_t)).distinct()
            .join(&s0)
            .filter(|(_key, (hc_t, k_t))| hc_t == k_t)
            .map(|(k_s, (_, k_t))| (k_s, k_t)).distinct();

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query7<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        //get transitive closure of the first stream
        // get transive closure of knows
        let closure1 = s0
            .iterate(|transitive| {
                let edges = s0.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&edges)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&edges)
                    .distinct()
            });

        let cq = s2
            .join(&s1.map(|(s, t)| (t, s)))
            .map(|(_key, (hc_t, l_s))| (l_s, hc_t)).distinct()
            .join(&closure1)
            .filter(|(_key, (hc_t, k_t))| hc_t == k_t)
            .map(|(k_s, (_, k_t))| (k_s, k_t)).distinct();

        // obtain transitive closure over the subgraph pattern
        let t = cq
            .iterate(|transitive| {
                let cq = cq.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&cq)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&cq)
                    .distinct()
            });

        // join with last `c` edge
        let results = t.map(|(s, t)| (t, s))
            .join(&s2.map(|(s, t)| (t, s)))
            .map(|(_key, (t_s, s2_s))| (t_s, s2_s)).distinct();

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query7_cq<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 3);
        // logical partitioning based on labels
        let streams = input
            .inner
            .partition(3, move |sgt| {
                if sgt.0.get_label() == &edge_predicates[0] {
                    (0, sgt)
                } else if sgt.0.get_label() == &edge_predicates[1] {
                    (1, sgt)
                } else {
                    (2, sgt)
                }
            });

        let s0 = streams[0].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s1 = streams[1].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));
        let s2 = streams[2].as_collection().map(|sgt| (sgt.get_source(), sgt.get_target()));

        let cq = s2
            .join(&s1.map(|(s, t)| (t, s)))
            .map(|(_key, (hc_t, l_s))| (l_s, hc_t)).distinct()
            .join(&s0)
            .filter(|(_key, (hc_t, k_t))| hc_t == k_t)
            .map(|(k_s, (_, k_t))| (k_s, k_t)).distinct();

        // obtain transitive closure over the subgraph pattern
        let t = cq
            .iterate(|transitive| {
                let cq = cq.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&cq)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&cq)
                    .distinct()
            });

        // join with last `c` edge
        let results = t.map(|(s, t)| (t, s))
            .join(&s2.map(|(s, t)| (t, s)))
            .map(|(_key, (t_s, s2_s))| (t_s, s2_s)).distinct();

        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }

    pub fn query8<'a>(input: Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphEdge, isize>, edge_predicates: Vec<String>, output_label: String) -> Collection<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple, isize> {
        assert_eq!(edge_predicates.len(), 1);

        let stream = input.map(|sge| (sge.get_target(), sge.get_source()));

        let cq = stream
            .join(&stream)
            .filter(|(_key, (s0, s1))| s0 != s1)
            .map(|(_key, (s0, s1))| (s0, s1)).distinct();

        // obtain transitive closure over the subgraph pattern
        let results = cq
            .iterate(|transitive| {
                let cq = cq.enter(&transitive.scope());
                transitive
                    .map(|(s, t)| (t, s))
                    .join(&cq)
                    .map(|(_key, (s1, t2))| (s1, t2))
                    .concat(&cq)
                    .distinct()
            });
        // construct sgts for reachable pairs
        results.map(move |(s, t)| StreamingGraphTuple::new(s, t, output_label.clone(), HalfOpenTimeInterval::ZERO))
    }
}


/// Pre-constructed SGA dataflows for queries in SGA paper (Table1)
pub struct SGAQueryLibrary;

impl SGAQueryLibrary {
    pub fn hash_join<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 2);
        // logical partitioning based on labels
        // split streams based on edge predicates
        let streams = input.partition(2, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else {
                (1, sgt)
            }
        });

        streams[0].hash_join(
            &streams[1],
            HashJoinAttributePair::TS,
            HashJoinAttributePair::ST,
            output_label,
        )
    }

    pub fn query1<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 1);
        // create RPQ string
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("*");

        input.regular_path_query(&query_string, output_label)
    }

    pub fn query2<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 2);

        let query_predicates = edge_predicates.clone();
        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else {
                (1, sgt)
            }
        });

        // create RPQ string
        let mut query_string = String::from(&query_predicates[1]);
        query_string.push_str("*");

        let closure = streams[1].regular_path_query(&query_string, "cq".to_string());

        streams[0].hash_join(&closure, HashJoinAttributePair::TS, HashJoinAttributePair::ST, output_label)
    }

    pub fn query2_a<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 2);
        // create RPQ string
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("/");
        query_string.push_str(&edge_predicates[1]);
        query_string.push_str("*");

        input.regular_path_query(&query_string, output_label)
    }

    pub fn query3<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        let query_predicates = edge_predicates.clone();
        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &query_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &query_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let mut query_string1 = String::from(&edge_predicates[1]);
        query_string1.push_str("*");
        let mut query_string2 = String::from(&edge_predicates[2]);
        query_string2.push_str("*");

        let closure1 = streams[1].regular_path_query(&query_string1, "cq1".to_string());
        let closure2 = streams[2].regular_path_query(&query_string2, "cq2".to_string());

        streams[0]
            .hash_join(&closure1, HashJoinAttributePair::TS, HashJoinAttributePair::ST, "j1".to_string())
            .hash_join(&closure2, HashJoinAttributePair::TS, HashJoinAttributePair::ST, output_label)
    }

    pub fn query3_a<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // create RPQ string
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("/");
        query_string.push_str(&edge_predicates[1]);
        query_string.push_str("*/");
        query_string.push_str(&edge_predicates[2]);
        query_string.push_str("*");

        input.regular_path_query(&query_string, output_label)
    }

    /// RPQ (a/b/c)+ automata based evaluation
    pub fn query4_a<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // create RPQ string
        let mut query_string = String::from("(");
        query_string.push_str(&edge_predicates[0]);
        query_string.push_str("/");
        query_string.push_str(&edge_predicates[1]);
        query_string.push_str("/");
        query_string.push_str(&edge_predicates[2]);
        query_string.push_str(")+");

        input.regular_path_query(&query_string, output_label)
    }

    /// RPQ (a/b/c)+ hybrid evaluation that first materializes a/b join, then joins with c
    pub fn query4_pc1<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        let edge_predicate3 = edge_predicates[2].clone();
        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let cq = streams[0]
            .hash_join(&streams[1], HashJoinAttributePair::TS, HashJoinAttributePair::ST, "cq".to_string());

        // create RPQ string
        let mut query_string = String::from("(cq/");
        query_string.push_str(&edge_predicate3);
        query_string.push_str(")+");

        streams[2].concat(&cq).regular_path_query(&query_string, output_label)
    }

    /// RPQ (a/b/c)+ hybrid evaluation that first materializes b/c join, then joins with a
    pub fn query4_pc2<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        let edge_predicate1 = edge_predicates[0].clone();
        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let cq = streams[1]
            .hash_join(&streams[2], HashJoinAttributePair::TS, HashJoinAttributePair::ST, "cq".to_string());

        // create RPQ string
        let mut query_string = String::from("(");
        query_string.push_str(&edge_predicate1);
        query_string.push_str("/cq)+");

        streams[0].concat(&cq).regular_path_query(&query_string, output_label)
    }

    /// RPQ (a/b/c)+ join based evaluation that first materializes a/b/c join
    pub fn query4<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);

        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let cq = streams[0]
            .hash_join(&streams[1], HashJoinAttributePair::TS, HashJoinAttributePair::ST, "j1".to_string())
            .hash_join(&streams[2], HashJoinAttributePair::TS, HashJoinAttributePair::ST, "cq".to_string());

        // create RPQ string
        let query_string = String::from("cq*");

        cq.regular_path_query(&query_string, output_label)
    }

    pub fn query5<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // split streams based on edge predicates

        // knows, hasCreator, replyOf are labels on LDBC

        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        streams[1]
            .hash_join(&streams[0], HashJoinAttributePair::TS, HashJoinAttributePair::ST, "j1".to_string())
            .hash_join(&streams[1], HashJoinAttributePair::TT, HashJoinAttributePair::SS, "j2".to_string())
            .hash_join_tuple(&streams[2], true, false, output_label)
    }

    pub fn query6<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // obtain closure of the first predicate
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("*");

        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let closure = streams[0].regular_path_query(&query_string, "c".to_string());
        streams[2]
            .hash_join(&streams[1], HashJoinAttributePair::ST, HashJoinAttributePair::TS, "j1".to_string())
            .hash_join_tuple(&closure, true, true, output_label)
    }

    pub fn query6_cq<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // obtain closure of the first predicate
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("*");

        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        streams[2]
            .hash_join(&streams[1], HashJoinAttributePair::ST, HashJoinAttributePair::TS, "j1".to_string())
            .hash_join_tuple(&streams[0], true, true, output_label)
    }

    pub fn query7<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // obtain closure of the first predicate
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("*");

        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        let closure = streams[0].regular_path_query(&query_string, "c".to_string());
        streams[2]
            .hash_join(&streams[1], HashJoinAttributePair::ST, HashJoinAttributePair::TS, "j1".to_string())
            .hash_join_tuple(&closure, true, true, "cq".to_string())
            .regular_path_query("cq*", "r".to_string())
            .hash_join(&streams[2], HashJoinAttributePair::TT, HashJoinAttributePair::SS, output_label)
    }

    pub fn query7_cq<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 3);
        // obtain closure of the first predicate
        let mut query_string = String::from(&edge_predicates[0]);
        query_string.push_str("*");

        // split streams based on edge predicates
        let streams = input.partition(3, move |sgt| {
            if sgt.get_label() == &edge_predicates[0] {
                (0, sgt)
            } else if sgt.get_label() == &edge_predicates[1] {
                (1, sgt)
            } else {
                (2, sgt)
            }
        });

        streams[2]
            .hash_join(&streams[1], HashJoinAttributePair::ST, HashJoinAttributePair::TS, "j1".to_string())
            .hash_join_tuple(&streams[0], true, true, "cq".to_string())
            .regular_path_query("cq*", "r".to_string())
            .hash_join(&streams[2], HashJoinAttributePair::TT, HashJoinAttributePair::SS, output_label)
    }

    pub fn query8<'a>(input: Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple>, edge_predicates: Vec<String>, output_label: String) -> Stream<Child<'a, Worker<Generic>, u64>, StreamingGraphTuple> {
        assert_eq!(edge_predicates.len(), 1);
        // obtain closure of the first predicate

        input
            .hash_join(&input, HashJoinAttributePair::TT, HashJoinAttributePair::SS, "cq".to_string())
            .filter(|sgt| sgt.get_source() != sgt.get_target())
            .inspect(|sgt| trace!("CQ: {:?}", sgt))
            .regular_path_query("cq*", output_label)
    }
}