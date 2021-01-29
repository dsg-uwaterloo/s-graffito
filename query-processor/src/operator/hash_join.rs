extern crate timely;

use std::cmp::{max, min};

use std::hash::BuildHasherDefault;

use hashbrown::HashMap;
use hashers::fx_hash::FxHasher;
use log::{trace};

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::operator::Operator;

use crate::input::{GraphEdge, SGT, StreamingGraphEdge};
use crate::input::tuple::StreamingGraphTuple;
use crate::operator::MinPQIndex;

use self::super::super::util::types::{HalfOpenInterval, HalfOpenTimeInterval, VertexType};
use self::timely::dataflow::channels::pact::Exchange;

/// Symmetric hash join implementation based on the direct approach as described in PVLDB submssion
/// It takes two streams of sgts as inputs and produces a stream of sgts as output
pub trait SymmetricHashJoin<G: Scope<Timestamp=u64>, D: Data + SGT<HalfOpenTimeInterval, StreamingGraphEdge>> {

    /// joins two streams based on the `join_predicate` and projects the join result based on the `join_output`
    /// `join_predicate` controls the endpoints of sgts that will be used for join
    /// `join_output` controls the endpoints that will be prohect in the resulting sgts
    fn hash_join<>(&self, other: &Stream<G, StreamingGraphTuple>, join_predicate: HashJoinAttributePair, join_output: HashJoinAttributePair, output_label: String) -> Stream<G, StreamingGraphTuple>;
    /// joins two streams based on the entire tuple, i.e., (source, target) pairs
    fn hash_join_tuple<>(&self, other: &Stream<G, StreamingGraphTuple>, rhs_reverse: bool, output_reverse: bool, output_label: String) -> Stream<G, StreamingGraphTuple>;
}

impl<G: Scope<Timestamp=u64>> SymmetricHashJoin<G, StreamingGraphTuple> for Stream<G, StreamingGraphTuple> {
    fn hash_join(&self, other: &Stream<G, StreamingGraphTuple>, join_predicate: HashJoinAttributePair, join_output: HashJoinAttributePair, output_label: String) -> Stream<G, StreamingGraphTuple> {
        let mut vector = Vec::new();

        let (key_selector1, key_selector2) = get_key_selector(&join_predicate);

        let (output_selector1, output_selector2) = get_key_selector(&join_output);

        let exchange_selector1 = key_selector1.clone();
        let exchange_selector2 = key_selector2.clone();
        let exchange_source = Exchange::new(move |x: &StreamingGraphTuple| exchange_selector1(x));
        let exchange_target = Exchange::new(move |x: &StreamingGraphTuple| exchange_selector2(x));

        self.binary_frontier(other, exchange_source, exchange_target, "SymmetricHashJoin", move |_capability, _info| {
            // construct operator state

            // stash incoming input, key is a pair of (join_attribute, output_attribute) and value is the expiry timestamp
            // priority is the start timestamp
            let mut stash1 = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());
            let mut stash2 = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());

            // use a single source of truth. PQ enables look-up by keys with a custom key type
            let mut index1: MinPQIndex<VertexType, MinPQIndex<VertexType, u64>> = MinPQIndex::default();
            let mut index2: MinPQIndex<VertexType, MinPQIndex<VertexType, u64>> = MinPQIndex::default();

            let mut expired_keys = Vec::<(u64, MinPQIndex<VertexType, u64>)>::new();

            // finally create the closure to perform computation
            move |input1, input2, output| {
                // stash incoming tuples from both streams
                input1.for_each(|time, data| {
                    data.swap(&mut vector);
                    let time_index = stash1.entry(time.retain()).or_insert(HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()));
                    //stash incoming tuple
                    for sgt1 in vector.drain(..) {
                        let tuple_key = key_selector1(&sgt1);
                        let inner_value = output_selector1(&sgt1);
                        let tuple_expiry = sgt1.interval.end;
                        trace!("Sgt {:?} at input 1", sgt1);

                        // simply stash the tuple and move on
                        time_index.entry((tuple_key, inner_value)).and_modify(|current_interval: &mut HalfOpenTimeInterval| {
                            // maintain the larger expiry per tuple
                            if current_interval.get_end() < tuple_expiry {
                                *current_interval = sgt1.interval;
                            }
                        }).or_insert(sgt1.interval);
                    }
                });

                input2.for_each(|time, data| {
                    data.swap(&mut vector);
                    let time_index = stash2.entry(time.retain()).or_insert(HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()));
                    //stash incoming tuple
                    for sgt2 in vector.drain(..) {
                        let tuple_key = key_selector2(&sgt2);
                        let inner_value = output_selector2(&sgt2);
                        let tuple_expiry = sgt2.interval.end;
                        trace!("Sgt {:?} at input 2", sgt2);

                        // simply stash the tuple and move on
                        time_index.entry((tuple_key, inner_value)).and_modify(|current_interval: &mut HalfOpenTimeInterval| {
                            if current_interval.get_end() < tuple_expiry {
                                *current_interval = sgt2.interval;
                            }
                        }).or_insert(sgt2.interval);
                    }
                });

                // purge elements from the index2 based on input 1 frontier
                // pop expired keys from index2
                while let Some((_, _, expiry_ts)) = index2.peek() {
                    if input1.frontier().less_equal(&expiry_ts) {
                        break;
                    }
                    // otherwise pop the element from the state as it has expired entries
                    let (key, inner_index, _) = index2.pop().unwrap();
                    expired_keys.push((key, inner_index));
                }

                // process expired keys from index2 and re-insert if there are non-expired values
                for (expired_key, mut expired_entry) in expired_keys.drain(..) {
                    let mut min_valid_timestamp = u64::MAX;
                    // remove all entries that are expired
                    while let Some((_key, _, expiry_ts)) = expired_entry.peek() {
                        if input1.frontier().less_equal(&expiry_ts) {
                            min_valid_timestamp = expiry_ts;
                            break;
                        }
                        // otherwise pop the element
                        expired_entry.pop();
                    }

                    // re-insert the inner index if it still has values
                    if !expired_entry.is_empty() {
                        index2.push(expired_key, expired_entry, min_valid_timestamp);
                    }
                }

                // purge elements from the index1 based on input2 frontier
                while let Some((_, _, expiry_ts)) = index1.peek() {
                    if input2.frontier().less_equal(&expiry_ts) {
                        break;
                    }
                    // otherwise pop the element from the state as it has expired entries
                    let (key, inner_index, _) = index1.pop().unwrap();
                    expired_keys.push((key, inner_index));
                }

                for (expired_key, mut expired_entry) in expired_keys.drain(..) {
                    let mut min_valid_timestamp = u64::MAX;
                    // remove all entries that are expired
                    while let Some((_key, _, expiry_ts)) = expired_entry.peek() {
                        if input2.frontier().less_equal(&expiry_ts) {
                            min_valid_timestamp = expiry_ts;
                            break;
                        }
                        // otherwise pop the element
                        expired_entry.pop();
                    }

                    // re-insert the inner index if it still has values
                    if !expired_entry.is_empty() {
                        index1.push(expired_key, expired_entry, min_valid_timestamp);
                    }
                }

                // finally safely perform join for items in the stash
                // all expired tuples are purged from the state, and join will only consider tuples that can safely be extracted from the stash, i.e.,
                // all input for that particular time has arrived (as the frontier guarentees) and we are keeping the one with max interval

                // consider sending everything in `stash1`.
                for (time, tuples) in stash1.iter_mut() {
                    // if input1 cannot produce data at `time`, process the stash
                    if !input1.frontier().less_equal(time.time()) {
                        let mut session = output.session(&time);
                        // update index1 and perform join
                        for ((join_key, join_attribute1), tuple_interval1) in tuples.drain() {
                            let start_ts1: u64 = tuple_interval1.get_start();
                            let expiry_ts1: u64 = tuple_interval1.get_end();
                            let mut has_larger_expiry: bool = true;

                            // place tuples into the index1
                            if let Some((inner_index, _b)) = index1.get_mut(&join_key) {
                                // check whether same value already exists with a larger timestamp
                                if let Some((_start_ts, current_expiry_ts)) = inner_index.get(&join_attribute1) {
                                    // if value has already larger expiry ts, do not process
                                    if current_expiry_ts >= expiry_ts1 {
                                        // set the has_largeR_expiry flag to signal join will NOT process a new result with a larger expiry
                                        has_larger_expiry = false;
                                    } else {
                                        // update the entry with larger expiry
                                        inner_index.push(join_attribute1, start_ts1, expiry_ts1);
                                    }
                                } else {
                                    // it does not exist, push new value
                                    inner_index.push(join_attribute1, start_ts1, expiry_ts1);
                                }
                            } else {
                                let mut new_inner_index = MinPQIndex::default();
                                new_inner_index.push(join_attribute1, start_ts1, expiry_ts1);
                                index1.push(join_key, new_inner_index, expiry_ts1);
                            }
                            // decrease priority in index 1
                            index1.try_decrease_priority(&join_key, expiry_ts1);

                            // perform join if incoming tuple is new or has larger expiry
                            if has_larger_expiry {
                                if let Some((inner_index, _)) = index2.get(&join_key) {
                                    for (join_attribute2, start_ts2, expiry_ts2) in inner_index.iter() {
                                        session.give(
                                            StreamingGraphTuple::new(
                                                join_attribute1,
                                                join_attribute2,
                                                output_label.clone(),
                                                HalfOpenTimeInterval::new(max(start_ts1, *start_ts2), min(expiry_ts1, expiry_ts2)),
                                            )
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // discard `time` entries with empty `list`.
                stash1.retain(|_time, list| list.len() > 0);

                // consider sending everything in `stash2`.
                for (time, tuples) in stash2.iter_mut() {
                    // if input2 cannot produce data at `time`, process the stash
                    if !input2.frontier().less_equal(time.time()) {
                        let mut session = output.session(&time);
                        // update index1 and perform join
                        for ((join_key, join_attribute2), tuple_interval2) in tuples.drain() {
                            let start_ts2: u64 = tuple_interval2.get_start();
                            let expiry_ts2: u64 = tuple_interval2.get_end();
                            let mut has_larger_expiry: bool = true;

                            // place tuples into the index2
                            if let Some((inner_index, _b)) = index2.get_mut(&join_key) {
                                // check whether same value already exists with a larger timestamp
                                if let Some((_start_ts, current_expiry_ts)) = inner_index.get(&join_attribute2) {
                                    // if value has already larger expiry ts, do not process
                                    if current_expiry_ts >= expiry_ts2 {
                                        // set the has_largeR_expiry flag to signal join will process a new result with a larger expiry
                                        has_larger_expiry = false;
                                    } else {
                                        // update the entry with larger expiry
                                        inner_index.push(join_attribute2, start_ts2, expiry_ts2);
                                    }
                                } else {
                                    // it does not exist, push new value
                                    inner_index.push(join_attribute2, start_ts2, expiry_ts2);
                                }
                            } else {
                                let mut new_inner_index = MinPQIndex::default();
                                new_inner_index.push(join_attribute2, start_ts2, expiry_ts2);
                                index2.push(join_key, new_inner_index, expiry_ts2);
                            }
                            // decrease priority in index 1
                            index2.try_decrease_priority(&join_key, expiry_ts2);

                            // perform join if incoming tuple is new or has larger expiry
                            if has_larger_expiry {
                                if let Some((inner_index, _)) = index1.get(&join_key) {
                                    for (join_attribute1, start_ts1, expiry_ts1) in inner_index.iter() {
                                        session.give(
                                            StreamingGraphTuple::new(
                                                join_attribute1,
                                                join_attribute2,
                                                output_label.clone(),
                                                HalfOpenTimeInterval::new(max(*start_ts1, start_ts2), min(expiry_ts1, expiry_ts2)),
                                            )
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // discard `time` entries with empty `list`.
                stash2.retain(|_time, list| list.len() > 0);
            }
        })
    }

    // rhs_reverse controls whether sgts in the second input should be reversed, i.e., (trg, src) instead of (src, trg)
    // output_reverse controls the order of enpoints in resulting sgts
    fn hash_join_tuple<>(&self, other: &Stream<G, StreamingGraphTuple>, rhs_reverse: bool, output_reverse: bool, output_label: String) -> Stream<G, StreamingGraphTuple> {
        // tuple to be stored as the join state
        type JoinKey = (VertexType, VertexType);

        let mut vector = Vec::new();

        // key selector for inputs streams, based on the edge direction
        let key_selector1 = forward_tuple_selector;
        let key_selector2 = if rhs_reverse {
            reverse_tuple_selector
        } else {
            forward_tuple_selector
        };

        // input1 is outputted based on the edge direction
        let output_selector1 = if output_reverse {
            reverse_tuple_selector
        } else {
            forward_tuple_selector
        };

        // input2 is outputted based on xor of the rhs_reverse and output_reverse flags
        let output_selector2 = if rhs_reverse ^ output_reverse {
            reverse_tuple_selector
        } else {
            forward_tuple_selector
        };

        let exchange_selector1 = key_selector1.clone();
        let exchange_selector2 = key_selector2.clone();
        let exchange_source = Exchange::new(move |x: &StreamingGraphTuple| exchange_selector1(x).0);
        let exchange_target = Exchange::new(move |x: &StreamingGraphTuple| exchange_selector2(x).0);

        self.binary_frontier(other, exchange_source, exchange_target, "SymmetricHashJoinTuple", move |_capability, _info| {
            // construct operator state

            // stash incoming input, key is a pair of (join_attribute, output_attribute) and value is the expiry timestamp
            // priority is the start timestamp
            let mut stash1 = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());
            let mut stash2 = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());

            // use a single source of truth. PQ enables look-up by keys with a custom key type
            // in this key, for every join key, we store its start_ts as value and its expiry is the priority in the MinPQIndex
            let mut index1: MinPQIndex<JoinKey, u64> = MinPQIndex::<JoinKey, u64>::default();
            let mut index2: MinPQIndex<JoinKey, u64> = MinPQIndex::<JoinKey, u64>::default();

            // finally create the closure to perform computation
            move |input1, input2, output| {
                // stash incoming tuples from both streams
                input1.for_each(|time, data| {
                    data.swap(&mut vector);
                    let time_index = stash1.entry(time.retain()).or_insert(HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()));

                    //stash incoming tuples
                    for sgt1 in vector.drain(..) {
                        let tuple_key = key_selector1(&sgt1);
                        let inner_value = output_selector1(&sgt1);
                        let tuple_expiry = sgt1.interval.get_end();
                        trace!("SGT {:?} at input 1", sgt1);

                        // sinply stash the tuple and move in
                        time_index.entry((tuple_key, inner_value))
                            .and_modify(|current_interval: &mut HalfOpenTimeInterval| {
                                // maintain the larger expiry per tuple
                                if current_interval.get_end() < tuple_expiry {
                                    *current_interval = sgt1.interval
                                }
                            })
                            .or_insert(sgt1.interval);
                    }
                });

                input2.for_each(|time, data| {
                    data.swap(&mut vector);
                    let time_index = stash2.entry(time.retain()).or_insert(HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default()));

                    //stash incoming tuples
                    for sgt2 in vector.drain(..) {
                        let tuple_key = key_selector2(&sgt2);
                        let inner_value = output_selector2(&sgt2);
                        let tuple_expiry = sgt2.interval.get_end();
                        trace!("SGT {:?} at input 2", sgt2);

                        // sinply stash the tuple and move in
                        time_index.entry((tuple_key, inner_value))
                            .and_modify(|current_interval: &mut HalfOpenTimeInterval| {
                                // maintain the larger expiry per tuple
                                if current_interval.get_end() < tuple_expiry {
                                    *current_interval = sgt2.interval
                                }
                            })
                            .or_insert(sgt2.interval);
                    }
                });

                // purge elements from the index2 based on input 1 frontier
                while let Some((_, _, expiry_ts)) = index2.peek() {
                    if input1.frontier().less_equal(&expiry_ts) {
                        break;
                    }
                    // otherwise pop the element from the state as its expiry has passed
                    index2.pop().unwrap();
                }

                // purge elements from the index1 based on input 2 frontier
                while let Some((_, _, expiry_ts)) = index1.peek() {
                    if input2.frontier().less_equal(&expiry_ts) {
                        break;
                    }
                    // otherwise pop the element from the state as its expiry has passed
                    index1.pop().unwrap();
                }

                // finally safely perform join for items in the stash without worrying about intervals
                // all expired tuples are purged from the state, and join will only consider tuples that can safely be extracted from the stash, i.e.,
                // all input for that particular time has arrived (as the frontier guarentees) and we are keeping the one with max interval

                // consider processing tuples whose time has been completed based on the frontier in stash1
                for (time, tuples) in stash1.iter_mut() {
                    // if input1 cannot produce data at `time`, process the stash
                    if !input1.frontier().less_equal(time.time()) {
                        let mut session = output.session(&time);
                        // update index 1, then perform the join
                        for ((join_key, join_value), tuple_interval1) in tuples.drain() {
                            let start_ts1 = tuple_interval1.get_start();
                            let expiry_ts1 = tuple_interval1.get_end();
                            let mut has_larger_expiry = true;

                            // place tuples int the index1
                            // check whether there is already an entry for the same key
                            if let Some((_start_ts, current_expiry_ts)) = index1.get(&join_key) {
                                // check whether existing entry already has larger expiry
                                if current_expiry_ts >= expiry_ts1 {
                                    // set the flag to skip join processing, same key already exists with a larger key
                                    has_larger_expiry = false;
                                } else {
                                    index1.push(join_key, start_ts1, expiry_ts1);
                                }
                            } else {
                                index1.push(join_key, start_ts1, expiry_ts1);
                            }

                            // get mathcing tuple from rhs has table
                            // perform join only if incoming tuple can produce new results with larger expiry
                            if has_larger_expiry {
                                if let Some((start_ts2, expiry_ts2)) = index2.get(&join_key) {
                                    session.give(
                                        StreamingGraphTuple::new(
                                            join_value.0,
                                            join_value.1,
                                            output_label.clone(),
                                            HalfOpenTimeInterval::new(max(start_ts1, *start_ts2), min(expiry_ts1, expiry_ts2)),
                                        )
                                    );
                                }
                            }
                        }
                    }
                }

                // discard `time` entries with empty `list`.
                stash1.retain(|_time, list| list.len() > 0);

                // consider processing tuples whose time has been completed based on the frontier in stash2
                for (time, tuples) in stash2.iter_mut() {
                    // if input2 cannot produce data at `time`, process the stash
                    if !input2.frontier().less_equal(time.time()) {
                        let mut session = output.session(&time);
                        // update index 2, then perform the join
                        for ((join_key, join_value), tuple_interval2) in tuples.drain() {
                            let start_ts2 = tuple_interval2.get_start();
                            let expiry_ts2 = tuple_interval2.get_end();
                            let mut has_larger_expiry = true;

                            // place tuples int the index2
                            // check whether there is already an entry for the same key
                            if let Some((_start_ts, current_expiry_ts)) = index2.get(&join_key) {
                                // check whether existing entry already has larger expiry
                                if current_expiry_ts >= expiry_ts2 {
                                    // set the flag to skip join processing, same key already exists with a larger key
                                    has_larger_expiry = false;
                                } else {
                                    index2.push(join_key, start_ts2, expiry_ts2);
                                }
                            } else {
                                index2.push(join_key, start_ts2, expiry_ts2);
                            }

                            // get mathcing tuple from lhs has table
                            // perform join only if incoming tuple can produce new results with larger expiry
                            if has_larger_expiry {
                                if let Some((start_ts1, expiry_ts1)) = index1.get(&join_key) {
                                    session.give(
                                        StreamingGraphTuple::new(
                                            join_value.0,
                                            join_value.1,
                                            output_label.clone(),
                                            HalfOpenTimeInterval::new(max(*start_ts1, start_ts2), min(expiry_ts1, expiry_ts2)),
                                        )
                                    );
                                }
                            }
                        }
                    }
                }

                // discard `time` entries with empty `list`.
                stash2.retain(|_time, list| list.len() > 0);
            }
        })
    }
}

/// Pair of sgt attributes for join:
/// SS: Join by source of both sgts
/// ST: Join the source of lhs with target of rhs
/// TS: Join the target of lhs with source of rhs
/// TT: Join by target of both sgts
pub enum HashJoinAttributePair {
    SS,
    ST,
    TS,
    TT,
}

fn forward_tuple_selector(tuple: &StreamingGraphTuple) -> (u64, u64) {
    (tuple.get_source(), tuple.get_target())
}

fn reverse_tuple_selector(tuple: &StreamingGraphTuple) -> (u64, u64) {
    (tuple.get_target(), tuple.get_source())
}

fn source_selector(tuple: &StreamingGraphTuple) -> u64 {
    tuple.get_source()
}

fn target_selector(tuple: &StreamingGraphTuple) -> u64 {
    tuple.get_target()
}

fn get_key_selector(predicate: &HashJoinAttributePair) -> (fn(&StreamingGraphTuple) -> VertexType, fn(&StreamingGraphTuple) -> VertexType)

{
    match predicate {
        HashJoinAttributePair::SS => {
            (source_selector, source_selector)
        }
        HashJoinAttributePair::ST => {
            (source_selector, target_selector)
        }
        HashJoinAttributePair::TS => {
            (target_selector, source_selector)
        }
        HashJoinAttributePair::TT => {
            (target_selector, target_selector)
        }
    }
}
