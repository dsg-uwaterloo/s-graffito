
use std::cmp::Reverse;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::hash::BuildHasherDefault;

use hashers::fx_hash::FxHasher;
use priority_queue::PriorityQueue;

pub mod delta;
pub mod tree_node;
pub mod spanning_tree;
pub mod window;
pub mod rpq;
pub mod hash_join;


/// custom struct to store entries in PriorityQueue
/// The goal is to enable key-based lookups for complex object stored in a PriorityQueue
#[derive(Clone, Debug)]
struct PQEntry<K: Copy + PartialEq + Clone + Debug + Hash + Default, V: Clone + Debug> {
    key: K,
    entry: Option<V>,
}

impl<K, V> PQEntry<K, V>
    where
        K: Copy + PartialEq + Clone + Debug + Hash + Default,
        V: Clone + Debug {
    pub fn create_key(entry_key: K) -> Self {
        Self {
            key: entry_key,
            entry: None,
        }
    }

    pub fn swap_key(&mut self, new_key: K) {
        self.key = new_key
    }

    pub fn create_entry(entry_key: K, value: V) -> Self {
        Self {
            key: entry_key,
            entry: Some(value),
        }
    }

    pub fn get_key(&self) -> K where {
        self.key
    }

    pub fn drain(self) -> V {
        self.entry.unwrap()
    }

    pub fn get_entry(&self) -> &V {
        self.entry.as_ref().unwrap()
    }

    pub fn get_entry_mut(&mut self) -> &mut V {
        self.entry.as_mut().unwrap()
    }
}

impl<K, V> Default for PQEntry<K, V>
    where
        K: Copy + PartialEq + Clone + Debug + Hash + Default,
        V: Clone + Debug {
    fn default() -> Self {
        Self {
            key: K::default(),
            entry: None,
        }
    }
}

impl<K, V> PartialEq for PQEntry<K, V>
    where
        K: Copy + PartialEq + Clone + Debug + Hash + Default,
        V: Clone + Debug {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }

    fn ne(&self, other: &Self) -> bool {
        self.key != other.key
    }
}

impl<K, V> Eq for PQEntry<K, V> where
    K: Copy + PartialEq + Clone + Debug + Hash + Default,
    V: Clone + Debug {}

impl<K, V> Hash for PQEntry<K, V> where
    K: Copy + PartialEq + Clone + Debug + Hash + Default,
    V: Clone + Debug {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }

    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H) where
        Self: Sized, {
        for piece in data {
            piece.hash(state)
        }
    }
}

/// Helper struct for PQIndex used in SGA Operator implementations
/// it is a min priority-queue backed by an indexmap that provides efficient key-lookups
/// Being backed by PQ, it supports efficient (logn) operations to pop, push and change priority
/// It uses `PQentry` type ot enable key based lookups
/// It uses `FxHasher` as the default has function
#[derive(Clone, Debug)]
pub struct MinPQIndex<K: Copy + PartialEq + Clone + Debug + Hash + Default, V: Clone + Debug> {
    index: PriorityQueue<PQEntry<K, V>, Reverse<u64>, BuildHasherDefault<FxHasher>>,
    index_key: PQEntry<K, V>,
}

impl<K, V> Default for MinPQIndex<K, V>
    where
        K: Copy + PartialEq + Clone + Debug + Hash + Default,
        V: Clone + Debug {
    fn default() -> Self {
        Self {
            index: PriorityQueue::with_hasher(BuildHasherDefault::<FxHasher>::default()),
            index_key: PQEntry::default(),
        }
    }
}

impl<K, V> MinPQIndex<K, V>
    where
        K: Copy + PartialEq + Clone + Debug + Hash + Default,
        V: Clone + Debug {
    /// insert a new element with given priority - log(n)
    pub fn push(&mut self, key: K, value: V, priority: u64) -> Option<u64> {
        // create a new key
        let entry = PQEntry::create_entry(key, value);
        self.index.push(entry, Reverse(priority)).map(|Reverse(ts)| ts)
    }

    /// arbitrary changes to the priority of the given item - log(n)
    pub fn change_priority(&mut self, key: &K, new_priority: u64) -> Option<u64> {
        self.index_key.swap_key(*key);
        self.index.change_priority(&self.index_key, Reverse(new_priority)).map(|Reverse(ts)| ts)
    }

    /// retrieve the value and its priority for a given key
    pub fn get(&self, key: &K) -> Option<(&V, u64)> {
        self.index.get(&PQEntry::create_key(*key)).map(|(val, Reverse(ts))| (val.get_entry(), *ts))
    }

    /// retrieve the mutable value and its priority for a given key
    pub fn get_mut(&mut self, key: &K) -> Option<(&mut V, u64)> {
        self.index_key.swap_key(*key);
        self.index.get_mut(&self.index_key).map(|map_entry| (map_entry.0.get_entry_mut(), (map_entry.1).0))
    }

    /// decrease the priority of the given key only if its priority is larger than the argument
    pub fn try_decrease_priority(&mut self, key: &K, priority: u64) {
        // get existing priority of the element
        self.index_key.swap_key(*key);
        // decrease prioroty of the given element if it has a higher priority
        if self.index.get_priority(&self.index_key).map_or(false, |Reverse(p)| *p > priority) {
            self.index.change_priority(&self.index_key, Reverse(priority));
        }
    }

    /// iterate over (key ,value, priority) triples in an arbitrary order
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(K, &'a V, u64)> {
        self.index.iter().map(|(entry, Reverse(p))| (entry.get_key(), entry.get_entry(), *p))
    }

    /// iterate over (value, priority) tuples in an arbitrary order
    pub fn value_iterator<'a>(&'a self) -> impl Iterator<Item=(&'a V, u64)> {
        self.index.iter().map(|(entry, Reverse(p))| (entry.get_entry(), *p))
    }

    /// retrieve reference to entry with the min priority
    pub fn peek(&self) -> Option<(K, &V, u64)> {
        self.index.peek().map(|(val, Reverse(ts))| (val.get_key(), val.get_entry(), *ts))
    }

    /// extract min priority element -- log(n)
    pub fn pop(&mut self) -> Option<(K, V, u64)> {
        self.index.pop().map(|(val, Reverse(ts))| (val.get_key(), val.drain(), ts))
    }

    /// remove the entry with the given key
    pub fn remove(&mut self, key: &K) -> Option<(V, u64)> {
        self.index_key.swap_key(*key);
        self.index.remove(&self.index_key).map(|(val, Reverse(ts))| (val.drain(), ts))
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

/// Tests for IntervalSet related functionality
#[cfg(test)]
mod tests {
    use crate::util::types::{HalfOpenTimeInterval, HalfOpenInterval};
    use crate::operator::tests::IntervalSetContent::{Single, Set};


    /// Helper struct to hold multiple intervals sorted
    /// Intervals are merged if they overlap
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    enum IntervalSetContent<I: HalfOpenInterval + Clone> {
        Single(I),
        Set(Vec<I>),
    }

    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct IntervalSet<I: HalfOpenInterval + Clone> {
        // set of non-overlapping intervals
        // intervals: Vec<I>,
        content: Option<IntervalSetContent<I>>
    }

    impl<I> IntervalSet<I> where I: HalfOpenInterval + Clone {
        pub fn new(interval: I) -> Self {
            Self {
                // intervals: vec![interval],
                content: Some(Single(interval))
            }
        }

        /// append the interval into correct spot with coalescing if necessary
        /// returns true if the expiry of the new interval sets the largest expiry for this interval set
        pub fn insert(&mut self, mut interval: I) -> bool {
            let is_max_expiry = self.get_max_expiry().map_or(true, |current_max| interval.get_end() > current_max);
            if let Some(isc) = &mut self.content {
                match isc {
                    Single(i) => {
                        if i.overlaps(&interval) {
                            // simply merge with existing interval
                            i.merge_mut(&interval);
                        } else {
                            // change enum type with a vector
                            let new_intervals = if i.get_start() < interval.get_start() {
                                vec![i.clone(), interval]
                            } else {
                                vec![interval, i.clone()]
                            };
                            std::mem::replace(isc, IntervalSetContent::Set(new_intervals));
                        }
                    }
                    Set(intervals) => {
                        // find the position where merged interval will be inserted
                        let start = intervals.iter().position(|curr_interval| {
                            // find the first point for merge, first item that has end later than the given start
                            curr_interval.get_end() >= interval.get_start()
                        }).unwrap_or(intervals.len());

                        // find the position where merge will not consider
                        let end = intervals.iter().position(|curr_interval| {
                            // find the first item that has start is later than the end
                            curr_interval.get_start() > interval.get_end()
                        }).unwrap_or(intervals.len());

                        // start merging all intervals within the given range
                        // no mergng if start and end are equal, simply insert at the position
                        for index in start..end {
                            interval.merge_mut(&intervals[index]);
                        }

                        // drain the elements in the range
                        intervals.drain(start..end);
                        // finally insert the newly constructed interval
                        intervals.insert(start, interval);

                        // if there is a single value left. demote it into a Single
                        if intervals.len() == 1 {
                            let new_content = IntervalSetContent::Single(intervals.pop().unwrap());
                            std::mem::replace(isc, new_content);
                        }
                    }
                }
            }

            // return true if the incoming increases the expiry
            is_max_expiry
        }

        pub fn expiry(&mut self, low_watermark: u64) {
            if let Some(isc) = &mut self.content {
                match isc {
                    Single(i) => {
                        if i.get_end() <= low_watermark {
                            // simply replace it with None
                            self.content.take();
                        }
                    }
                    Set(intervals) => {
                        // find the range that has expired
                        let end = intervals.iter().position(|interval| {
                            interval.get_end() > low_watermark
                        }).unwrap_or(intervals.len());

                        // drain the range
                        intervals.drain(0..end);

                        // if there is a single value left. demote it into a Single
                        if intervals.len() == 1 {
                            let new_content = IntervalSetContent::Single(intervals.pop().unwrap());
                            std::mem::replace(isc, new_content);
                        } else if intervals.is_empty() {
                            self.content.take();
                        }
                    }
                }
            }
        }

        // returns intervals as a Vec of tuples
        fn as_pairs(&self) -> Vec<(u64, u64)> {
            self.content.as_ref().map(|isc| {
                match isc {
                    Single(i) => {
                        vec![(i.get_start(), i.get_end())]
                    }
                    Set(intervals) => {
                        intervals.iter().cloned().map(|interval| (interval.get_start(), interval.get_end())).collect()
                    }
                }
            }).unwrap_or(vec![])
        }

        // get the min expiry in this interval set
        pub fn get_min_expiry(&self) -> Option<u64> {
            self.content.as_ref().map(|isc| {
                match isc {
                    Single(i) => {
                        i.get_end()
                    }
                    Set(intervals) => {
                        intervals.first().unwrap().get_end()
                    }
                }
            })
        }

        // get the max expiry in this interval set
        pub fn get_max_expiry(&self) -> Option<u64> {
            self.content.as_ref().map(|isc| {
                match isc {
                    Single(i) => {
                        i.get_end()
                    }
                    Set(intervals) => {
                        intervals.last().unwrap().get_end()
                    }
                }
            })
        }
    }

    #[test]
    fn single_item() {
        let set = IntervalSet::<HalfOpenTimeInterval>::new(HalfOpenTimeInterval::new(3, 6));

        assert_eq!(set.as_pairs(), vec![(3, 6)]);
    }

    #[test]
    fn gap_insert() {
        let mut set = IntervalSet::new(HalfOpenTimeInterval::new(3, 6));
        set.insert(HalfOpenTimeInterval::new(10, 13));
        set.insert(HalfOpenTimeInterval::new(15, 21));

        assert_eq!(set.as_pairs(), vec![(3, 6), (10, 13), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(8, 9));

        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 9), (10, 13), (15, 21)]);
    }

    #[test]
    fn insert_test() {
        let mut set = IntervalSet::new(HalfOpenTimeInterval::new(3, 6));
        set.insert(HalfOpenTimeInterval::new(10, 13));
        set.insert(HalfOpenTimeInterval::new(15, 21));


        set.insert(HalfOpenTimeInterval::new(8, 9));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 9), (10, 13), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(8, 11));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 13), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(11, 14));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 14), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(11, 12));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 14), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(8, 14));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 14), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(5, 14));
        assert_eq!(set.as_pairs(), vec![(3, 14), (15, 21)]);

        set.insert(HalfOpenTimeInterval::new(14, 15));
        assert_eq!(set.as_pairs(), vec![(3, 21)]);

        set.insert(HalfOpenTimeInterval::new(1, 2));
        assert_eq!(set.as_pairs(), vec![(1, 2), (3, 21)]);

        set.insert(HalfOpenTimeInterval::new(22, 25));
        assert_eq!(set.as_pairs(), vec![(1, 2), (3, 21), (22, 25)]);
    }

    #[test]
    fn expiry() {
        let mut set = IntervalSet::new(HalfOpenTimeInterval::new(3, 6));
        set.insert(HalfOpenTimeInterval::new(10, 13));
        set.insert(HalfOpenTimeInterval::new(15, 21));
        set.insert(HalfOpenTimeInterval::new(8, 9));
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 9), (10, 13), (15, 21)]);

        set.expiry(2);
        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 9), (10, 13), (15, 21)]);

        set.expiry(6);
        assert_eq!(set.as_pairs(), vec![(8, 9), (10, 13), (15, 21)]);

        set.expiry(11);
        assert_eq!(set.as_pairs(), vec![(10, 13), (15, 21)]);
    }

    #[test]
    fn min_expiry() {
        let mut set = IntervalSet::new(HalfOpenTimeInterval::new(3, 6));

        set.insert(HalfOpenTimeInterval::new(10, 13));
        set.insert(HalfOpenTimeInterval::new(15, 21));
        set.insert(HalfOpenTimeInterval::new(8, 9));

        assert_eq!(set.as_pairs(), vec![(3, 6), (8, 9), (10, 13), (15, 21)]);

        assert_eq!(set.get_min_expiry().unwrap(), 6);
    }
}