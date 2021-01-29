extern crate abomonation;
extern crate abomonation_derive;
extern crate strum;
extern crate strum_macros;

use std::cmp::{max, min};
use std::fmt;

use abomonation_derive::Abomonation;
use strum_macros::EnumString;

/// custom type definitions
pub type VertexType = u64;
pub type StateType = u8;
pub type VertexStatePair = (VertexType, StateType);

/// constants
pub const REPORTING_PERIOD_MILLISECONDS: u64 = 5000;

/// Operation mode for SGA operators
#[derive(EnumString, PartialEq, Copy, Clone)]
pub enum OperationType {
    Direct,
    NegativeTuple,
}

/// HalOpen Interval Trait
pub trait HalfOpenInterval {
    fn overlaps(&self, other: &Self) -> bool;
    fn merge_mut(&mut self, other: &Self);
    fn intersect_mut(&mut self, other: &Self);
    fn merge(first: &Self, second: &Self) -> Self;
    fn intersect(first: &Self, second: &Self) -> Self;
    fn get_start(&self) -> u64;
    fn get_end(&self) -> u64;
}

/// Half-open time interval that is used to represent validity intervals
#[derive(Copy, Clone, PartialEq, Abomonation, Debug, Hash, Eq)]
pub struct HalfOpenTimeInterval {
    pub start: u64,
    pub end: u64,
}


impl fmt::Display for HalfOpenTimeInterval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

impl HalfOpenTimeInterval {
    pub const ZERO: Self = Self { start: 0, end: 0 };

    pub fn new(start: u64, end: u64) -> Self {
        Self { start: start, end: end }
    }
}


impl HalfOpenInterval for HalfOpenTimeInterval {
    /// checks whether two interval overlaps
    fn overlaps(&self, other: &Self) -> bool {
        if self.start > other.start {
            self.start < other.end
        } else if self.start < other.start {
            self.end > other.start
        } else {
            true
        }
    }

    /// update the inerval by merging with other
    fn merge_mut(&mut self, other: &Self) {
        self.start = min(self.start, other.start);
        self.end = max(self.end, other.end);
    }

    /// update the interval by intersection with other
    fn intersect_mut(&mut self, other: &Self) {
        self.start = max(self.start, other.start);
        self.end = min(self.end, other.end);
    }

    /// create new interval as a union of two intervals
    fn merge(first: &Self, second: &Self) -> Self {
        HalfOpenTimeInterval::new(min(first.start, second.start), max(first.end, second.end))
    }

    /// create a new interval as an intersection of two intervals
    fn intersect(first: &Self, second: &Self) -> Self {
        HalfOpenTimeInterval::new(max(first.start, second.start), min(first.end, second.end))
    }

    fn get_start(&self) -> u64 {
        self.start
    }

    fn get_end(&self) -> u64 {
        self.end
    }
}