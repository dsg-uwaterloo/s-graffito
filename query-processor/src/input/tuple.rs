extern crate abomonation;
extern crate abomonation_derive;
extern crate timely;

use abomonation_derive::Abomonation;

use crate::input::{GraphEdge, SGT, StreamingGraphEdge};

use self::super::super::util::types::{HalfOpenTimeInterval, VertexType};

/// StreamingGraphTuple implementation
#[derive(Clone, Debug, Abomonation, PartialEq, Hash,
Eq)]
pub struct StreamingGraphTuple {
    pub source: u64,
    pub target: u64,
    pub label: String,
    pub interval: HalfOpenTimeInterval,
    pub append: bool,
}

impl GraphEdge for StreamingGraphTuple {
    fn get_source(&self) -> VertexType {
        self.source
    }

    fn get_target(&self) -> VertexType {
        self.target
    }

    fn get_label(&self) -> &str {
        &self.label
    }
}

impl SGT<HalfOpenTimeInterval, StreamingGraphEdge> for StreamingGraphTuple {
    fn from_edge(edge: &StreamingGraphEdge, interval: HalfOpenTimeInterval) -> Self {
        Self {
            source: edge.get_source(),
            target: edge.get_target(),
            label: edge.get_label().to_string(),
            interval: interval,
            append: edge.append,
        }
    }

    fn new(source: u64, target: u64, label: String, interval: HalfOpenTimeInterval) -> Self {
        Self { source: source, target: target, label: label, interval: interval, append: true }
    }

    fn get_interval(&self) -> HalfOpenTimeInterval {
        self.interval
    }
}
