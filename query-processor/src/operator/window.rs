extern crate timely;

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;

use self::super::super::input::{SGE, SGT, StreamingGraphEdge};
use self::super::super::input::tuple::StreamingGraphTuple;
use self::super::super::util::types::HalfOpenTimeInterval;

/// `WSCAN` operator bsaed on PVLDB Submission
/// It consumes a stream of StreamingGraphEdge's and produces a stream of StreamingGraphTuple's
/// Adjusts the validity interval of a stream of sges based on the provided window specification
pub trait SlidingWindow<G: Scope, D: Data + SGE, D2: Data + SGT<HalfOpenTimeInterval, StreamingGraphEdge>> {
    /// Produces a streaming graph tuple from given inut graph edge
    /// and adjusts its validity interval based on given window_size parameters
    fn sliding_window(&self, window_size: u64) -> Stream<G, D2>;
}


impl<G: Scope> SlidingWindow<G, StreamingGraphEdge, StreamingGraphTuple> for Stream<G, StreamingGraphEdge> {
    /// Adjust the validity interval of te given input
    fn sliding_window(&self, window_size: u64) -> Stream<G, StreamingGraphTuple> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "SlidingWindow", move |_, _| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time)
                    .give_iterator(vector.drain(..)
                        .map(|input_edge|
                            StreamingGraphTuple::from_edge(&input_edge,
                                                           HalfOpenTimeInterval::new(input_edge.get_timestamp(), input_edge.get_timestamp() + window_size))
                        ));
            });
        })
    }
}
