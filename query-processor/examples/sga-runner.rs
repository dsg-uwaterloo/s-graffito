extern crate timely;

use std::iter::Iterator;
use std::thread;
use std::time::Duration;

use timely::dataflow::*;
use timely::dataflow::operators::{Input, Probe, Inspect};

use sgraffito_query::operator::{window::SlidingWindow};
use sgraffito_query::input::{SGE, GraphEdge, StreamingGraphEdge, LineFileReader, InputFileReader};

use log::{info, trace};

use metrics_runtime::Receiver;

use sgraffito_query::util::types::{REPORTING_PERIOD_MILLISECONDS};
use sgraffito_query::util::metrics::csv_exporter::CSVExporter;
use sgraffito_query::util::metrics::csv_builder::CSVBuilder;
use sgraffito_query::query::query_library::SGAQueryLibrary;

/// Utility to run StreamingGraphQueries on SGA-based query processor prototype. Arguments
/// 1. window size
/// 2. slide size
/// 3. Input type: allowed values are `{s, st, i, it}` where `s`, `i` represent string or integer vertex identifiers and `t` denotes a timestamped input file
/// 4. filename: Absolute path for the input stream file
/// 5. reporting file: Absolute path where metrics will be recorded
/// 6. query name: name of the query to be executed
/// 7. arg_count: # of edge predicates that are required by the `query`
/// 8. space seperated list of edge predicates
fn main() {
    let mut args = std::env::args();
    args.next();

    // command-line args: numbers of nodes and edges in the random graph.
    let window_size: u64 = args.next().unwrap().parse().unwrap();
    let slide_size: u64 = args.next().unwrap().parse().unwrap();
    let input_type_name = args.next().unwrap();
    let filename = args.next().unwrap();
    let reporting_file = args.next().unwrap();
    let query_name = args.next().unwrap();
    let argument_count: usize = args.next().unwrap().parse().unwrap();

    let mut edge_predicates = Vec::new();
    // parse arguments into a vector
    for _i in 0..argument_count {
        edge_predicates.push(args.next().unwrap());
    }

    // initialize env_logger
    env_logger::init();

    // initialize runtime and metric logger
    let receiver = Receiver::builder().build().expect("failed to create receiver");
    let mut exporter = CSVExporter::new(
        receiver.controller(),
        CSVBuilder::default(),
        &reporting_file,
        Duration::from_millis(REPORTING_PERIOD_MILLISECONDS)
    );

    // spawn a bakcground thread to run metric logger
    thread::spawn(move || exporter.run());

    timely::execute_from_args(std::env::args().skip(6), move |worker| {

        let mut input: InputHandle<u64, StreamingGraphEdge> = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // initialize sink
        let mut sink = receiver.sink();


        let mut batch_start= sink.now();

        let timer = ::std::time::Instant::now();

        let query_arguments = edge_predicates.clone();
        worker.dataflow::<u64, _, _>(|scope| {

            let windowed_stream = scope.input_from(&mut input).sliding_window(window_size);

            let result = match query_name.as_str() {
                "join" => {
                    SGAQueryLibrary::hash_join(windowed_stream, query_arguments, "join".to_string())
                },
                "query1" => {
                    SGAQueryLibrary::query1(windowed_stream, query_arguments, "q1".to_string())
                },
                "query2" => {
                    SGAQueryLibrary::query2(windowed_stream, query_arguments, "q2".to_string())
                },
                "query2-a" => {
                    SGAQueryLibrary::query2_a(windowed_stream, query_arguments, "q2".to_string())
                },
                "query3" => {
                    SGAQueryLibrary::query3(windowed_stream, query_arguments, "q3".to_string())
                },
                "query3-a" => {
                    SGAQueryLibrary::query3_a(windowed_stream, query_arguments, "q3".to_string())
                },
                "query4" => {
                    SGAQueryLibrary::query4(windowed_stream, query_arguments, "q4".to_string())
                },
                "query4-a" => {
                    SGAQueryLibrary::query4_a(windowed_stream, query_arguments, "q4".to_string())
                },
                "query4-pc1" => {
                    SGAQueryLibrary::query4_pc1(windowed_stream, query_arguments, "q4".to_string())
                },
                "query4-pc2" => {
                    SGAQueryLibrary::query4_pc2(windowed_stream, query_arguments, "q4".to_string())
                },
                "query5" => {
                    SGAQueryLibrary::query5(windowed_stream, query_arguments, "q5".to_string())
                },
                "query6" => {
                    SGAQueryLibrary::query6(windowed_stream, query_arguments, "q6".to_string())
                },
                "query6-cq" => {
                    SGAQueryLibrary::query6_cq(windowed_stream, query_arguments, "q6".to_string())
                },
                "query7" => {
                    SGAQueryLibrary::query7(windowed_stream, query_arguments, "q7".to_string())
                },
                "query7-cq" => {
                    SGAQueryLibrary::query7_cq(windowed_stream, query_arguments, "q7".to_string())
                },
                "query8" => {
                    SGAQueryLibrary::query8(windowed_stream, query_arguments, "q8".to_string())
                },
                _ => {
                    panic!("Supplied query name is not defined: {}", &query_name);
                }
            };

            result
                .inspect(|x| trace!("Query result {:?}", x ))
                .probe_with(&mut probe);
        });


        let reader = match input_type_name.as_str() {
            "i" => LineFileReader::open(&filename, false, true).expect("Cannot open input graph file"),
            "it" => LineFileReader::open(&filename, true, true).expect("Cannot open input graph file"),
            "s" => LineFileReader::open(&filename, false, false).expect("Cannot open input graph file"),
            "st" => LineFileReader::open(&filename, true, false).expect("Cannot open input graph file"),
            _ => panic!("Input type {} is not valid", input_type_name)
        };

        let start_time = reader.get_start_timestamp();

        let mut total_edge_counter = 0;
        let mut processed_edge_counter = 0;
        let mut last_batch_process = start_time;
        let mut edge_ts = 0;

        let mut first_window = true;

        for sge in reader {
            trace!("Next sgt from input stream {:?}", sge);
            total_edge_counter += 1;

            let edge_predicate = sge.get_label();
            edge_ts = sge.get_timestamp();

            // do not computation and measurements until slide is full for the first time
            if edge_ts - start_time >= slide_size {
                // perform the first flush
                if first_window {
                    first_window = false;

                    //update batch process marker
                    last_batch_process = edge_ts;
                    // update edge_counter for accurate measuring
                    total_edge_counter = 0;
                    processed_edge_counter = 0;

                    // advance input so that computation is performed
                    input.advance_to(edge_ts);
                    worker.step_while(|| probe.less_than(input.time()));
                    info!("Window is fully populated at {} after {}", edge_ts, timer.elapsed().as_secs());
                } else if edge_ts - last_batch_process >= slide_size {
                    // perform  slide and measure elapsed time
                    trace!("Slide at {}", edge_ts);
                    last_batch_process = edge_ts;

                    // computation timer
                    let start = sink.now();

                    // advance time to trigger computation
                    input.advance_to(edge_ts);
                    worker.step_while(|| probe.less_than(input.time()));
                    info!("Input advance to: {} after {} secs", edge_ts, timer.elapsed().as_secs());

                    sink.record_timing("batch-latency", start, sink.now());
                    sink.record_value("batch-size", processed_edge_counter);
                    sink.record_timing("total-latency", batch_start, sink.now());
                    sink.record_value("total-size", total_edge_counter);

                    // reset edge-counter to count #of edges for next batch
                    processed_edge_counter = 0;
                    total_edge_counter = 0;

                    // reset timer
                    batch_start = sink.now();
                }
            }

            // check if the edge predicate matches input label
            if edge_predicates.iter().any(|p| p == edge_predicate) {
                // then update input stream and increment edge counter
                input.send( sge);
                processed_edge_counter += 1;
            }

        }
        // advance input to last seen edge_ts to ensure all standing tuples are processed
        input.advance_to(edge_ts + 1);
        worker.step_while(|| probe.less_than(input.time()));
        trace!("Input processing has ended {}", edge_ts);

        // measure total time to execute the entire input
        sink.record_value("total-time", timer.elapsed().as_secs());
    }).unwrap(); // asserts error-free execution;
    thread::sleep(Duration::from_millis(REPORTING_PERIOD_MILLISECONDS));
}
