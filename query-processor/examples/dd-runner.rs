extern crate differential_dataflow;
extern crate timely;

use std::collections::VecDeque;
use std::thread;
use std::time::Duration;


use differential_dataflow::input::InputSession;

use log::{info, trace};
use metrics_runtime::Receiver;

use timely::dataflow::operators::probe::Handle;

use sgraffito_query::input::{GraphEdge, InputFileReader, LineFileReader, StreamingGraphEdge};
use sgraffito_query::util::metrics::csv_builder::CSVBuilder;
use sgraffito_query::util::metrics::csv_exporter::CSVExporter;
use sgraffito_query::util::types::REPORTING_PERIOD_MILLISECONDS;

use sgraffito_query::query::query_library::DDQueryLibrary;

/// Utility to run StreamingGraphQueries on DD-based query processor prototype. Arguments
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

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(7), move |worker| {
        // initialize sink
        let mut sink = receiver.sink();


        let mut input1 = InputSession::new();
        let mut probe = Handle::new();

        let mut batch_start = sink.now();

        let timer = ::std::time::Instant::now();

        let query_arguments = edge_predicates.clone();
        // create a TC differential dataflow
        worker.dataflow::<u64,_,_>(|scope| {

            let input_stream = input1.to_collection(scope);

            let result = match query_name.as_str() {
                "join" => {
                    DDQueryLibrary::hash_join(input_stream, query_arguments, "join".to_string())
                },
                "query1" => {
                    DDQueryLibrary::query1(input_stream, query_arguments, "q1".to_string())
                },
                "query2" => {
                    DDQueryLibrary::query2(input_stream, query_arguments, "q2".to_string())
                },
                "query3" => {
                    DDQueryLibrary::query3(input_stream, query_arguments, "q3".to_string())
                },
                "query4" => {
                    DDQueryLibrary::query4(input_stream, query_arguments, "q4".to_string())
                },
                "query5" => {
                    DDQueryLibrary::query5(input_stream, query_arguments, "q5".to_string())
                },
                "query6" => {
                    DDQueryLibrary::query6(input_stream, query_arguments, "q6".to_string())
                },
                "query6-cq" => {
                    DDQueryLibrary::query6_cq(input_stream, query_arguments, "q6".to_string())
                },
                "query7" => {
                    DDQueryLibrary::query7(input_stream, query_arguments, "q7".to_string())
                },
                "query7-cq" => {
                    DDQueryLibrary::query7_cq(input_stream, query_arguments, "q7".to_string())
                },
                "query8" => {
                    DDQueryLibrary::query8(input_stream, query_arguments, "q8".to_string())
                },
                _ => {
                    panic!("Supplied query name is not defined: {}", &query_name);
                }
            };

            result.inspect(|x| trace!("Query result {:?}", x))
                .probe_with(&mut probe);

        });

        // read graph data from file
        let reader = match input_type_name.as_str() {
            "i" => LineFileReader::open(&filename, false, true).expect("Cannot open input graph file"),
            "it" => LineFileReader::open(&filename, true, true).expect("Cannot open input graph file"),
            "s" => LineFileReader::open(&filename, false, false).expect("Cannot open input graph file"),
            "st" => LineFileReader::open(&filename, true, false).expect("Cannot open input graph file"),
            _ => panic!("Input type {} is not valid", input_type_name)
        };

        // Vector to store window content
        let mut window_content1: VecDeque<StreamingGraphEdge> = VecDeque::new();

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
            edge_ts = sge.timestamp;

            // do not start computation and measurements until window is full for the first time
            if edge_ts - start_time >= window_size {
                // perform first window flush
                if first_window {
                    first_window = false;

                    // update batch process marker
                    last_batch_process = edge_ts;
                    // update edge_counter for accurate measuring
                    total_edge_counter = 0;
                    processed_edge_counter = 0;

                    // advance input so that computation of the first window is performed
                    input1.advance_to(edge_ts);
                    input1.flush();
                    worker.step_while(|| probe.less_than(input1.time()));
                    info!("Window is fully populated at {} after {} secs", edge_ts, timer.elapsed().as_secs());
                } else if edge_ts - last_batch_process >= slide_size {
                    // perform window slide and measure elapsed time
                    trace!("Slide at {}", edge_ts);
                    last_batch_process = edge_ts;

                    // determine expired tuples from the window content and push negative tuples
                    while !window_content1.is_empty() {
                        let sgt = window_content1.pop_front().unwrap();
                        if sgt.timestamp + window_size <= edge_ts {
                            input1.update(sgt.clone(), -1);
                        } else {
                            // place it back to window and break
                            window_content1.push_front(sgt);
                            break;
                        }
                    }

                    // computation timer
                    let start = sink.now();

                    // advance time to trigger computation
                    input1.advance_to(edge_ts);
                    input1.flush();
                    worker.step_while(|| probe.less_than(input1.time()));
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

            // propagate update only if its used by the query
            if edge_predicates.iter().any(|p| p == edge_predicate) {
                // update input collection and the window content
                input1.update(sge.clone(), 1);
                window_content1.push_back(sge);
                processed_edge_counter += 1;
            }
        }
        // advance input1 to last seen edge_ts to ensure all standing tuples are processed
        input1.advance_to(edge_ts + 1);
        input1.flush();
        worker.step_while(|| probe.less_than(input1.time()));
        trace!("Input processing has ended {}", edge_ts);

        // measure total time to execute the entire input
        sink.record_value("total-time", timer.elapsed().as_secs());
    }).unwrap();
    thread::sleep(Duration::from_millis(REPORTING_PERIOD_MILLISECONDS));
}
