//! Exports metrics into a csv file
//! mimics the LogExporter from metrics_runtime
//!
//! This exporter can utilize observers that are able to be converted to a textual representation
//! via [`Drain<String>`].  It will emit that output to a specified csv file
//!
//! # Run Modes
//! - Using `run` will block the current thread, capturing a snapshot and logging it based on the
//! configured interval.
//! - Using `async_run` will return a future that can be awaited on, mimicing the behavior of
//! `run`.
#![deny(missing_docs)]

use std::{thread, time::Duration};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use csv::Writer;
use log::trace;
use metrics_core::{Builder, Drain, Observe, Observer};

/// Exports metrics by converting them to a textual representation and print into a csv
pub struct CSVExporter<C, B>
    where
        B: Builder,
{
    controller: C,
    observer: B::Output,
    reporting_dir: PathBuf,
    interval: Duration,
    metric_writer: HashMap<String, Writer<File>>,
}

impl<C, B> CSVExporter<C, B>
    where
        B: Builder,
        B::Output: Drain<Vec<(String, Vec<String>, Vec<String>)>> + Observer,
        C: Observe,
{
    /// Creates a new [`CSVExporter`] that logs at the configurable level.
    ///
    /// Observers expose their output by being converted into strings.
    pub fn new(controller: C, builder: B, reporting_dir: &str, interval: Duration) -> Self {
        // create directory if not exists
        let dir = std::path::Path::new(reporting_dir).to_path_buf();
        match std::fs::create_dir_all(dir.as_path()) {
            Ok(_) => {
                CSVExporter {
                    controller,
                    observer: builder.build(),
                    reporting_dir: dir,
                    interval,
                    metric_writer: HashMap::new(),
                }
            }
            Err(e) => {
                panic!("Cannot create reporting directory {} {}", reporting_dir, e);
            }
        }
    }

    /// Runs this exporter on the current thread, logging output at the interval
    /// given on construction.
    pub fn run(&mut self) {
        loop {
            thread::sleep(self.interval);

            self.turn();
        }
    }

    /// Run this exporter, logging output only once.
    pub fn turn(&mut self) {
        self.controller.observe(&mut self.observer);

        // retrieve the current snapshot of metrics
        for (metric_name, headers, values) in self.observer.drain() {
            trace!("Recording merics for {}", metric_name);
            // create the full directory for the metric
            let metric_report_path = self.reporting_dir.as_path().join(Path::new(&metric_name).with_extension("csv"));
            // retrieve the writer for given metric
            let writer = self.metric_writer.entry(metric_name).or_insert_with(|| {
                // get full path
                let mut w = csv::Writer::from_path(metric_report_path).unwrap();

                // write headers
                match w.write_record(headers) {
                    Ok(_) => {
                        trace!("Writing metric headers ");
                        w.flush().expect("Cannot flush metric file");
                    }
                    Err(e) => {
                        eprintln!("Error during writing metric headers {:?}", e);
                    }
                }
                // return the writer
                w
            });

            // write records
            match writer.write_record(values) {
                Ok(_) => {
                    trace!("Writing metric values");
                    writer.flush().expect("Cannot flush metric file");
                }
                Err(e) => {
                    eprintln!("Error during writing metric values {:?}", e);
                }
            }
        }
    }
}
