use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, prelude::*};
use std::iter::Iterator;
use std::marker::Sized;

use log::trace;

use crate::util::types::{HalfOpenInterval, VertexType};

pub mod tuple;

// helper function to calculate hash values
fn calculate_hash<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

// enum to define various input formats
pub enum InputStreamKind {
    String, // vertices have string identifiers and edges do not carry sourcetimestamp
    StringTimestampted, // vertices have string identifiers and edges are timestamped by the source
    Integer, // vertices have integer identifiers and edges do not carry a source timestamp
    IntegerTimestamped, // vertices have integer identifiers and edges are timestamped by the source
}

/// Trait for Static graph edges
pub trait GraphEdge {
    fn get_source(&self) -> VertexType;
    fn get_target(&self) -> VertexType;
    fn get_label(&self) -> &str;
}

/// Trait for Streaming graph edges
pub trait SGE: GraphEdge {
    fn new(source: VertexType, target: VertexType, label: String, timestamp: u64) -> Self;
    fn get_timestamp(&self) -> u64;
}

/// Trait for Streaming graph tuples
pub trait SGT<T: HalfOpenInterval, E: GraphEdge>: GraphEdge {
    fn from_edge(streaming_graph_edge: &E, interval: T) -> Self;
    fn new(source: VertexType, target: VertexType, label: String, interval: T) -> Self;
    fn get_interval(&self) -> T;
}

/// Streaming Graph edges that are provided by a source
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct StreamingGraphEdge {
    pub source: u64,
    pub target: u64,
    pub label: String,
    pub timestamp: u64,
    pub append: bool,
}

impl GraphEdge for StreamingGraphEdge {
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

impl SGE for StreamingGraphEdge {
    fn new(s: u64, t: u64, l: String, ts: u64) -> Self {
        Self { source: s, target: t, label: l, timestamp: ts, append: true }
    }
    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Trait for FileBased input streams
pub trait InputFileReader: Iterator {
    fn open(input_file: &str, has_timestamp: bool, integer_ids: bool) -> Result<Self, std::io::Error> where Self: Sized;
    fn close(&self);
    fn get_start_timestamp(&self) -> u64;
}

/// Integer based file reader, edge endpoints are hashed
pub struct LineFileReader {
    reader: BufReader<File>,
    start_timestamp: u64,
    is_timestamped: bool,
    integer_ids: bool,
    current_timestamp: u64,
    first_line: Option<String>,
}

impl Iterator for LineFileReader {
    type Item = StreamingGraphEdge;

    fn next(&mut self) -> Option<StreamingGraphEdge> {
        let mut line_fields: Vec<String> = Vec::new();

        if self.first_line.is_some() && self.is_timestamped {
            let line = self.first_line.as_ref().unwrap().to_string();
            line_fields = line.split_whitespace().map(|s| s.to_string()).collect();
            self.first_line = None;
        } else {
            while self.is_timestamped && line_fields.len() < 4 || line_fields.len() < 3 {
                let mut line = String::new();

                let len = self.reader.read_line(&mut line).expect("Error reading the next line from input stream");

                if len == 0 {
                    return None;
                }

                line_fields = line.split_whitespace().map(|s| s.to_string()).collect();
                if self.is_timestamped && line_fields.len() < 4 {
                    continue;
                } else if line_fields.len() < 3 {
                    continue;
                }

                trace!("Next line from input stream {}", line);
            }
        }

        let source = if self.integer_ids {
            line_fields[0].parse().unwrap()
        } else {
            calculate_hash(&line_fields[0])
        };
        let edge_predicate = &line_fields[1];

        let target = if self.integer_ids {
            line_fields[2].parse().unwrap()
        } else {
            calculate_hash(&line_fields[2])
        };
        let edge_ts: u64 = if self.is_timestamped {
            line_fields[3].parse().unwrap()
        } else {
            self.current_timestamp + 1
        };

        // update the current timestamp
        self.current_timestamp = edge_ts;

        Some(StreamingGraphEdge::new(source, target, edge_predicate.to_string(), edge_ts))
    }
}

impl InputFileReader for LineFileReader {
    /// initialize a Filesed input reader
    fn open(input_file: &str, has_timestamp: bool, integer_ids: bool) -> Result<Self, std::io::Error> {
        let mut file_reader = BufReader::new(File::open(input_file).expect("Cannot open input file"));

        let mut first_ts = 0;

        let mut first_line = None;

        // if input does not have timestamp, use incremental counters
        if has_timestamp {
            let mut line = String::new();
            file_reader.read_line(&mut line).expect("Cannot open input graph file");
            first_ts = line.split_whitespace().nth(3).unwrap().parse().unwrap();
            first_line = Some(line);

            trace!("First line read while opening -- {:?}", first_line);
        }

        // create the file reader object
        Ok(Self { reader: file_reader, start_timestamp: first_ts, is_timestamped: has_timestamp, integer_ids: integer_ids, current_timestamp: first_ts, first_line: first_line })
    }

    fn close(&self) {
        unimplemented!()
    }

    fn get_start_timestamp(&self) -> u64 {
        self.start_timestamp
    }
}