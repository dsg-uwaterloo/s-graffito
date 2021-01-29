use std::collections::BTreeMap;
use std::time::SystemTime;

use hdrhistogram::Histogram;
use metrics_core::{Builder, Drain, Key, Observer};
use metrics_util::{parse_quantiles, Quantile};

pub enum MetricValue {
    Unsigned(u64),
    Signed(i64),
    Hist(Histogram<u64>),
}

/// Custom CSV-Based metric reporting
pub struct CSVBuilder {}

impl CSVBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl Builder for CSVBuilder {
    type Output = CSVObserver;

    fn build(&self) -> Self::Output {
        CSVObserver {
            quantiles: parse_quantiles(&[0.25, 0.5, 0.75, 0.9, 0.99, 0.999]),
            content: BTreeMap::new(),
        }
    }
}

impl Default for CSVBuilder {
    fn default() -> Self { Self::new() }
}

pub struct CSVObserver {
    pub(crate) quantiles: Vec<Quantile>,
    pub(crate) content: BTreeMap<String, MetricValue>,
}

impl Observer for CSVObserver {
    fn observe_counter(&mut self, key: Key, value: u64) {
        *self.content.entry(key.name().to_string()).or_insert(MetricValue::Unsigned(value)) = MetricValue::Unsigned(value);
    }

    fn observe_gauge(&mut self, key: Key, value: i64) {
        *self.content.entry(key.name().to_string()).or_insert(MetricValue::Signed(value)) = MetricValue::Signed(value);
    }

    fn observe_histogram(&mut self, key: Key, values: &[u64]) {
        let entry = self
            .content
            .entry(key.name().to_string())
            .or_insert_with(|| MetricValue::Hist(Histogram::<u64>::new(4).expect("failed to create histogram")));

        if let MetricValue::Hist(hist) = entry {
            for value in values {
                hist
                    .record(*value)
                    .expect("failed to observe histogram value");
            }
        }
    }
}


impl Drain<Vec<(String, Vec<String>, Vec<String>)>> for CSVObserver {
    fn drain(&mut self) -> Vec<(String, Vec<String>, Vec<String>)> {
        let mut measurements = Vec::new();

        // report all measurements
        for (key, value) in self.content.iter() {
            let mut headers = Vec::new();
            let mut values = Vec::new();

            match value {
                MetricValue::Hist(val) => {
                    let hist_pairs = hist_to_values(val, &self.quantiles);
                    for (hist_label, hist_value) in hist_pairs {
                        headers.push(hist_label);
                        values.push(hist_value.to_string());
                    }
                }
                MetricValue::Unsigned(val) => {
                    headers.push(key.to_string());
                    values.push(val.to_string());
                }
                MetricValue::Signed(val) => {
                    headers.push(key.to_string());
                    values.push(val.to_string());
                }
            }

            if !headers.is_empty() && !values.is_empty() {
                // insert system time as the first column
                headers.insert(0, "timestamp".to_string());
                values.insert(0, SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().to_string());
                measurements.push((key.to_string(), headers, values))
            }
        }

        // return measurements
        measurements
    }
}

/// Creates list of (percentile-value) pairs from a given Histogram
fn hist_to_values(
    hist: &Histogram<u64>,
    quantiles: &[Quantile],
) -> Vec<(String, u64)> {
    let mut values = Vec::new();

    values.push((format!("count"), hist.len()));
    values.push((format!("{}", "mean"), hist.mean().round() as u64));

    for quantile in quantiles {
        let value = hist.value_at_quantile(quantile.value());
        values.push((format!("{}", quantile.label()), value));
    }

    values
}
