# Streaming Graph Query Processor

Our prototype implementation for a streaming graph query processor as a part of [S-Graffito](https://dsg-uwaterloo.github.io/s-graffito/).

It is based on `Streaming Graph Algebra` that we propose to precisely describes semantics of complex graph queries over streaming graphs. 
Our prototype uses [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow) as the underlying execution engine.
Operators of our query processor are implemented as Timely operators, 
and streaming graph queries are formulated as dataflow computation to be executed on the underlying Timely execution engine.

For more details about our framework, please refer to our [paper](arxiv-link)

### Setup
Being implemented on top of Timely Dataflow, SGQ processor is implemented in Rust and use Cargo package manager.
To run SGQ processor, download the source code, install Cargo and run the following:

```
$ cd sgraffito-query && cargo build --release
```

### Usage

We provide two helper utility to execute streaming graph queries on both using SGA operators and [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) operators, namely `sga-runner` and `dd-runner`.

To execute a query:

```$ cargo run --example [dd|sga]-runner window slide input_type input_file output_dir query arguments [predicates]```

* `window` the size of the time-based sliding window 
* `slide` slide interval that controls the granularity of window movements
* `input_type` possible values:
  1. `s` string vertex identifiers & no source timestamp
  2. `st` string vertex identifiers & timestamped by source
  3. `i` integer vertex identifiers & no source timestamp
  4. `it` integer vertex identifiers & timestamped by source
* `input_file` absolute path to input file
* `output_dir` absolute path for directory to log runtime metrics
* `query` Tha name of the streaming graph query from the Table 1 of our paper
* `arguments` # of arguments for a particular `query`
* `predicates` Arguments (edge labels) for the `query`

Input files have the following format (if the input is not timestamped, use `s` or `i` for the `input_type` parameter):
```source_identifier edge_label target_identifier [timestamp]```

### Reproducibility

We provide a helper python script (`scripts/test-runner.py`), a set of configuration files (`config`) to reproduce the experiments presented in our paper.
Additionally, datasets we used in the experiments in a pre-processed form can be downloaded from [here](https://vault.cs.uwaterloo.ca/s/6PyPGJfJQ6zcmKD).
Otherwise, the following scripts require input files to have no self-edges and to include one edge per line, 
where each edge is represented by `source_vertex`, `label`, `target_vertex`, and `unix_timestamp` separated by whitespace.


To run a particular experiment from a configuration file:

``` $ scripts/test-runner.py config/[configuration_file]```

Each configuration file defines a set of experiments over a single dataset with multiple values for other parameters. An example configuration:

```
{
    "name" : "so",
        "dataset" : "path to stackoverflow dataset",
        "input-type" : "it",
        "report-folder" : "so-results",
        "timeout" : 600,
        "project-base" : "project base directory",
        "runs" : [
        {
            "query-name" : "query1",
            "index" : "1",
            "exec-name": "sga-runner",
            "window-size" : 864000,
            "slide-size" : 86400,
            "predicates" : [
                "a2q"
            ]
        },
        {
            "query-name" : "query2",
            "index" : "1",
            "exec-name": "sga-runner",
            "window-size" : 864000,
            "slide-size" : 86400,
            "predicates" : [
                "a2q",
                "c2q"
            ]
        }
    ]
}
```

This configuration files specifies 2 runs over the StackOverflow dataset for `query1` and `query2` with 10 day windows and 1 day slide intervals.
To use configuration files, please set `dataset`, `report-folder`, and `project-base` parameters based on your local setup.