# Herbert

## Introduction

Herbert is a Kafka-inspired event streaming system built in Rust.
Its goal is to act as a data exchange point for transactional and analytic workloads building on top of stream processing patterns, while still allowing batch SQL workflows.
It should blur the border between batch and stream processing by allowing to easily switch between them. 
Key foundation for this is the [stream-table duality](https://kafka.apache.org/42/streams/core-concepts/#duality-of-streams-and-tables), where in contrast to Kafka, every Topic is not just a log, but also materialized as a table.

## Current implementation

Currently Herbert is a simple single instance multi-threaded TCP server, that implements a small subset of the Kafka protocol and missing functionality in its own Herbert protocol.
For the topic implementation it uses an in-memory log which is backed by a [write-ahead log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging) for durability.
Either arbitrary bytes or [Apache Arrow](https://arrow.apache.org/) RecordBatches can be pushed to it, for the latter the topic is schema-aware and validates incoming data first.
Consumers of a topic can commit offsets on a record level, which are also persisted for durability.

## Usage

Start a Herbert instance by running

```sh
cargo run --bin herbert
```

To create a schema aware topic, first prepare a schema in a json format, e.g.

```json
{
  "fields": [
    {
      "name": "id",
      "nullable": false,
      "data_type": "Int64",
      "metadata": {},
      "dict_id": 0,
      "dict_is_ordered": false
    },
    {
      "name": "name",
      "nullable": false,
      "data_type": "Utf8",
      "metadata": {},
      "dict_id": 0,
      "dict_is_ordered": false
    }
  ],
  "metadata": {}
}
```

and then use

```
cargo run --bin herbert-cli create-topic --broker 127.0.0.1:9002 --topic foobar --schema-path schema.json
```

To produce data prepare records as a JSON line file, e.g.

```jsonl
{"id": 1, "name": "Stefan"}
{"id": 2, "name": "Herbert"}
```

and then run

```sh
cargo run --bin herbert-cli produce-record-batch --broker 127.0.0.1:9001 --topic foobar --schema-path schema.json --data-path data.jsonl
```

To consume the data run:

```sh
cargo run --bin herbert-cli consume --broker 127.0.0.1:9001 --topic foobar --consumer-group "xyz"
```


## Roadmap


### Polish current state

 - [x] Create error.rs with comprehensive error types
 - [x] Remove all unwrap() and panic() calls
 - [ ] Add graceful shutdown handling
 - [x] Write README.md, LICENSE
 - [ ] Add inline documentation to all public APIs
 - [ ] Add unit tests for all modules
 - [ ] Add integration tests (produce/consume flows)
 - [ ] Add config file support (figment + TOML)
 - [ ] Set up GitHub Actions CI

### Features
 - [ ] Add background task that compacts in-memory Arrow Topic to table using either [parquet](https://parquet.apache.org/) or [vortex](https://vortex.dev/).
 - [ ] Add support for open table formats. 
 - [ ] Add a python API with [PyO3](https://pyo3.rs/v0.28.3/).
 - [ ] Use [tokio](https://docs.rs/tokio/latest/tokio/) runtime.
 - [ ] Use object storage for durability, see [object_store](https://docs.rs/object_store/latest/object_store/). 
 - [ ] Improve concurrent performance by replacing RwLock
 - [ ] Integrate SQL API with [Apache Datafusion](https://datafusion.apache.org/)

