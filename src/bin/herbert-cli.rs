use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use clap::{Parser, Subcommand};
use herbert::client::consume_continuos;
use herbert::client::create_topic;

use herbert::client::produce;
use herbert::client::produce_record_batch;

#[derive(Parser, Debug)]
#[command(name = "herbert--cli")]
#[command(about = "This is herbert, say hello to him.", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Produce a message to herbert
    Produce {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic to which you want to produce to
        #[arg(short, long)]
        topic: String,

        /// The message you want to produce
        #[arg(short, long)]
        message: String,
    },

    /// Produce a record batch from a given json file with a given schema
    ProduceRecordBatch {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic to produce to
        #[arg(short, long)]
        topic: String,

        /// Path to JSON file with record data
        #[arg(short, long)]
        data_path: String,

        /// Path to schema JSON file
        #[arg(short, long)]
        schema_path: String,
    },

    /// Consume message(s) from herbert
    Consume {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic from which you want to comsume to
        #[arg(short, long)]
        topic: String,

        /// The number of message you want to consume in maximum
        #[arg(short, long)]
        max_messages: i32,

        /// The name of the consumer group that shall be used
        #[arg(short, long)]
        consumer_group: String,
    },

    /// Create a topic in Herbert
    CreateTopic {
        /// Herbert broker address, e.g. 127.0.0.1:9092
        #[arg(short, long)]
        broker: String,

        /// The topic which shall be created
        #[arg(short, long)]
        topic: String,

        /// Path to a JSON file where an arrow schema is defined.
        #[arg(short, long)]
        schema_path: Option<String>,
    },
}

fn load_schema_from_json(schema_path: &str) -> Result<Schema> {
    let data = fs::read_to_string(schema_path)?;
    let schema: Schema = serde_json::from_str(&data)?;
    Ok(schema)
}

fn load_record_batch_from_json(data_path: &str, schema: SchemaRef) -> Result<RecordBatch> {
    let file = File::open(data_path).unwrap();

    let mut json = arrow_json::ReaderBuilder::new(schema)
        .build(BufReader::new(file))
        .unwrap();
    let batch = json.next().unwrap().unwrap();
    Ok(batch)
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    match args.command {
        Command::Produce {
            broker,
            topic,
            message,
        } => {
            let _ = produce(&broker, &topic, &message);
        }
        Command::ProduceRecordBatch {
            broker,
            topic,
            data_path,
            schema_path,
        } => {
            let schema = match load_schema_from_json(&schema_path) {
                Ok(schema) => schema,
                Err(e) => {
                    eprintln!("Error loading schema from {}: {}", schema_path, e);
                    return;
                }
            };
            match load_record_batch_from_json(&data_path, Arc::new(schema)) {
                Ok(record_batch) => {
                    let _ = produce_record_batch(&broker, &topic, &record_batch);
                }
                Err(e) => {
                    eprintln!("Error loading the data from {}: {}", data_path, e)
                }
            }
        }
        Command::Consume {
            broker,
            topic,
            max_messages,
            consumer_group,
        } => {
            let _ = consume_continuos(&broker, &topic, max_messages, &consumer_group);
        }
        Command::CreateTopic {
            broker,
            topic,
            schema_path,
        } => {
            let schema = match schema_path {
                Some(ref path) => match load_schema_from_json(path) {
                    Ok(schema) => Some(schema),
                    Err(e) => {
                        eprintln!("Error loading schema from {}: {}", path, e);
                        return;
                    }
                },
                None => None,
            };

            if let Err(e) = create_topic(&broker, &topic, schema) {
                eprintln!("Failed to create topic: {}", e);
            }
        }
    };
}
