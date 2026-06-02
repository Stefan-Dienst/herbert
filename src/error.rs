use std::io;
use std::string::FromUtf8Error;

use arrow_schema::ArrowError;
use arrow_schema::Schema;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HerbertError {
    #[error("io error: {0}")]
    IO(#[from] io::Error),

    #[error("io error: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    #[error("Lock poisoned")]
    PoisonError,

    #[error("unknown  indicator encountered: {0}")]
    IndicatorError(u8),

    #[error("no topic data found")]
    NoTopicData,

    #[error("no partition data found")]
    NoPartitionData,

    #[error("no record data found")]
    NoRecordData,

    #[error("invalid arrow ipc: {0}")]
    InvalidArrowIpc(String),

    #[error("arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    #[error("invalid arrow ipc")]
    SchemaError { expected: String, found: String },

    #[error("topic {0} already exists")]
    TopicAlreadyExists(String),

    #[error("queue is empty")]
    EmptyQueue,

    #[error("something unexpected happened")]
    UnknownError,
}
