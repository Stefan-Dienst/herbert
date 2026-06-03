use std::io;
use std::string::FromUtf8Error;

use arrow_schema::ArrowError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HerbertError {
    #[error("io error: {0}")]
    IO(#[from] io::Error),

    #[error("io error: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    #[error("error computing size")]
    ComputeSize,

    #[error("decoding failed")]
    Decode,

    #[error("encoding failed")]
    Encode,

    #[error("Lock poisoned")]
    PoisonError,

    #[error("serialization failed")]
    Serialization,

    #[error("deserialization failed")]
    Deserialization,

    #[error("unknown  indicator encountered: {0}")]
    IndicatorError(u8),

    #[error("no topic data found")]
    NoTopicData,

    #[error("topic list is empty")]
    EmptyTopicList,

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

    #[error("no WAL file found")]
    NoWalFileFound,

    #[error("no offset file found")]
    NoOffsetFileFound,

    #[error("request could not be decoded")]
    RequestDecode,

    #[error("something unexpected happened")]
    UnknownError,
}
