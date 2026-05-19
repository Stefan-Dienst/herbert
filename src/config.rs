use std::env;
use std::path::{Path, PathBuf};

pub struct Config {
    pub kafka_port: u16,
    pub herbert_port: u16,
    pub num_uncommitted_messages: usize,
    pub wal_path: PathBuf,
    pub offset_path: PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            kafka_port: env::var("KAFKA_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(9001),
            herbert_port: env::var("HERBERT_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(9002),
            num_uncommitted_messages: env::var("NUM_UNCOMMITTED_MESSAGES")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1),
            wal_path: env::var("WAL_PATH")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(PathBuf::from("herbert.wal")),
            offset_path: env::var("OFFSET_PATH")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(PathBuf::from("herbert_offset.json")),
        }
    }

    pub fn default() -> Self {
        Self {
            kafka_port: 9001,
            herbert_port: 9002,
            num_uncommitted_messages: 1,
            wal_path: PathBuf::from("herbert.wal"),
            offset_path: PathBuf::from("herbert_offset.json"),
        }
    }

    pub fn test_default() -> Self {
        Self {
            kafka_port: 9001,
            herbert_port: 9002,
            num_uncommitted_messages: 1,
            wal_path: PathBuf::from("test.wal"),
            offset_path: PathBuf::from("test_offset.json"),
        }
    }

    pub fn with_wal_path(self, path: &Path) -> Self {
        Self {
            wal_path: PathBuf::from(path),
            ..self
        }
    }

    pub fn with_offset_path(self, path: &Path) -> Self {
        Self {
            offset_path: PathBuf::from(path),
            ..self
        }
    }
}
