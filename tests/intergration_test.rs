use std::fmt::format;
use std::net::TcpStream;
use std::process::Stdio;
use std::process::{Child, Command};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use arrow_array::{record_batch, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use herbert::client::{
    consume, consume_record_batches, create_topic, produce, produce_record_batch,
};
use tempfile::TempDir;

struct ServerGuard {
    child: Child,
    _temp_dir: TempDir,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_on_server(broker: &str, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if TcpStream::connect(broker).is_ok() {
            return true;
        }
    }
    false
}

fn start_server(kafka_port: u16, herbert_port: u16) -> ServerGuard {
    let temp_dir = TempDir::new().expect("should be able to create tmp dir");
    let wal_path = temp_dir.path().join("test.wal");
    let offset_path = temp_dir.path().join("test_offset.json");

    let child = Command::new("target/debug/herbert")
        .env("RUST_LOG", "off")
        .env("KAFKA_PORT", kafka_port.to_string())
        .env("HERBERT_PORT", herbert_port.to_string())
        .env("WAL_PATH", wal_path.to_str().unwrap())
        .env("OFFSET_PATH", offset_path.to_str().unwrap())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to start the herbert server");

    let broker = format!("127.0.0.1:{}", kafka_port);
    if !wait_on_server(&broker, 5) {
        panic!("Could not start server.")
    }

    ServerGuard {
        child,
        _temp_dir: temp_dir,
    }
}

#[test]
fn test_publishing_and_consuming_message() {
    let kafka_port = 9001;
    let herbert_port = 9002;
    let broker = format!("127.0.0.1:{}", kafka_port);
    let topic = "test";

    let _child = start_server(kafka_port, herbert_port);

    let _ = produce(&broker, topic, "ok");

    sleep(Duration::from_secs(1));
    let records = consume(&broker, topic, 1, "");
    assert!(records.unwrap() == vec!["ok".as_bytes()]);
}

#[test]
fn test_offsets_for_consuming() {
    let kafka_port = 9003;
    let herbert_port = 9004;
    let broker = format!("127.0.0.1:{}", kafka_port);
    let topic = "test2";

    let _child = start_server(kafka_port, herbert_port);

    let _ = produce(&broker, topic, "1");
    sleep(Duration::from_secs(1));
    let _ = produce(&broker, topic, "2");
    sleep(Duration::from_secs(1));
    let _ = produce(&broker, topic, "3");

    sleep(Duration::from_secs(2));
    let records = consume(&broker, topic, 1, "consumer-1");
    assert_eq!(
        records.unwrap(),
        vec!["1".as_bytes(), "2".as_bytes(), "3".as_bytes()]
    );
    let records = consume(&broker, topic, 1, "consumer-1");
    assert_eq!(records.unwrap().len(), 0);

    let _ = produce(&broker, topic, "4");
    sleep(Duration::from_secs(1));
    let _ = produce(&broker, topic, "5");

    sleep(Duration::from_secs(2));
    let records = consume(&broker, topic, 1, "consumer-2");
    assert_eq!(
        records.unwrap(),
        vec![
            "1".as_bytes(),
            "2".as_bytes(),
            "3".as_bytes(),
            "4".as_bytes(),
            "5".as_bytes()
        ]
    );

    let records = consume(&broker, topic, 1, "consumer-1");
    assert_eq!(records.unwrap(), vec!["4".as_bytes(), "5".as_bytes()]);

    let records = consume(&broker, topic, 1, "consumer-2");
    assert_eq!(records.unwrap().len(), 0);
}

#[test]
fn test_record_batches() {
    let kafka_port = 9005;
    let herbert_port = 9006;

    let broker = "127.0.0.1";
    let kafka_api = format!("{broker}:{}", kafka_port);
    let herbert_api = format!("{broker}:{}", herbert_port);
    let topic = "record_batch";

    let _child = start_server(kafka_port, herbert_port);

    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ];
    let schema = Schema::new(fields);

    let _ = create_topic(&herbert_api, topic, Some(schema.clone()));
    sleep(Duration::from_secs(1));

    let id_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec!["herbert", "kafka"]));

    let batch = RecordBatch::try_new(Arc::new(schema), vec![id_array, name_array]).unwrap();

    let _ = produce_record_batch(&kafka_api, topic, &batch);
    sleep(Duration::from_secs(1));

    let records = consume_record_batches(&kafka_api, topic, 1, "consumer-1");
    assert_eq!(records.unwrap(), vec![batch]);
}
