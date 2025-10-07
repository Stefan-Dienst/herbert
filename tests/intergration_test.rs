use std::process::Stdio;
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::Duration;

use herbert::client::{consume, produce};

struct ServerGuard {
    child: Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server() -> ServerGuard {
    let child = Command::new("target/debug/herbert")
        // .env("RUST_LOG", "info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to ssart the herbert server");

    sleep(Duration::from_secs(1));

    ServerGuard { child }
}

#[test]
fn test_publishing_and_consuming_message() {
    let broker = "127.0.0.1:9001";
    let topic = "test";

    let _child = start_server();

    let _ = produce(broker, topic, "ok");

    sleep(Duration::from_secs(1));
    let records = consume(broker, topic, 1, "");
    assert!(records.unwrap() == vec!["ok".as_bytes()]);
}

#[test]
fn test_offsets_for_consuming() {
    let broker = "127.0.0.1:9001";
    let topic = "test2";

    let _child = start_server();

    let _ = produce(broker, topic, "1");
    sleep(Duration::from_secs(1));
    let _ = produce(broker, topic, "2");
    sleep(Duration::from_secs(1));
    let _ = produce(broker, topic, "3");

    sleep(Duration::from_secs(2));
    let records = consume(broker, topic, 1, "consumer-1");
    assert_eq!(
        records.unwrap(),
        vec!["1".as_bytes(), "2".as_bytes(), "3".as_bytes()]
    );
    let records = consume(broker, topic, 1, "consumer-1");
    assert_eq!(records.unwrap().len(), 0);

    let _ = produce(broker, topic, "4");
    sleep(Duration::from_secs(1));
    let _ = produce(broker, topic, "5");

    sleep(Duration::from_secs(2));
    let records = consume(broker, topic, 1, "consumer-2");
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

    let records = consume(broker, topic, 1, "consumer-1");
    assert_eq!(records.unwrap(), vec!["4".as_bytes(), "5".as_bytes()]);

    let records = consume(broker, topic, 1, "consumer-2");
    assert_eq!(records.unwrap().len(), 0);
}
