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
    let records = consume(broker, topic, 1);
    assert!(records.unwrap() == vec!["ok".as_bytes()]);
}
