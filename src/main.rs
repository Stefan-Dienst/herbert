use herbert::server;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    server::run().await
}
