use herbert::server;

pub fn main() -> std::io::Result<()> {
    env_logger::init();
    server::run()
}
