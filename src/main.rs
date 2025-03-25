use log::{error, info};
use std::{
    io::Read,
    net::{TcpListener, TcpStream},
};

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
}

impl RequestHeader {
    fn parse(buffer: &[u8; 512]) -> Self {
        let mut cursor = Cursor::new(&buffer);

        let size = cursor.read_i32::<BigEndian>().unwrap();

        let request_api_key = cursor.read_i16::<BigEndian>().unwrap();
        let request_api_version = cursor.read_i16::<BigEndian>().unwrap();
        let correlation_id = cursor.read_i32::<BigEndian>().unwrap();
        let size_of_client_id = cursor.read_i16::<BigEndian>().unwrap();

        let mut unparsed_client_id = vec![0; size_of_client_id.try_into().unwrap()];
        cursor.read_exact(&mut unparsed_client_id).unwrap();

        let client_id = String::from_utf8(unparsed_client_id).expect("Invalid utf8 sequence");
        info!("This is the client id: {:?}", client_id);
        RequestHeader {
            request_api_key: request_api_key,
            request_api_version: request_api_version,
            correlation_id: correlation_id,
            client_id: client_id,
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    info!("I have received a connection!");
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                info!("Client disconnected.");
                break;
            }
            Ok(n) => {
                let header = RequestHeader::parse(&buffer);
                info!("{:?}", header)
            }
            Err(..) => {
                error!("Error");
                break;
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    add(1, 2);
    let adress = "127.0.0.1:9001";
    info!("Starting the TCP server. Listening on {:?}", adress);
    let listener = TcpListener::bind(adress)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream);
            }
            Err(..) => {
                error!("Oh oh!");
            }
        }
    }
    Ok(())
}

fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 1 + 2)
    }
}
