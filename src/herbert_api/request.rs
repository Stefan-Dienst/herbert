use arrow_schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Request {
    CreateTopic {
        topic: String,
        schema: Option<Schema>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = Request::CreateTopic {
            topic: "test".into(),
            schema: None,
        };
        dbg!(&req);

        let encoded = serde_json::to_vec(&req).unwrap();
        dbg!(&encoded);

        let decoded: Request = serde_json::from_slice(&encoded).unwrap();
        dbg!(&decoded);

        // Simulate routing
        match decoded {
            Request::CreateTopic { topic, .. } => {
                println!("Handle CreateTopic for {}", topic);
            }
        }
    }
}
