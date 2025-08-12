use arrow_schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTopic {
    topic: String,
    schema: Option<Schema>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic_serialization() {
        let create_topic = CreateTopic {
            topic: String::from("test"),
            schema: None,
        };
        dbg!(&create_topic);
        let encoded = serde_json::to_vec(&create_topic);
        dbg!(&encoded);
        let decoded: CreateTopic = serde_json::from_slice(&encoded.unwrap()).unwrap();
        dbg!(decoded);
    }
}
