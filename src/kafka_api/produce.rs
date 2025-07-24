use anyhow::Result;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use log::{error, info};
use std::sync::Arc;

use crate::topic_manager::TopicManager;

pub fn create_produce_request(topic: &str, record: Bytes) -> ProduceRequest {
    let mut produce_request = ProduceRequest::default();
    let topic_to_produce_to = create_topic_produce_data(topic, record);
    produce_request.topic_data.push(topic_to_produce_to);
    produce_request
}

fn create_topic_produce_data(topic: &str, record: Bytes) -> TopicProduceData {
    let mut topic_to_produce_to = TopicProduceData::default();
    topic_to_produce_to.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    let mut things_to_produce = PartitionProduceData::default();
    things_to_produce.records = Some(record);

    topic_to_produce_to.partition_data.push(things_to_produce);
    topic_to_produce_to
}

pub fn handle_produce_request(
    buf: Bytes,
    api_version: i16,
    topic_manager: &Arc<TopicManager>,
) -> Result<ProduceResponse> {
    let produce_request = ProduceRequest::decode(&mut Bytes::from(buf), api_version);
    match produce_request {
        Ok(ProduceRequest { topic_data, .. }) => {
            handle_topic_data(topic_data, topic_manager)?;
        }
        _ => {
            error!("Something wrong with the produce request.")
        }
    }
    Ok(ProduceResponse::default())
}

fn handle_topic_data(
    topic_data: Vec<TopicProduceData>,
    topic_manager: &Arc<TopicManager>,
) -> Result<()> {
    let first_topic = topic_data
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("No topic data provided"))?;

    let topic_name = &first_topic.name;

    let partition = first_topic
        .partition_data
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("No partition data available"))?;

    let records = partition
        .records
        .clone()
        .ok_or_else(|| anyhow::anyhow!("No records available"))?;

    topic_manager.add(topic_name, records);
    Ok(())
}

#[cfg(test)]
mod tests {
    use kafka_protocol::{messages, protocol::Encodable};

    use super::*;

    #[test]
    fn test_handle_topic_data() {
        let topic_name = "test";
        let record = Bytes::from("yeah");

        let topic_manager = Arc::new(TopicManager::default());
        let produce_request = create_produce_request(&topic_name, record.clone());

        let _ = handle_topic_data(produce_request.topic_data, &topic_manager);

        let message = topic_manager.fetch(topic_name, 0);

        assert_eq!(*message.unwrap(), record)
    }

    #[test]
    fn test_handle_topic_data_failure() {
        let produce_request = ProduceRequest::default();
        let topic_manager = Arc::new(TopicManager::default());

        let result = handle_topic_data(produce_request.topic_data, &topic_manager);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_produce_request() {
        let topic_name = "test";
        let record = Bytes::from("yeah");

        let topic_manager = Arc::new(TopicManager::default());
        let produce_request = create_produce_request(&topic_name, record.clone());

        let mut request_buffer = BytesMut::new();
        let produce_request_api_version = 9;
        let _ = produce_request.encode(&mut request_buffer, produce_request_api_version);
        let _ = handle_produce_request(
            request_buffer.into(),
            produce_request_api_version,
            &topic_manager,
        );

        let message = topic_manager.fetch(topic_name, 0);

        assert_eq!(*message.unwrap(), record)
    }

    #[test]
    fn test_handle_produce_request_failure() {
        let produce_request = ProduceRequest::default();
        let topic_manager = Arc::new(TopicManager::default());

        let mut request_buffer = BytesMut::new();
        let produce_request_api_version = 9;
        let _ = produce_request.encode(&mut request_buffer, produce_request_api_version);
        let result = handle_produce_request(
            request_buffer.into(),
            produce_request_api_version,
            &topic_manager,
        );
        assert!(result.is_err());
    }
}
