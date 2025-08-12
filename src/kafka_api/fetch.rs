use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use log::{error, info};
use std::sync::Arc;

use crate::topic_manager::{self, TopicManager};

pub fn create_fetch_request(topic: &str, max_messages: i32, fetch_offset: i64) -> FetchRequest {
    let mut fetch_request = FetchRequest::default();

    // FIXME: Max bytes does not work. Herbet can't decode it somehow.
    fetch_request.max_bytes = max_messages;

    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = TopicName::from(StrBytes::from_string(topic.to_string()));
    let mut fetch_partition = FetchPartition::default();
    fetch_partition.fetch_offset = fetch_offset;
    fetch_topic.partitions.push(fetch_partition);

    fetch_request.topics.push(fetch_topic);
    fetch_request
}

pub fn handle_fetch_request(
    buf: Bytes,
    api_version: i16,
    topic_manager: &Arc<TopicManager>,
) -> Result<FetchResponse> {
    let fetch_request = FetchRequest::decode(&mut Bytes::from(buf), api_version);
    match fetch_request {
        Ok(FetchRequest {
            max_bytes, topics, ..
        }) => {
            // NOTE: Right now we always only assume one topic with one partition.
            let first_topic = topics
                .get(0)
                .ok_or_else(|| anyhow::anyhow!("No Topic data available in fetch request"))?;
            let topic_name = first_topic.topic.to_string();
            let first_partition = first_topic
                .partitions
                .get(0)
                .ok_or_else(|| anyhow::anyhow!("No partition data available in fetch request"))?;
            let fetch_offset = first_partition.fetch_offset;
            let records = topic_manager.fetch(&topic_name, fetch_offset)?;

            let mut response = FetchResponse::default();
            let mut topic_response = FetchableTopicResponse::default();
            let mut partition_data = PartitionData::default();
            partition_data.records = Some(records);
            topic_response.partitions.push(partition_data);
            response.responses.push(topic_response);
            Ok(response)
        }
        _ => {
            error!("Something wrong with the fetch request.");
            panic!();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::RwLock};

    use crate::storage::in_memory_log::InMemoryLog;

    use super::*;

    #[test]
    fn test_handle_fetch_request() {
        let topic_manager = Arc::new(TopicManager::default());
        let record = Bytes::from("test");
        topic_manager.add("foobar", record.clone());

        let fetch_request = create_fetch_request("foobar", 3, 0);
        let mut request_buffer = BytesMut::new();
        let fetch_request_api_version = 9;
        let _ = fetch_request.encode(&mut request_buffer, fetch_request_api_version);
        let response = handle_fetch_request(
            request_buffer.into(),
            fetch_request_api_version,
            &topic_manager,
        );
        let record = response
            .unwrap()
            .responses
            .get(0)
            .unwrap()
            .partitions
            .get(0)
            .unwrap()
            .records
            .clone();
        assert_eq!(record, Some(Bytes::from("test")))
    }

    #[test]
    fn test_handle_fetch_request_with_offset() {
        let topic_manager = Arc::new(TopicManager::new(
            Arc::new(InMemoryLog::new()),
            RwLock::new(HashMap::new()),
        ));
        for ele in 0..5 {
            topic_manager.add("foobar", Bytes::from(ele.to_string()));
        }

        let fetch_request = create_fetch_request("foobar", 3, 3);
        let mut request_buffer = BytesMut::new();
        let fetch_request_api_version = 9;
        let _ = fetch_request.encode(&mut request_buffer, fetch_request_api_version);
        let response = handle_fetch_request(
            request_buffer.into(),
            fetch_request_api_version,
            &topic_manager,
        );
        let record = response
            .unwrap()
            .responses
            .get(0)
            .unwrap()
            .partitions
            .get(0)
            .unwrap()
            .records
            .clone();
        assert_eq!(record, Some(Bytes::from("3\04")))
    }
}
