use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, GroupId, OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use log::{error, info};
use std::sync::Arc;
use std::vec;

use crate::offset_manager::OffsetManager;
use crate::topic_manager::{self, TopicManager};

pub fn create_offset_fetch_request(consumer_group: &str, topic: &str) -> OffsetFetchRequest {
    let mut offset_fetch_request = OffsetFetchRequest::default();
    offset_fetch_request.group_id =
        GroupId::from(StrBytes::from_string(consumer_group.to_string()));
    let mut offset_fetch_topic = OffsetFetchRequestTopic::default();
    offset_fetch_topic.name = TopicName::from(StrBytes::from_string(topic.to_string()));

    offset_fetch_request.topics = Some(vec![offset_fetch_topic]);
    offset_fetch_request
}

pub fn handle_offset_fetch_request(
    buf: Bytes,
    api_version: i16,
    offset_manager: &Arc<OffsetManager>,
) -> Result<OffsetFetchResponse> {
    let offset_fetch_request = OffsetFetchRequest::decode(&mut Bytes::from(buf), api_version);
    match offset_fetch_request {
        Ok(OffsetFetchRequest {
            group_id, topics, ..
        }) => {
            // NOTE: Right now we always only assume one topic with one partition.
            let topic_vec = topics.ok_or_else(|| anyhow::anyhow!("No topic data"))?;
            let first_topic = topic_vec
                .get(0)
                .ok_or_else(|| anyhow::anyhow!("Topic list is empty"))?;
            let topic_name = first_topic.name.to_string();
            let offset = offset_manager.get_offset(&group_id, &topic_name)?;

            let mut response = OffsetFetchResponse::default();
            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = TopicName::from(StrBytes::from_string(topic_name.to_string()));
            let mut partition_response = OffsetFetchResponsePartition::default();
            partition_response.committed_offset = offset;

            topic_response.partitions.push(partition_response);
            response.topics.push(topic_response);
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
    use super::*;

    #[test]
    fn test_handle_offset_fetch_request() {
        let offset_manager = Arc::new(OffsetManager::new());
        let consumer_group = "test";
        let topic = "foobar";
        let offset = 10;
        let _ = offset_manager.set_offset(consumer_group, topic, offset);

        let offset_fetch_request = create_offset_fetch_request(consumer_group, topic);
        let mut request_buffer = BytesMut::new();
        let offset_fetch_request_api_version = 6;
        let _ = offset_fetch_request.encode(&mut request_buffer, offset_fetch_request_api_version);
        let response = handle_offset_fetch_request(
            request_buffer.into(),
            offset_fetch_request_api_version,
            &offset_manager,
        );
        let got_offset = response
            .unwrap()
            .topics
            .get(0)
            .unwrap()
            .partitions
            .get(0)
            .unwrap()
            .committed_offset
            .clone();
        assert_eq!(got_offset, offset)
    }
}
