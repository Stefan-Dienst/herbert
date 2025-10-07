use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::{GroupId, OffsetCommitRequest, OffsetCommitResponse, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use log::error;
use std::sync::Arc;

use crate::offset_manager::OffsetManager;

pub fn create_offset_commit_request(
    consumer_group: &str,
    topic: &str,
    offset: i64,
) -> OffsetCommitRequest {
    let mut offset_commit_request = OffsetCommitRequest::default();

    offset_commit_request.group_id =
        GroupId::from(StrBytes::from_string(consumer_group.to_string()));
    let mut offset_topic = OffsetCommitRequestTopic::default();
    offset_topic.name = TopicName::from(StrBytes::from_string(topic.to_string()));
    let mut offset_partition = OffsetCommitRequestPartition::default();
    offset_partition.committed_offset = offset;
    offset_topic.partitions.push(offset_partition);

    offset_commit_request.topics.push(offset_topic);
    offset_commit_request
}

pub fn handle_offset_commit_request(
    buf: Bytes,
    api_version: i16,
    offset_manager: &Arc<OffsetManager>,
) -> Result<OffsetCommitResponse> {
    let offset_commit_request = OffsetCommitRequest::decode(&mut Bytes::from(buf), api_version);
    match offset_commit_request {
        Ok(OffsetCommitRequest {
            group_id, topics, ..
        }) => {
            // NOTE: Right now we always only assume one topic with one partition.
            let first_topic = topics
                .get(0)
                .ok_or_else(|| anyhow::anyhow!("No Topic data available in fetch request"))?;
            let topic_name = first_topic.name.to_string();
            let first_partition = first_topic
                .partitions
                .get(0)
                .ok_or_else(|| anyhow::anyhow!("No partition data available in fetch request"))?;
            let offset = first_partition.committed_offset;
            let _ = offset_manager.set_offset(&group_id, &topic_name, offset);
        }
        _ => {
            error!("Something wrong with the fetch request.");
            panic!();
        }
    }
    Ok(OffsetCommitResponse::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn test_handle_offset_commit_request() {
        let offset_manager = Arc::new(OffsetManager::new());

        let consumer_group = "test";
        let topic = "foobar";
        let offset = 10;

        let offset_commit_request = create_offset_commit_request(consumer_group, topic, offset);
        let mut request_buffer = BytesMut::new();
        let offset_commit_request_api_version = 9;
        let _ =
            offset_commit_request.encode(&mut request_buffer, offset_commit_request_api_version);
        let _response = handle_offset_commit_request(
            request_buffer.into(),
            offset_commit_request_api_version,
            &offset_manager,
        );
        assert_eq!(
            offset_manager
                .offsets
                .read()
                .unwrap()
                .get(&(consumer_group.to_string(), topic.to_string()))
                .unwrap(),
            &10
        )
    }
}
