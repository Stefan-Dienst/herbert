use crate::storage::{offsets::Offsets, segment::Segment};

pub struct Partition {
    active_segment: Segment,
    rolled_segments: Vec<Segment>,
    offsets: Offsets,
    config: PartitionConfig,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            active_segment: Segment::new(),
            rolled_segments: Vec::new(),
            offsets: Offsets::new(),
            config: PartitionConfig::default(),
            //TODO: maybe add WAL here one per patition. then depending on the writer one could
            //force a flush... or let it peridodically flush.
        }
    }

    pub fn append() {
        todo!(
            "here records should be added to the active segment
        Then also additional checks like if the segment should be rolled must be done.

        Also the offset structure must be updated.
        

        Also an ACK must be returned to the client that called this method. This should indicate 

        TODO: How can this be made durable in the same way that the data is durable?
        When i have the WAL on a replay the offsets must also be correct.

            "
        )
    }

    pub fn roll_segment() {
        todo!("used to make segments immutable...")
    }

    pub fn get() {
        todo!("don't know yet...")
    }
}

#[derive(Default)]
struct PartitionConfig {}
