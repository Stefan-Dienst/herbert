use arrow_array::RecordBatch;
use boxcar;

pub struct Segment {
    inner: boxcar::Vec<RecordBatch>,
    min_offset: usize,
    max_offset: usize,
    max_committed_offset: usize,
    rolled: bool,
}

impl Segment {
    pub fn new() -> Self {
        Segment {
            inner: boxcar::Vec::new(),
            min_offset: 0,
            max_offset: 0,
            // TODO: Maybe move the commiting logic to partitions.
            // Segment should be simple I think.
            max_committed_offset: 0,
            rolled: false,
        }
    }

    pub fn append() {
        todo!("yeah")
    }

    pub fn get(offset: usize) -> RecordBatch {
        todo!(
            "requirements:
            - offset must be record level granular.
            
            maybe should also have a size, of how many records should be returned?

            Shoudl be responsible to getting the correct place and number of records.

            "
        )
    }
}
