pub struct Offsets {}

impl Offsets {
    pub fn new() -> Self {
        Self {}
    }
}

// Must be responsilbe for doing the mapping from partition offset to segment + position in Record
// batch offset.
// On every append this struct must be updated to know what happened.
