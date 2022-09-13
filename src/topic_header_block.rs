use std::io::Write;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const TOPIC_HEADER_MAGIC: u64 = 123246369;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicHeaderBlock {
    pub event_stream_name: String,
    pub first_message_ptr: u64,
    pub binary_version: u32,
}

#[cfg(test)]
mod test {
    use crate::index_block::IndexBlock;
    use crate::topic_header_block::{TOPIC_HEADER_MAGIC, TopicHeaderBlock};

    #[test]
    fn it_serializes_and_deserializes() {
        let idx = TopicHeaderBlock {
            event_stream_name: "test_stream".parse().unwrap(),
            first_message_ptr: 0,
            binary_version: 1_000_000,
        };

        let res = bincode::serialize(&idx).unwrap();
        assert!(res.len() <= 512);
        assert_eq!(idx, bincode::deserialize(&res).unwrap());
    }
}
