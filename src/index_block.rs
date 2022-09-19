use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct IndexBlock {
    pub(crate) height: u64,
    pub(crate) data_size: u64,
    pub(crate) start_idx: u64,
    pub(crate) end_idx: u64,
    pub(crate) timestamp: u64,
}

#[cfg(test)]
mod test {
    use crate::index_block::IndexBlock;

    #[test]
    fn it_serializes_and_deserializes() {
        let idx = IndexBlock {
            height: 1,
            data_size: 100,
            start_idx: 200,
            end_idx: 300,
            timestamp: 123456789,
        };

        let res = bincode::serialize(&idx).unwrap();
        assert_eq!(res.len(), 40);
        assert_eq!(idx, bincode::deserialize(&res).unwrap());
    }
}