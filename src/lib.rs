use std::cell::RefCell;

use ic_cdk::api::stable::StableWriter;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use crate::constants::*;
use crate::constants::*;
use crate::index_block::IndexBlock;
use crate::read_write::{BlockRead, BlockWrite, MemoryReader, MemoryWriter};
use crate::topic_header_block::{TOPIC_HEADER_MAGIC, TopicHeaderBlock};
pub use crate::topic_message::TopicMessage;

mod events;
mod index_block;
mod topic_header_block;
mod read_write;
mod constants;
mod topic_message;

pub struct EventFilesystem {
    write_fn: BlockWrite,
    writer: RefCell<MemoryWriter>,
    read_fn: BlockRead,
    reader: MemoryReader,
    topic_header: TopicHeaderBlock,
}

impl EventFilesystem {
    pub fn get_file_system(write_fn: BlockWrite,
                           read_fn: BlockRead,
                           clock: fn() -> u64) -> EventFilesystem {
        let height = read_block_height(read_fn);
        debug!("EventFilesystem thinks Block height is {}", height);
        let mut writer = RefCell::new(
            MemoryWriter::new(height,  clock)
        );

        let reader = MemoryReader::new();

        let topic_header = read_topic_block(read_fn);
        EventFilesystem {
            write_fn,
            writer,
            read_fn,
            reader,
            topic_header,
        }
    }

    pub fn get_topic_height(&self) -> u64 {
        return read_block_height(self.read_fn);
    }

    pub fn stable_store<T: Serialize>(&self, data: T) -> Result<(), String> {
        let data = bincode::serialize(&data).map_err(|e| format!("Failed to serialize: {}", e))?;
        if data.len() > FREE_MEMORY_BLOCK_SIZE as usize {
            return Err(format!("Data is too large: {}", data.len()));
        }
        (self.write_fn)(FREE_MEMORY_BLOCK_SIZE_IDX, &data.len().to_le_bytes());
        (self.write_fn)(FREE_MEMORY_BLOCK_START_IDX, data.as_slice());
        Ok(())
    }

    pub fn stable_restore<T: DeserializeOwned>(&self) -> Result<T, String> {
        let mut size = [0u8; 8];
        (self.read_fn)(FREE_MEMORY_BLOCK_SIZE_IDX, &mut size);
        let size = u64::from_le_bytes(size);

        let mut bytes = vec![0u8; size as usize];
        (self.read_fn)(FREE_MEMORY_BLOCK_START_IDX, bytes.as_mut_slice());
        bincode::deserialize(&bytes).map_err(|e| format!("Failed to deserialize: {}", e))
    }

    pub fn get_or_create(write_fn: BlockWrite,
                         read_fn: BlockRead,
                         clock: fn() -> u64,
                         event_stream_name: String,
    ) -> Self {
        let mut writer = RefCell::new(
            MemoryWriter::new(0, clock)
        );
        let reader = MemoryReader::new();

        if is_magic_number_valid(read_fn) {
            return Self::get_file_system(write_fn, read_fn, clock);
        } else {
            write_magic_number(write_fn);
            write_block_height(0, write_fn);

            let topic_block = TopicHeaderBlock {
                event_stream_name,
                first_message_ptr: 0,
                binary_version: 1_000_000,
            };

            write_topic_block(&topic_block, write_fn);

            EventFilesystem {
                write_fn,
                writer,
                read_fn,
                reader,
                topic_header: topic_block,
            }
        }
    }

    pub fn read_topic_message<T : DeserializeOwned>(&self, id: u64) -> Result<T, String> {
        self.reader.read_topic_message(id, self.read_fn)
    }

    pub fn write_topic_message<S: Serialize>(&self, data: &S) -> Result<u64, String> {
        let mut writer = self.writer.borrow_mut();
        return match writer.write(data, self.write_fn) {
            Ok(idx) => {
                debug!("Wrote topic_message at index {:?}", idx);
                write_block_height(writer.block_offset(), self.write_fn);
                Ok(idx.start_idx)
            }
            Err(e) => {
                Err(e)
            }
        };
    }

    pub fn read_topic_messages<T : DeserializeOwned>(&self, start: u64, take: u64) -> Result<Vec<T>, String> {
        self.reader.read_range::<T>(start, take, self.read_fn)
    }
}

fn is_magic_number_valid(reader: BlockRead) -> bool {
    let mut bytes = [0u8; 8];
    reader(MAGIC_NUMBER_IDX, &mut bytes);
    let magic_number = u64::from_le_bytes(bytes);
    magic_number == TOPIC_HEADER_MAGIC
}

fn write_magic_number(writer: BlockWrite) {
    writer(0, &TOPIC_HEADER_MAGIC.to_le_bytes());
}

fn read_topic_block(reader: BlockRead) -> TopicHeaderBlock {
    let mut topic_block_size = &mut [0u8; 8];
    reader(TOPIC_BLOCK_SIZE_IDX, topic_block_size);
    let topic_block_size = u64::from_le_bytes(*topic_block_size);

    let mut bytes = vec![0u8; topic_block_size as usize];
    reader(TOPIC_BLOCK_DATA_START_IDX, &mut bytes);
    bincode::deserialize(&bytes).unwrap()
}

fn write_topic_block(header: &TopicHeaderBlock, writer: BlockWrite) -> () {
    let topic_block_bytes = bincode::serialize(header).unwrap();
    assert!(topic_block_bytes.len() <= TOPIC_BLOCK_MAX_SIZE);
    let topic_block_size = (topic_block_bytes.len() as u64).to_le_bytes();

    writer(TOPIC_BLOCK_SIZE_IDX, &topic_block_size);
    writer(TOPIC_BLOCK_DATA_START_IDX, &topic_block_bytes);
}

fn write_block_height(height: u64, writer: BlockWrite) -> () {
    debug!("Writing block height to stable {}", height);
    writer(TOPIC_HEIGHT_IDX, &height.to_le_bytes());
}

fn read_block_height(reader: BlockRead) -> u64 {
    let mut bytes = [0u8; 8];
    reader(TOPIC_HEIGHT_IDX, &mut bytes);
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use std::ops::Range;

    use log::info;

    use crate::{BlockRead, BlockWrite, EventFilesystem, IDX_ZONE_END, read_topic_block, TOPIC_HEADER_MAGIC};

    thread_local! {
        static MEMORY: RefCell<Vec<u8>> = RefCell::new(vec![0u8; IDX_ZONE_END as usize + 1024 * 1024]);
    }

    #[test]
    fn it_gets_filesystem_and_rw() {
        let writer = get_write();
        let reader = get_read();

        let file_system = EventFilesystem::get_or_create(
            writer,
            reader,
            || 0,
            "test".to_string(),
        );

        MEMORY.with(|v| {
            let v = v.borrow();
            let magic_maybe: &[u8] = &v[0..8];
            let test = (&v[8..16]).to_vec().clone().get(0);
            let u64_magic = u64::from_le_bytes(magic_maybe.try_into().unwrap());
            assert_eq!(u64_magic, TOPIC_HEADER_MAGIC);
        });

        let topic_block = read_topic_block(reader);
        assert_eq!(topic_block.event_stream_name, "test");

        let message : String = "hello world".to_string();

        let res = file_system.write_topic_message::<String>(&message).unwrap();

        let file_system = EventFilesystem::get_file_system(
            writer,
            reader,
            || 0,
        );

        assert_eq!(file_system.read_topic_messages::<String>(res, 1).unwrap()[0], message);

        let message2 = "hello world2".to_string();
        let res = file_system.write_topic_message(&message2).unwrap();

        assert_eq!(res, 1);
        assert_eq!(file_system.read_topic_message::<String>(res).unwrap(), message2);
    }

    #[test]
    fn it_updates_topic_height() {
        let writer = get_write();
        let reader = get_read();

        let file_system = EventFilesystem::get_or_create(
            writer,
            reader,
            || 0,
            "test".to_string(),
        );

        assert_eq!(file_system.get_topic_height(), 0);
        for i in 0..100 {
            let message : String = format!("hello world {}", i);
            let res = file_system.write_topic_message::<String>(&message).unwrap();
        }
        assert_eq!(file_system.get_topic_height(), 100);

        let file_system = EventFilesystem::get_file_system(
            writer,
            reader,
            || 0,
        );

        assert_eq!(file_system.get_topic_height(), 100);
    }

    #[test]
    fn it_writes_and_gets_free_blockstore() {
        let file_system = EventFilesystem::get_or_create(
            get_write(),
            get_read(),
            || 0,
            "test".to_string(),
        );

        let data = "hello world";

        file_system.stable_store(data);

        assert_eq!(file_system.stable_restore::<String>().unwrap(), data);
    }

    fn get_write() -> BlockWrite {
        return |offset, bytes| {
            MEMORY.with(|mut mem| {
                let offset = offset as usize;
                let mut mem = mem.borrow_mut();
                for i in offset..offset + bytes.len() {
                    mem[i] = bytes[i - offset];
                }
            });
        };
    }

    fn get_read() -> BlockRead {
        return |offset, bytes| {
            MEMORY.with(|mut mem| {
                let offset = offset as usize;
                let mem = mem.borrow();
                for i in offset..offset + bytes.len() {
                    let val = mem.get(i as usize).unwrap().clone();
                    bytes[i - offset] = val;
                }
            });
        };
    }
}
