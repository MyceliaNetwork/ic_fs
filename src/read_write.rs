use std::fs::read;
use std::io::{Read, Write};
use log::{debug, info};
use serde::de::DeserializeOwned;

use serde::Serialize;

use crate::{BLOCK_SIZE, IDX_BLOCK_SIZE, IDX_ZONE_END, IDX_ZONE_IDX};
use crate::index_block::IndexBlock;
use crate::topic_message::TopicMessage;

pub type BlockWrite = fn(offset: u64, data: &[u8]) -> ();

pub type BlockRead = fn(offset: u64, buf: &mut [u8]) -> ();

pub struct MemoryWriter {
    block_offset: u64,
    // Start index of the index block zone
    clock: fn() -> u64,
}

fn get_block_count(data_size : u64) -> u64 {
    let mut blocks = data_size / BLOCK_SIZE;
    if data_size % BLOCK_SIZE != 0 {
        blocks += 1;
    }
    return blocks;
}

fn get_data_offset_from_height(height: u64) -> u64 {
    return IDX_ZONE_END + (height * BLOCK_SIZE);
}

impl MemoryWriter
{
    pub fn new(block_offset: u64, clock: fn() -> u64) -> Self {
        MemoryWriter {
            block_offset,
            clock,
        }
    }


    pub fn write<S: Serialize>(&mut self, value: &S, writer: BlockWrite) -> Result<IndexBlock, String> {
        let bytes = bincode::serialize(value).map_err(|e| format!("Failed to serialize: {}", e))?;

        // Calculate how many whole blocks we need to fill
        let mut blocks = get_block_count(bytes.len() as u64);

        let idx = IndexBlock {
            data_size: bytes.len() as u64,
            start_idx: self.block_offset,
            end_idx: self.block_offset + blocks as u64,
            timestamp: (self.clock)(),
        };

        // record index block
        self.write_idx(&idx, writer);

        // write data
        let offset = IDX_ZONE_END + (self.block_offset * BLOCK_SIZE);
        info!("Writing data at offset {} for idx {:?}", offset, idx);
        writer(offset, &bytes);

        // move offset
        self.block_offset += blocks as u64;
        Ok(idx)
    }

    fn write_idx(&mut self, idx: &IndexBlock, writer: BlockWrite) -> Result<(), String> {
        let bytes = bincode::serialize(idx).map_err(|e| format!("Failed to serialize: {}", e))?;
        // Move to index region, move over number of blocks
        let mut offset = IDX_ZONE_IDX + (self.block_offset * IDX_BLOCK_SIZE);
        info!("Writing index block: {:?} offset {}", idx, offset);
        writer(offset, &bytes);
        Ok(())
    }

    pub fn block_offset(&self) -> u64 {
        self.block_offset
    }
}

pub struct MemoryReader
{
}

impl MemoryReader
{
    pub(crate) fn new() -> Self {
        MemoryReader {
        }
    }

    // We could do a lotttt more here, but for now we'll just loop
    pub fn read_range<T : DeserializeOwned>(&self, start: u64, count: u64, reader: BlockRead) -> Result<Vec<T>, String> {
        let mut messages = Vec::new();
        for i in start..start + count {
            match self.read_topic_message(i, reader) {
                Ok(msg) => messages.push(msg),
                Err(e) => return Err(e),
            }
        }
        Ok(messages)
    }

    pub(crate) fn read_topic_message<T : DeserializeOwned>(&self, height: u64, reader: BlockRead) -> Result<T, String> {
        let idx = self.read_idx(height, reader)?;
        debug!("Read index  {:?}", idx);

        let read_start = get_data_offset_from_height(idx.start_idx);
        let mut buf = vec![0u8; idx.data_size as usize];
        debug!("Reading {:?} from offset {:?}", idx.data_size, read_start);
        reader(read_start, &mut buf);

        bincode::deserialize::<T>(&*buf).map_err(|e| format!("Failed to deserialize: {}", e))
    }

    pub fn read_idx(&self, offset: u64, reader: BlockRead) -> Result<IndexBlock, String> {
        let mut bytes = [0u8; 32];
        reader(IDX_ZONE_IDX + (IDX_BLOCK_SIZE * offset), &mut bytes);
        let idx = bincode::deserialize(&bytes).map_err(|e| format!("Failed to deserialize: {}", e))?;
        Ok(idx)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::constants::*;
    use crate::read_write::{get_block_count, get_data_offset_from_height, MemoryReader, MemoryWriter};

    thread_local! {
        static MEMORY: RefCell<Vec<u8>> = RefCell::new(vec![0u8; IDX_ZONE_END as usize + 1024 * 1024 * 128]);
    }

    fn get_writer() -> MemoryWriter {
        MemoryWriter {
            block_offset: 0,
            clock: || 0,
        }
    }

    fn get_reader() -> MemoryReader {
        MemoryReader {
        }
    }

    fn write(offset: u64, data: &[u8]) -> () {
        MEMORY.with(|v| {
            let mut v = v.borrow_mut();
            for i in offset..offset + data.len() as u64 {
                v[i as usize] = data[(i - offset) as usize];
            }
        });
    }

    fn read(offset: u64, data: &mut [u8]) -> () {
        MEMORY.with(|v| {
            let mut v = v.borrow();
            for i in offset..offset + data.len() as u64 {
                data[(i - offset) as usize] = v[i as usize];
            }
        });
    }

    #[test]
    fn it_writes_and_reads_a_blob() {
        let message = "Hello, world!".to_string();
        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(&message, write).unwrap();
        let out = reader.read_topic_message::<String>(res.start_idx, read).unwrap();

        assert_eq!(out, message);
    }

    #[test]
    fn it_writes_and_reads_multiple() {
        let bytes = "Hello, world!".to_string();
        let bytes_two = "Foo Bar Baz".to_string();
        let bytes_three = "A".to_string();

        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(&bytes, write).unwrap();
        let res_two = writer.write(&bytes_two, write).unwrap();
        let res_three = writer.write(&bytes_three, write).unwrap();

        let out = reader.read_topic_message::<String>(res.start_idx, read).unwrap();
        let out_two = reader.read_topic_message::<String>(res_two.start_idx, read).unwrap();
        let out_three = reader.read_topic_message::<String>(res_three.start_idx, read).unwrap();

        assert_eq!(out, bytes);
        assert_eq!(out_two, bytes_two);
        assert_eq!(out_three, bytes_three);
    }

    #[test]
    fn it_writes_and_reads_large_multiple() {
        let bytes = vec![12u8; 1024 * 1024];
        let bytes_two = vec![33u8; 1024 * 1024];

        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(&bytes, write).unwrap();
        let res_two = writer.write(&bytes_two, write).unwrap();

        let out = reader.read_topic_message::<Vec<u8>>(res.start_idx, read).unwrap();
        let out_two = reader.read_topic_message::<Vec<u8>>(res_two.start_idx, read).unwrap();

        assert_eq!(out, bytes);
        assert_eq!(out_two, bytes_two);
    }

    #[test]
    fn it_writes_partial_block() {
        let bytes = vec![12u8; 512];
        let bytes_two = vec![33u8; 513];
        let bytes_three = vec![55u8; 512 * 2 + 1];

        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(&bytes, write).unwrap();
        let res_two = writer.write(&bytes_two, write).unwrap();
        let res_three = writer.write(&bytes_three, write).unwrap();

        let out = reader.read_topic_message::<Vec<u8>>(res.start_idx, read).unwrap();
        let out_two = reader.read_topic_message::<Vec<u8>>(res_two.start_idx, read).unwrap();
        let out_three = reader.read_topic_message::<Vec<u8>>(res_three.start_idx, read).unwrap();

        assert_eq!(out, bytes);
        assert_eq!(out_two, bytes_two);
        assert_eq!(out_three, bytes_three);
    }

    #[test]
    pub fn it_gets_block_count_for_data() {
        assert_eq!(get_block_count(0), 0);
        assert_eq!(get_block_count(1), 1);
        assert_eq!(get_block_count(512*1), 1);
        assert_eq!(get_block_count(512*2), 2);
        assert_eq!(get_block_count(512*2 + 1), 3);
        assert_eq!(get_block_count(512*10 + 50), 11);
    }

    #[test]
    pub fn it_get_offset_from_block_height() {
        assert_eq!(get_data_offset_from_height(0), IDX_ZONE_END);
        assert_eq!(get_data_offset_from_height(1), IDX_ZONE_END + BLOCK_SIZE);
        assert_eq!(get_data_offset_from_height(10), IDX_ZONE_END + BLOCK_SIZE * 10);
    }
}