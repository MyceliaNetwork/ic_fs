use std::arch::aarch64::vaba_u8;
use std::fs::read;
use std::io::{Read, Write};

use serde::Serialize;

use crate::{BLOCK_SIZE, IDX_BLOCK_SIZE, IDX_ZONE_END};
use crate::index_block::IndexBlock;
use crate::topic_message::TopicMessage;

pub type BlockWrite = fn(offset: u64, data: &[u8]) -> ();

pub type BlockRead = fn(offset: u64, buf: &mut [u8]) -> ();

pub struct MemoryWriter {
    block_offset: u64,
    // Current block to be written. Note, this is count of BLOCK_SIZE chunks
    idx_start: u64,
    // Start index of the index block zone
    clock: fn() -> u64,
}

impl MemoryWriter
{
    pub fn new(block_offset: u64, idx_start: u64, clock: fn() -> u64) -> Self {
        MemoryWriter {
            block_offset,
            idx_start,
            clock,
        }
    }


    pub fn write<S: Serialize>(&mut self, value: &S, writer: BlockWrite) -> Result<IndexBlock, String> {
        let bytes = bincode::serialize(value).map_err(|e| format!("Failed to serialize: {}", e))?;

        // Calculate how many whole blocks we need to fill
        let mut blocks = bytes.len() / 512;
        let mut blocks = blocks.max(1);

        // Calculate if we need partial block
        if bytes.len() > BLOCK_SIZE as usize && bytes.len() % BLOCK_SIZE as usize > 0 {
            blocks += 1;
        };

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
        writer(offset, &bytes);

        // move offset
        self.block_offset += blocks as u64;
        Ok(idx)
    }

    fn write_idx(&mut self, idx: &IndexBlock, writer: BlockWrite) -> Result<(), String> {
        let bytes = bincode::serialize(idx).map_err(|e| format!("Failed to serialize: {}", e))?;
        // Move to index region, move over number of blocks
        let mut offset = self.idx_start + (self.block_offset * IDX_BLOCK_SIZE);
        writer(offset, &bytes);
        Ok(())
    }

    pub fn block_offset(&self) -> u64 {
        self.block_offset
    }
}

pub struct MemoryReader
{
    idx_start: u64,
}

impl MemoryReader
{
    pub(crate) fn new(idx_start: u64) -> Self {
        MemoryReader {
            idx_start,
        }
    }
    // We could do a lotttt more here, but for now we'll just loop
    pub fn read_range(&self, start: u64, count: u64, reader: BlockRead) -> Result<Vec<TopicMessage>, String> {
        let mut messages = Vec::new();
        for i in start..start + count {
            match self.read_topic_message(i, reader) {
                Ok(msg) => messages.push(msg),
                Err(e) => return Err(e),
            }
        }
        Ok(messages)
    }

    pub(crate) fn read_topic_message(&self, height: u64, reader: BlockRead) -> Result<TopicMessage, String> {
        let mut bytes = [0u8; 32];
        let mut idx_offset = self.idx_start + (height * IDX_BLOCK_SIZE);
        reader(idx_offset, &mut bytes);
        let idx: IndexBlock = bincode::deserialize(&bytes).map_err(|e| format!("Failed to deserialize: {}", e))?;

        let start = IDX_ZONE_END + (idx.start_idx * BLOCK_SIZE);
        let mut buf = vec![0u8; idx.data_size as usize];

        reader(start, &mut buf);
        Ok(TopicMessage {
            data: buf
        })
    }

    fn read_idx(&self, offset: u64, reader: BlockRead) -> Result<IndexBlock, String> {
        let mut bytes = [0u8; 32];
        reader(self.idx_start + (IDX_BLOCK_SIZE * offset), &mut bytes);
        let idx = bincode::deserialize(&bytes).map_err(|e| format!("Failed to deserialize: {}", e))?;
        Ok(idx)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::constants::*;
    use crate::read_write::{MemoryReader, MemoryWriter};

    thread_local! {
        static MEMORY: RefCell<Vec<u8>> = RefCell::new(vec![0u8; IDX_ZONE_END as usize + 1024 * 1024]);
    }

    fn get_writer() -> MemoryWriter {
        MemoryWriter {
            block_offset: 0,
            clock: || 0,
            idx_start: IDX_ZONE_OFFSET,
        }
    }

    fn get_reader() -> MemoryReader {
        MemoryReader {
            idx_start: IDX_ZONE_OFFSET
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
        let bytes = b"Hello, world!";
        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(bytes, write).unwrap();
        let out = reader.read_topic_message(res.start_idx, read).unwrap();

        assert_eq!(out.data, bytes);
    }

    #[test]
    fn it_writes_and_reads_multiple() {
        let bytes = b"Hello, world!";
        let bytes_two = b"Foo Bar Baz";
        let bytes_three = b"A";

        let mut writer = get_writer();
        let mut reader = get_reader();

        let res = writer.write(bytes, write).unwrap();
        let res_two = writer.write(bytes_two, write).unwrap();
        let res_three = writer.write(bytes_three, write).unwrap();

        let out = reader.read_topic_message(res.start_idx, read).unwrap();
        let out_two = reader.read_topic_message(res_two.start_idx, read).unwrap();
        let out_three = reader.read_topic_message(res_three.start_idx, read).unwrap();

        assert_eq!(out.data, bytes);
        assert_eq!(out_two.data, bytes_two);
        assert_eq!(out_three.data, bytes_three);
    }
}