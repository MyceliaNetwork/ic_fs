# IC Event File Store

# Stable Memory Layout

512 Bytes blocks totaling 16,777,216 blocks

# Start

magic_number

topic_block_size

header_block

# Index Blocks

data size | u64 | 8 Bytes

start block | u64 | 8 Bytes

end block | u64 | 8 Bytes

time_added | u64 | 8 Bytes

- We need to allocate 536,870,912 bytes or 1,048,576 Blocks or 8192 pages

# Event Filesystem Event

data, 504 Bytes

next_block, u64 | 8 Bytes