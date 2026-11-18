# Storage Format

## Segments

| Field | Contract |
| --- | --- |
| Directory | 1 per local topic-partition replica |
| Filename | 20 ASCII decimal digits + .log |
| Filename expression | String.format(Locale.ROOT, "%020d.log", baseOffset) |
| Base offset | 0–9223372036854775807 |
| Segment order | Ascending numerical base offset |
| Active segment | Last segment |
| Append targets | 1 active segment |
| Byte order | Big-endian |
| Offset sequence | Consecutive; new partition starts at 0 |
| First record offset | Segment base offset |
| Empty segment | Valid header only; final segment only |
| Record splitting across segments | Forbidden |
| Segment roll | Before next record exceeds segment size |

## Segment Header

| Field | Byte position | Bytes | Value |
| --- | ---: | ---: | --- |
| Magic | 0 | 4 | ZKLG; 5A 4B 4C 47 |
| Version | 4 | 2 | 1 |
| First record | 6 | | |

## Record

| Field | Byte position within record | Bytes | Contract |
| --- | ---: | ---: | --- |
| Offset | 0 | 8 | Nonnegative int64 |
| Value length N | 8 | 4 | 0 <= N <= 1048576 |
| CRC32C | 12 | 4 | Raw 32-bit checksum |
| Value | 16 | N | Opaque bytes |

## Limits and Checksum

| Field | Contract |
| --- | --- |
| Fixed record header | 16 bytes |
| Maximum value | 1048576 bytes |
| Maximum complete record | 1048592 bytes |
| CRC algorithm | CRC32C |
| CRC coverage | Serialized offset + serialized value length + value |
| CRC exclusions | CRC field; segment header |
| Oversized value | Reject before modifying log |
| Unknown format version | Reject |
| Offset overflow | Reject |

## Append and Acknowledgment

| Boundary | Contract |
| --- | --- |
| Appended | Complete record written through FileChannel |
| Locally flushed | Record covered by successful force |
| ISR | Caught-up eligible replicas, including leader |
| Successful acknowledgment | Leader + required ISR appends; ISR meets minimum |
| Per-record force before acknowledgment | No |
| Flush policy | Periodic |
| nextOffset | Exclusive local log-end offset |
| High watermark | Exclusive committed offset |
| Consumer visibility | Offsets below high watermark |
| Simultaneous loss of all unflushed replicas | Acknowledged data may be lost |
| Request timeout | Outcome may be uncertain |
| Retry deduplication | |

## Rolling and Checkpoints

| Operation | Contract |
| --- | --- |
| Segment publication | Complete header in temporary file; force; atomic rename |
| Active-channel switch | Under partition write lock |
| flushedNextOffset | Exclusive offset covered by successful local force |
| Checkpoint ordering | Data force before checkpoint advancement |
| Checkpoint replacement | Temporary write; force; atomic replace |
| Checkpoint byte layout | |
| Missing/invalid checkpoint | Scan records |
| Durable directory publication | |

## Recovery

| Condition | Action |
| --- | --- |
| Complete valid records | Recover next offset |
| Incomplete active final record | Guarded tail truncation |
| CRC-invalid active final record ending at EOF | Guarded tail truncation |
| Truncation boundary | Start of damaged record |
| Known flushed/committed prefix | Never truncate |
| Unknown durability/commit boundary | Reject or repair from authoritative replica |
| Corruption in sealed segment | Reject |
| Corruption before final record | Reject |
| Invalid header, length, or offset order | Reject |
| strict mode | Reject damage; no modification |
| Uncertain partial write / force failure | Mark partition unavailable |
| Replica reconciliation | Before serving traffic |
