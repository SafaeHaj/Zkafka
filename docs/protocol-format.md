# Protocol Format

## Encoding

| Field | Bytes | Contract |
| --- | ---: | --- |
| Byte order | | Big-endian |
| Type | 1 | Operation ID |
| String length | 2 | Unsigned UTF-8 byte count; 0–65535 |
| String | N | UTF-8 bytes |
| Topic | | Nonblank |
| ID/count | 4 | Nonnegative int32 |
| Offset | 8 | Nonnegative int64; produce-error sentinel -1 |
| Error | 2 | Unsigned; 0 = success |
| Value length | 4 | Nonnegative int32 |
| Value | N | Opaque bytes |
| Body boundary | | Buffer position through limit |
| Truncated/trailing bytes | | Reject |
| Unknown error code | | Preserve numeric value |

## Transport

| Field | Bytes | Contract |
| --- | ---: | --- |
| Body length prefix | 4 | Positive int32; excludes prefix |
| Body | N | Exactly declared bytes |
| Maximum frame body | | 4194304 bytes |
| Outstanding requests per connection | | 1 |
| Wire version field | | |
| Correlation ID | | |
| Unidentifiable malformed frame | | Close connection |

## Requests

| Type | Operation | Fields after type, in order |
| --- | --- | --- |
| 0x01 | Produce | topic String; partition int32; value length int32; value bytes |
| 0x02 | Fetch | topic String; partition int32; offset int64; maxBytes int32 |
| 0x03 | Metadata | |
| 0x04 | Create topic | topic String; partition count int32; replication factor int16 |
| 0x05 | Replicate | topic String; partition int32; offset int64; value length int32; value bytes |

## Request Constraints

| Field | Contract |
| --- | --- |
| Value bytes | 0–1048576 |
| Partition ID | >= 0 |
| Offset | >= 0 |
| Fetch maxBytes | > 0 |
| Partition count | > 0 |
| Replication factor | 1–32767 |
| Replication offset | Leader-assigned logical offset |

## Produce Response — 0x11

| Field | Bytes | Contract |
| --- | ---: | --- |
| Type | 1 | 0x11 |
| Topic | 2 + N | String |
| Partition | 4 | >= 0 |
| Offset | 8 | >= 0 on success; -1 on failure |
| Error | 2 | Error code |

## Fetch Response — 0x12

| Field | Bytes | Contract |
| --- | ---: | --- |
| Type | 1 | 0x12 |
| Topic | 2 + N | String |
| Partition | 4 | >= 0 |
| Error | 2 | Error code |
| Message count | 4 | >= 0; 0 on failure |
| Messages | Variable | Repeated fetched-message records |

## Fetched Message

| Field | Bytes | Contract |
| --- | ---: | --- |
| Offset | 8 | >= 0 |
| Value length N | 4 | 0–1048576 |
| Value | N | Opaque bytes |

## Fetch Rules

| Field | Contract |
| --- | --- |
| Order | Increasing offset |
| Visibility | Committed records only |
| maxBytes budget | Repeated message bytes, including 12-byte headers |
| Partial records | Forbidden |
| First eligible record exceeds budget | MESSAGE_TOO_LARGE |
| Request at high watermark | Empty success |
| Unavailable offset / offset above high watermark | OFFSET_OUT_OF_RANGE |

## Metadata Response — 0x13

| Field | Bytes | Contract |
| --- | ---: | --- |
| Type | 1 | 0x13 |
| Error | 2 | Error code |
| Broker count | 4 | >= 0; 0 on failure |
| Brokers | Variable | Repeated broker records |
| Topic count | 4 | >= 0; 0 on failure |
| Topics | Variable | Repeated topic records |

## Broker

| Field | Bytes | Contract |
| --- | ---: | --- |
| ID | 4 | >= 0; unique |
| Host | 2 + N | Nonblank UTF-8 String |
| Port | 4 | 1–65535 |

## Topic

| Field | Bytes | Contract |
| --- | ---: | --- |
| Name | 2 + N | Nonblank String; unique |
| Partition count | 4 | >= 0 |
| Partitions | Variable | Repeated partition records |

## Partition Metadata

| Field | Bytes | Contract |
| --- | ---: | --- |
| ID | 4 | >= 0; unique within topic |
| Leader ID | 4 | Listed broker; included in replicas |
| Replica count | 4 | > 0 |
| Replica IDs | 4 each | Unique listed broker IDs |

## Error Codes

| Code | Name |
| ---: | --- |
| 0 | NONE |
| 1 | INVALID_REQUEST |
| 2 | UNKNOWN_TOPIC_OR_PARTITION |
| 3 | NOT_LEADER |
| 4 | MESSAGE_TOO_LARGE |
| 5 | OFFSET_OUT_OF_RANGE |
| 6 | TOPIC_ALREADY_EXISTS |
| 7 | INVALID_REPLICATION_FACTOR |
| 8 | INTERNAL_ERROR |
| | Insufficient ISR |
| | Acknowledgment timeout |
| | Storage failure |

## Other Responses

| Operation | Type | Record |
| --- | --- | --- |
| Create topic | 0x14 | Type followed by uint16 error code |
| Replication acknowledgment | 0x15 | Type followed by uint16 error code |
