# Client Contracts

## Producer

| Field | Contract |
| --- | --- |
| Topic | Fixed per producer instance |
| Automatic routing | Round-robin per producer instance |
| Partition candidates | Sorted partition IDs from metadata |
| Explicit routing | send(partition, value) |
| Explicit send advances rotation | No |
| Rotation advancement | After automatic selection, including failed/uncertain delivery |
| Local validation failure advances rotation | No |
| Keys | None |
| String values | UTF-8 |
| Send result | topic, partition, offset |
| Concurrent sends on one producer | Serialized |
| Automatic produce retry | None |
| Global ordering across partitions | None |

## Transport and Metadata

| Field | Contract |
| --- | --- |
| Request framing | 4-byte big-endian body length + body |
| Response framing | 4-byte big-endian body length + body |
| Connection lifetime | 1 exchange |
| Connect deadline | client.connect.timeout.ms |
| Request deadline | client.request.timeout.ms; write + read after connection |
| DNS deadline | |
| Maximum body | network.max.frame.bytes |
| Response topic/partition | Must match outstanding request |
| Metadata update | Immutable snapshot replacement |
| Produce/fetch destination | Partition leader from metadata |
| Unknown assignment | Refresh metadata once; fail if still absent |
| NOT_LEADER / UNKNOWN_TOPIC_OR_PARTITION | Invalidate cached metadata; expose error |
| Refresh bootstrap | broker.host + broker.port |
| Injected transport ownership | Client closes transport |
| Injected client ownership | Producer/consumer closes client |
| Close | Idempotent; abort outstanding network exchanges |

## Consumer

| Field | Contract |
| --- | --- |
| Assignment | 1 explicit topic-partition |
| Record | offset + immutable value bytes |
| Initial local position | 0 |
| Manual poll position | Last returned record offset + 1 |
| Empty poll position | Unchanged |
| Background position | Advance after each successful handler call |
| Handler failure | Stop; expose lastFailure; retain unsuccessful delivery position |
| Seek | Nonnegative offset; manual mode only |
| Manual/background operations | Mutually exclusive |
| Background polling interval | consumer.poll.interval.ms; required when starting |
| Stop wait | consumer.shutdown.timeout.ms |
| Self-stop from handler | No self-join |
| Stop timeout | IOException; worker may still be exiting |
| Consumer-group commits | |
| Persistent position | |

## Errors

| Condition | Result |
| --- | --- |
| Invalid local arguments | IllegalArgumentException / NullPointerException |
| Broker rejects request | BrokerException with numeric error code |
| Unknown broker code | Preserved; knownError empty |
| Malformed/mismatched response | ClientProtocolException |
| Network timeout | SocketTimeoutException |
| Network disconnect | IOException |
| Produce I/O failure | Delivery may be uncertain; no automatic resend |
| Invalid fetched offset order/range | ClientProtocolException |
| Fetch exceeds requested byte budget | ClientProtocolException |
| Background handler failure | lastFailure; no automatic position advance for failed record |
