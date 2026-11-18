# Configuration

| Key | Value | Unit |
| --- | --- | --- |
| broker.id | 1 | |
| broker.host | localhost | |
| broker.port | 9092 | |
| storage.directory | data/broker-1 | |
| zookeeper.connect | localhost:2181 | |
| zookeeper.session.timeout.ms | 5000 | ms |
| zookeeper.connect.timeout.ms | 10000 | ms |
| broker.request.threads | 10 | threads |
| broker.shutdown.timeout.ms | 30000 | ms |
| client.connect.timeout.ms | 5000 | ms |
| client.request.timeout.ms | 5000 | ms |
| replication.request.timeout.ms | 2000 | ms |
| controller.retry.delay.ms | 2000 | ms |
| storage.max.record.bytes | 1048576 | bytes |
| storage.max.segment.bytes | 67108864 | bytes |
| storage.flush.interval.ms | 10000 | ms |
| storage.recovery.mode | tail-truncate | |
| replication.factor | 3 | replicas |
| replication.min.in.sync.replicas | 2 | replicas |
| network.max.frame.bytes | 4194304 | bytes |
| fetch.max.response.bytes | 4194304 | bytes |
| network.read.buffer.bytes | 1024 | bytes |
| network.accept.poll.interval.ms | 100 | ms |
| network.read.poll.interval.ms | 50 | ms |
| consumer.poll.interval.ms | | ms |
| consumer.shutdown.timeout.ms | 1000 | ms |

## Constraints

| Setting | Constraint |
| --- | --- |
| Broker ID | >= 0 |
| Port | 1–65535 |
| Timeouts, intervals, thread counts | > 0 |
| Minimum ISR | 1 <= minimum ISR <= replication factor |
| Segment size | >= 6 + 16 + maximum record bytes |
| Frame size | >= maximum request/response body size |
| Fetch response size | <= frame size |
| Relative paths | Process working directory |
| Background consumer polling | Requires consumer.poll.interval.ms |
