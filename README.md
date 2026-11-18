# Zkafka

Zkafka is a learning project that recreates parts of early, ZooKeeper-backed Apache Kafka. It explores partitioned logs, leader and follower replication, and offset-based reads. For learning and local demos only. **Not intended for production.**

**Author:** Ariyes  
**Stack:** Java 21, Java NIO, Apache ZooKeeper, Maven, and Spotless.

## Project layout

| Path | Contents |
| --- | --- |
| `src/main/java/com/zkafka/broker` | Broker, ZooKeeper coordination, partitions, and disk storage |
| `src/main/java/com/zkafka/client` | Producer, consumer, and TCP transport |
| `src/main/java/com/zkafka/common` and `config` under the Java package | Wire protocol, shared models, errors, and settings loader |
| `src/test/java` | Standalone checks for protocol, storage, client, transport, and configuration |
| `config/`, `demo/`, `docs/` | Runtime settings, runnable demo, and design notes |

## Run the demo

Start standalone ZooKeeper on `localhost:2181`. From the project root on Windows, build and start the broker:

```powershell
.\mvnw.cmd -DskipTests package
java -jar target/zkafka-1.0-SNAPSHOT.jar config/zkafka-local.properties
```

With the broker running on port `9092`, open a second terminal at the project root and run:

```powershell
jshell --class-path target/zkafka-1.0-SNAPSHOT.jar demo/hello.jsh
```

Each run creates a fresh topic, publishes `hello` at offset `0`, and fetches it back as `0: hello`.

For lint and compilation, run `./mvnw.cmd spotless:check test-compile`. The checks under `src/test/java` are Java entry points. Run them directly, for example `java -cp "target/classes;target/test-classes" com.zkafka.broker.ProtocolTest`.

## Assumptions

- The local demo uses one broker, one partition, and one replica.
- Zkafka uses its own length-prefixed TCP protocol and Java client.
- Replica assignments are static. A partition is unavailable while its assigned leader is offline.
