package com.zkafka.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

/** Immutable startup settings loaded once and passed to application components. */
public record ZkafkaConfig(
    Broker broker,
    ZooKeeperSettings zooKeeper,
    Execution execution,
    Storage storage,
    Replication replication,
    Network network,
    ConsumerSettings consumer) {

  private static final Set<String> KEYS =
      Set.of(
          "broker.id",
          "broker.host",
          "broker.port",
          "storage.directory",
          "zookeeper.connect",
          "zookeeper.session.timeout.ms",
          "zookeeper.connect.timeout.ms",
          "broker.request.threads",
          "broker.shutdown.timeout.ms",
          "client.connect.timeout.ms",
          "client.request.timeout.ms",
          "replication.request.timeout.ms",
          "controller.retry.delay.ms",
          "storage.max.record.bytes",
          "storage.max.segment.bytes",
          "storage.flush.interval.ms",
          "storage.recovery.mode",
          "replication.factor",
          "replication.min.in.sync.replicas",
          "network.max.frame.bytes",
          "fetch.max.response.bytes",
          "network.read.buffer.bytes",
          "network.accept.poll.interval.ms",
          "network.read.poll.interval.ms",
          "consumer.poll.interval.ms",
          "consumer.shutdown.timeout.ms");

  public ZkafkaConfig {
    Objects.requireNonNull(broker, "broker");
    Objects.requireNonNull(zooKeeper, "zooKeeper");
    Objects.requireNonNull(execution, "execution");
    Objects.requireNonNull(storage, "storage");
    Objects.requireNonNull(replication, "replication");
    Objects.requireNonNull(network, "network");
    Objects.requireNonNull(consumer, "consumer");
    // Largest replication body with a maximum-length encoded topic.
    long maximumBody = 1L + 2 + 65535 + 4 + 8 + 4 + storage.maxRecordBytes();
    if (network.maxFrameBytes() < maximumBody) {
      throw new IllegalArgumentException(
          "network.max.frame.bytes must fit the maximum record and request headers: "
              + maximumBody);
    }
    if (network.fetchMaxResponseBytes() < maximumBody + 6) {
      throw new IllegalArgumentException(
          "fetch.max.response.bytes must fit the maximum record and response headers: "
              + (maximumBody + 6));
    }
  }

  public static ZkafkaConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    return fromProperties(properties);
  }

  public static ZkafkaConfig fromProperties(Properties properties) {
    Objects.requireNonNull(properties, "properties");
    for (String key : properties.stringPropertyNames()) {
      if (!KEYS.contains(key)) {
        throw new IllegalArgumentException("Unknown configuration key: " + key);
      }
    }
    return new ZkafkaConfig(
        new Broker(
            integer(properties, "broker.id"),
            text(properties, "broker.host"),
            integer(properties, "broker.port")),
        new ZooKeeperSettings(
            text(properties, "zookeeper.connect"),
            integer(properties, "zookeeper.session.timeout.ms"),
            integer(properties, "zookeeper.connect.timeout.ms")),
        new Execution(
            integer(properties, "broker.request.threads"),
            integer(properties, "broker.shutdown.timeout.ms"),
            integer(properties, "client.connect.timeout.ms"),
            integer(properties, "client.request.timeout.ms"),
            integer(properties, "replication.request.timeout.ms"),
            integer(properties, "controller.retry.delay.ms")),
        new Storage(
            Path.of(text(properties, "storage.directory")),
            integer(properties, "storage.max.record.bytes"),
            longInteger(properties, "storage.max.segment.bytes"),
            integer(properties, "storage.flush.interval.ms"),
            RecoveryMode.parse(text(properties, "storage.recovery.mode"))),
        new Replication(
            integer(properties, "replication.factor"),
            integer(properties, "replication.min.in.sync.replicas")),
        new Network(
            integer(properties, "network.max.frame.bytes"),
            integer(properties, "fetch.max.response.bytes"),
            integer(properties, "network.read.buffer.bytes"),
            integer(properties, "network.accept.poll.interval.ms"),
            integer(properties, "network.read.poll.interval.ms")),
        new ConsumerSettings(
            optionalInteger(properties, "consumer.poll.interval.ms"),
            integer(properties, "consumer.shutdown.timeout.ms")));
  }

  public record Broker(int id, String host, int port) {
    public Broker {
      if (id < 0) {
        throw new IllegalArgumentException("broker.id must be nonnegative");
      }
      nonblank(host, "broker.host");
      if (port < 1 || port > 65535) {
        throw new IllegalArgumentException("broker.port must be between 1 and 65535");
      }
    }
  }

  public record ZooKeeperSettings(
      String connect, int sessionTimeoutMillis, int connectTimeoutMillis) {
    public ZooKeeperSettings {
      nonblank(connect, "zookeeper.connect");
      positive(sessionTimeoutMillis, "zookeeper.session.timeout.ms");
      positive(connectTimeoutMillis, "zookeeper.connect.timeout.ms");
    }
  }

  public record Execution(
      int requestThreads,
      int shutdownTimeoutMillis,
      int clientConnectTimeoutMillis,
      int clientRequestTimeoutMillis,
      int replicationRequestTimeoutMillis,
      int controllerRetryDelayMillis) {
    public Execution {
      positive(requestThreads, "broker.request.threads");
      positive(shutdownTimeoutMillis, "broker.shutdown.timeout.ms");
      positive(clientConnectTimeoutMillis, "client.connect.timeout.ms");
      positive(clientRequestTimeoutMillis, "client.request.timeout.ms");
      positive(replicationRequestTimeoutMillis, "replication.request.timeout.ms");
      positive(controllerRetryDelayMillis, "controller.retry.delay.ms");
    }
  }

  public record Storage(
      Path directory,
      int maxRecordBytes,
      long maxSegmentBytes,
      int flushIntervalMillis,
      RecoveryMode recoveryMode) {
    public Storage {
      Objects.requireNonNull(directory, "storage.directory");
      Objects.requireNonNull(recoveryMode, "storage.recovery.mode");
      positive(maxRecordBytes, "storage.max.record.bytes");
      positive(flushIntervalMillis, "storage.flush.interval.ms");
      if (maxSegmentBytes < 6L + 16 + maxRecordBytes) {
        throw new IllegalArgumentException(
            "storage.max.segment.bytes must fit the segment header and maximum record");
      }
    }
  }

  public record Replication(int factor, int minInSyncReplicas) {
    public Replication {
      if (factor < 1 || factor > 32767) {
        throw new IllegalArgumentException("replication.factor must be between 1 and 32767");
      }
      if (minInSyncReplicas < 1 || minInSyncReplicas > factor) {
        throw new IllegalArgumentException(
            "replication.min.in.sync.replicas must be between 1 and replication.factor");
      }
    }
  }

  public record Network(
      int maxFrameBytes,
      int fetchMaxResponseBytes,
      int readBufferBytes,
      int acceptPollIntervalMillis,
      int readPollIntervalMillis) {
    public Network {
      positive(maxFrameBytes, "network.max.frame.bytes");
      positive(fetchMaxResponseBytes, "fetch.max.response.bytes");
      positive(readBufferBytes, "network.read.buffer.bytes");
      positive(acceptPollIntervalMillis, "network.accept.poll.interval.ms");
      positive(readPollIntervalMillis, "network.read.poll.interval.ms");
      if (readBufferBytes > maxFrameBytes) {
        throw new IllegalArgumentException(
            "network.read.buffer.bytes must not exceed network.max.frame.bytes");
      }
      if (fetchMaxResponseBytes > maxFrameBytes) {
        throw new IllegalArgumentException(
            "fetch.max.response.bytes must not exceed network.max.frame.bytes");
      }
    }
  }

  public record ConsumerSettings(OptionalInt pollIntervalMillis, int shutdownTimeoutMillis) {
    public ConsumerSettings {
      Objects.requireNonNull(pollIntervalMillis, "consumer.poll.interval.ms");
      pollIntervalMillis.ifPresent(value -> positive(value, "consumer.poll.interval.ms"));
      positive(shutdownTimeoutMillis, "consumer.shutdown.timeout.ms");
    }

    public int requiredPollIntervalMillis() {
      return pollIntervalMillis.orElseThrow(
          () ->
              new IllegalStateException(
                  "Set consumer.poll.interval.ms before starting background consumption"));
    }
  }

  public enum RecoveryMode {
    STRICT,
    TAIL_TRUNCATE;

    private static RecoveryMode parse(String value) {
      return switch (value) {
        case "strict" -> STRICT;
        case "tail-truncate" -> TAIL_TRUNCATE;
        default -> throw new IllegalArgumentException("Invalid storage.recovery.mode: " + value);
      };
    }
  }

  private static String text(Properties properties, String key) {
    String value = properties.getProperty(key);
    nonblank(value, key);
    return value.strip();
  }

  private static int integer(Properties properties, String key) {
    String value = text(properties, key);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(key + " must be a 32-bit integer: " + value, exception);
    }
  }

  private static OptionalInt optionalInteger(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null || value.isBlank()) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(integer(properties, key));
  }

  private static long longInteger(Properties properties, String key) {
    String value = text(properties, key);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(key + " must be a 64-bit integer: " + value, exception);
    }
  }

  private static void nonblank(String value, String key) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing or blank configuration: " + key);
    }
  }

  private static void positive(long value, String key) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be positive");
    }
  }
}
