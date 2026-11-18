package com.zkafka.config;

import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.protocol.Protocol;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Dependency-free configuration and codec integration checks. */
public final class ZkafkaConfigTest {
  private static int checks;

  public static void main(String[] args) throws Exception {
    Properties original = new Properties();
    try (Reader reader =
        Files.newBufferedReader(Path.of("config/zkafka.properties"), StandardCharsets.UTF_8)) {
      original.load(reader);
    }
    ZkafkaConfig config = ZkafkaConfig.fromProperties(original);
    check(
        config.broker().port() == Integer.parseInt(original.getProperty("broker.port")),
        "Broker port");
    check(
        config.network().maxFrameBytes() == config.network().fetchMaxResponseBytes(),
        "Shared frame/fetch cap");
    check(
        config.storage().recoveryMode() == ZkafkaConfig.RecoveryMode.TAIL_TRUNCATE,
        "Recovery mode");
    check(config.consumer().pollIntervalMillis().isEmpty(), "Undecided poll interval stays empty");
    rejectState(() -> config.consumer().requiredPollIntervalMillis(), "consumer.poll.interval.ms");

    reject(original, "broker.id", "-1");
    reject(original, "broker.port", "65536");
    reject(original, "broker.request.threads", "0");
    reject(original, "broker.host", " ");
    reject(original, "zookeeper.connect.timeout.ms", "");
    reject(original, "client.request.timeout.ms", "five seconds");
    reject(original, "client.request.timeout.ms", "2147483648");
    reject(original, "storage.max.segment.bytes", "12");
    reject(original, "storage.recovery.mode", "ignore-corruption");
    reject(original, "replication.min.in.sync.replicas", "100");
    reject(original, "fetch.max.response.bytes", "16777216");
    reject(original, "network.max.frame.bytes", "1024");
    reject(original, "network.read.buffer.bytes", "0");
    reject(original, "consumer.poll.interval.ms", "-1");
    reject(original, "broker.requset.threads", "10");

    Properties missing = copy(original);
    missing.remove("storage.directory");
    reject(() -> ZkafkaConfig.fromProperties(missing), "storage.directory");

    Properties custom = copy(original);
    custom.setProperty("broker.request.threads", "3");
    custom.setProperty("zookeeper.session.timeout.ms", "7000");
    custom.setProperty("consumer.poll.interval.ms", "200");
    custom.setProperty("storage.max.record.bytes", "4");
    custom.setProperty("storage.max.segment.bytes", "26");
    custom.setProperty("storage.flush.interval.ms", "3000");
    custom.setProperty("storage.recovery.mode", "strict");
    ZkafkaConfig small = ZkafkaConfig.fromProperties(custom);
    check(
        small.execution().requestThreads() == 3 && small.zooKeeper().sessionTimeoutMillis() == 7000,
        "Overrides reach typed groups");
    check(small.consumer().requiredPollIntervalMillis() == 200, "Explicit poll interval");
    check(
        small.storage().flushIntervalMillis() == 3000
            && small.storage().recoveryMode() == ZkafkaConfig.RecoveryMode.STRICT,
        "Storage settings");
    custom.setProperty("broker.request.threads", "9");
    check(small.execution().requestThreads() == 3, "Configuration snapshots mutable properties");

    Path saved = Files.createTempFile(Path.of("target"), "zkafka-config-", ".properties");
    try {
      Files.writeString(
          saved, "# UTF-8 settings\n" + toPropertiesText(custom), StandardCharsets.UTF_8);
      check(
          ZkafkaConfig.load(saved).execution().requestThreads() == 9, "Load from an explicit path");
    } finally {
      Files.deleteIfExists(saved);
    }

    Protocol protocol = new Protocol(small);
    protocol.encodeProduceRequest("orders", 0, new byte[4]);
    reject(
        () -> protocol.encodeProduceRequest("orders", 0, new byte[5]), "storage.max.record.bytes");
    reject(
        () -> protocol.encodeReplicateRequest("orders", 0, 0, new byte[5]),
        "storage.max.record.bytes");
    reject(
        () -> protocol.encodeFetchRequest("orders", 0, 0, small.network().fetchMaxResponseBytes()),
        "fetch.max.response.bytes");
    int maxFetchBytes = protocol.maxFetchBytes("orders");
    protocol.encodeFetchRequest("orders", 0, 0, maxFetchBytes);
    check(
        maxFetchBytes + 1 + 2 + 6 + 4 + 2 + 4 == small.network().fetchMaxResponseBytes(),
        "Fetch budget reserves response header");

    byte[] oversizedFetch = new byte[1 + 2 + 6 + 4 + 2 + 4 + 8 + 4 + 5];
    ByteBuffer body = ByteBuffer.wrap(oversizedFetch);
    body.put(Protocol.FETCH_RESPONSE)
        .putShort((short) 6)
        .put("orders".getBytes(StandardCharsets.UTF_8));
    body.putInt(0).putShort((short) 0).putInt(1).putLong(0).putInt(5).put(new byte[5]).flip();
    reject(() -> protocol.decodeFetchResponse(body), "storage.max.record.bytes");
    reject(
        () ->
            protocol.decodeProduceResponse(
                ByteBuffer.allocate(small.network().maxFrameBytes() + 1)),
        "network.max.frame.bytes");

    Protocol normal = new Protocol(config);
    normal.encodeProduceRequest("orders", 0, new byte[config.storage().maxRecordBytes()]);
    reject(
        () ->
            normal.encodeProduceRequest(
                "orders", 0, new byte[config.storage().maxRecordBytes() + 1]),
        "storage.max.record.bytes");
    check(
        normal
                .decodeProduceResponse(
                    normal.encodeProduceResponse("orders", 0, 42, ErrorCode.NONE))
                .offset()
            == 42,
        "Configured codec round trip");
    System.out.println("Configuration checks passed: " + checks);
  }

  private static Properties copy(Properties original) {
    Properties copy = new Properties();
    copy.putAll(original);
    return copy;
  }

  private static String toPropertiesText(Properties properties) {
    StringBuilder text = new StringBuilder();
    properties.stringPropertyNames().stream()
        .sorted()
        .forEach(
            key -> text.append(key).append('=').append(properties.getProperty(key)).append('\n'));
    return text.toString();
  }

  private static void reject(Properties original, String key, String value) {
    Properties changed = copy(original);
    changed.setProperty(key, value);
    // Cross-field failures can name the related property rather than this key.
    reject(() -> ZkafkaConfig.fromProperties(changed), null);
  }

  private static void reject(Runnable action, String message) {
    try {
      action.run();
    } catch (IllegalArgumentException expected) {
      check(
          message == null || expected.getMessage().contains(message),
          "Useful error: " + expected.getMessage());
      return;
    }
    throw new AssertionError("Expected invalid configuration/input");
  }

  private static void rejectState(Runnable action, String message) {
    try {
      action.run();
    } catch (IllegalStateException expected) {
      check(expected.getMessage().contains(message), "Missing optional setting");
      return;
    }
    throw new AssertionError("Expected missing optional setting");
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
    checks++;
  }
}
