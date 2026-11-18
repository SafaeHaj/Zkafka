package com.zkafka.broker;

import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.protocol.Protocol;
import com.zkafka.config.ZkafkaConfig;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** Dependency-free regression checks. Run main after compiling with Java 21. */
public final class ProtocolTest {
  private static int checks;
  private static Protocol protocol;

  public static void main(String[] args) throws Exception {
    protocol = new Protocol(ZkafkaConfig.load(Path.of("config/zkafka.properties")));
    requestLayouts();
    produceResponses();
    fetchResponses();
    metadataResponses();
    collectionOwnership();
    System.out.println("Protocol checks passed: " + checks);
  }

  private static void requestLayouts() {
    byte[] value = {1, 2, 3};
    checkBytes(
        protocol.encodeProduceRequest("café", 2, value),
        wire(
            out -> {
              out.writeByte(0x01);
              string(out, "café");
              out.writeInt(2);
              out.writeInt(3);
              out.write(value);
            }));
    checkBytes(
        protocol.encodeFetchRequest("orders", 2, 123L, 1024),
        wire(
            out -> {
              out.writeByte(0x02);
              string(out, "orders");
              out.writeInt(2);
              out.writeLong(123L);
              out.writeInt(1024);
            }));
    checkBytes(protocol.encodeMetadataRequest(), new byte[] {0x03});
    checkBytes(
        protocol.encodeCreateTopicRequest("orders", 3, (short) 2),
        wire(
            out -> {
              out.writeByte(0x04);
              string(out, "orders");
              out.writeInt(3);
              out.writeShort(2);
            }));
    checkBytes(
        protocol.encodeReplicateRequest("orders", 2, 123L, value),
        wire(
            out -> {
              out.writeByte(0x05);
              string(out, "orders");
              out.writeInt(2);
              out.writeLong(123L);
              out.writeInt(3);
              out.write(value);
            }));
    reject(() -> protocol.encodeProduceRequest(" ", 0, value));
    reject(() -> protocol.encodeProduceRequest("orders", -1, value));
    reject(() -> protocol.encodeProduceRequest("orders", 0, null));
    reject(() -> protocol.encodeFetchRequest("orders", 0, -1, 1));
    reject(() -> protocol.encodeFetchRequest("orders", 0, 0, 0));
    reject(() -> protocol.encodeCreateTopicRequest("orders", 0, (short) 1));
    reject(() -> protocol.encodeCreateTopicRequest("orders", 1, (short) 0));
    reject(() -> protocol.encodeReplicateRequest("orders", 0, -1, value));
    reject(() -> protocol.encodeProduceRequest("a".repeat(65536), 0, value));
  }

  private static void produceResponses() {
    byte[] success = produce(0, 42);
    checkBytes(protocol.encodeProduceResponse("orders", 0, 42, ErrorCode.NONE), success);
    Protocol.ProduceResult result = protocol.decodeProduceResponse(ByteBuffer.wrap(success));
    check(result.offset() == 42 && result.errorCode() == 0, "Produce result");
    checkBytes(
        protocol.encodeProduceResponse("orders", 0, 42, ErrorCode.NOT_LEADER), produce(3, -1));
    result = protocol.decodeProduceResponse(ByteBuffer.wrap(produce(65535, -1)));
    check(result.errorCode() == 65535, "Unknown unsigned error preserved");
    reject(() -> protocol.decodeProduceResponse(ByteBuffer.wrap(produce(0, -1))));
    reject(() -> protocol.decodeProduceResponse(ByteBuffer.wrap(produce(3, 42))));
    reject(() -> protocol.decodeProduceResponse(ByteBuffer.wrap(intAt(success, 9, -1))));
    byte[] invalidUtf8 = success.clone();
    invalidUtf8[3] = (byte) 0xFF;
    reject(() -> protocol.decodeProduceResponse(ByteBuffer.wrap(invalidUtf8)));
    byte[] blankTopic = success.clone();
    Arrays.fill(blankTopic, 3, 9, (byte) ' ');
    reject(() -> protocol.decodeProduceResponse(ByteBuffer.wrap(blankTopic)));
    String longTopic = "a".repeat(65535);
    check(
        protocol
                .decodeProduceResponse(
                    protocol.encodeProduceResponse(longTopic, 0, 1, ErrorCode.NONE))
                .offset()
            == 1,
        "Unsigned topic length");
    check(
        protocol
                .decodeProduceResponse(protocol.encodeProduceResponse("café", 0, 1, ErrorCode.NONE))
                .offset()
            == 1,
        "UTF-8 round trip");
    responseBoundaries(success, protocol::decodeProduceResponse);
  }

  private static void fetchResponses() {
    byte[] success = fetch(0, new byte[] {1, 2, 3}, new byte[0]);
    Protocol.FetchResult result = protocol.decodeFetchResponse(ByteBuffer.wrap(success));
    check(
        result.messages().size() == 2
            && result.messages().get(0).offset() == 10
            && result.messages().get(1).offset() == 11,
        "Fetch order and offsets");
    check(
        Arrays.equals(result.messages().get(0).value(), new byte[] {1, 2, 3})
            && result.messages().get(1).value().length == 0,
        "Fetch binary and empty values");
    check(
        protocol.decodeFetchResponse(ByteBuffer.wrap(fetch(0))).messages().isEmpty(),
        "Empty fetch");
    check(
        protocol.decodeFetchResponse(ByteBuffer.wrap(fetch(65535))).errorCode() == 65535,
        "Unknown fetch error");
    reject(() -> protocol.decodeFetchResponse(ByteBuffer.wrap(fetch(3, new byte[0]))));
    // With the six-byte topic, count is at 15, first offset at 19, length at 27.
    reject(() -> protocol.decodeFetchResponse(ByteBuffer.wrap(intAt(success, 15, -1))));
    reject(
        () -> protocol.decodeFetchResponse(ByteBuffer.wrap(intAt(success, 15, Integer.MAX_VALUE))));
    reject(() -> protocol.decodeFetchResponse(ByteBuffer.wrap(intAt(success, 27, -1))));
    reject(
        () -> protocol.decodeFetchResponse(ByteBuffer.wrap(intAt(success, 27, Integer.MAX_VALUE))));
    byte[] negativeOffset = success.clone();
    ByteBuffer.wrap(negativeOffset).putLong(19, -1L);
    reject(() -> protocol.decodeFetchResponse(ByteBuffer.wrap(negativeOffset)));
    responseBoundaries(success, protocol::decodeFetchResponse);
  }

  private static void metadataResponses() {
    byte[] success = metadata(0, 1, 1, 1, 1);
    Protocol.MetadataResult result = protocol.decodeMetadataResponse(ByteBuffer.wrap(success));
    check(
        result.brokers().get(0).getHost().equals("localhost")
            && result.brokers().get(0).getPort() == 9092,
        "Broker address");
    Protocol.TopicMetadata topic = result.topics().get(0);
    check(
        topic.topicName().equals("orders")
            && topic.partitionMetadata().get(0).leaderBrokerId() == 1
            && topic.partitionMetadata().get(0).replicaBrokerIds().equals(List.of(1)),
        "Metadata hierarchy");
    check(
        protocol
                .decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 1, 1, 2, 1)))
                .topics()
                .get(0)
                .partitionMetadata()
                .size()
            == 2,
        "Multiple partitions");
    check(
        protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 0, 0, 0))).brokers().isEmpty(),
        "Empty successful metadata");
    check(
        protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(65535, 0, 0, 0))).errorCode()
            == 65535,
        "Unknown metadata error");
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(8, 1, 1, 1, 1))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 2, 1, 1, 1))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 1, 2, 1, 1))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 1, 1, 1, 1, 1))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(metadata(0, 1, 1, 1, 2))));
    // Offsets in the single-broker, single-topic fixture.
    int[] countPositions = {3, 26, 38, 50};
    for (int position : countPositions) {
      reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(intAt(success, position, -1))));
      reject(
          () ->
              protocol.decodeMetadataResponse(
                  ByteBuffer.wrap(intAt(success, position, Integer.MAX_VALUE))));
    }
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(intAt(success, 22, 0))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(intAt(success, 42, -1))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(intAt(success, 46, 2))));
    reject(() -> protocol.decodeMetadataResponse(ByteBuffer.wrap(intAt(success, 54, -1))));
    responseBoundaries(success, protocol::decodeMetadataResponse);
  }

  private static void collectionOwnership() {
    List<Protocol.FetchedMessage> source = new ArrayList<>();
    Protocol.FetchResult result = new Protocol.FetchResult(source, 0);
    source.add(new Protocol.FetchedMessage(0, new byte[0]));
    check(result.messages().isEmpty(), "Result snapshots source list");
    try {
      result.messages().clear();
      throw new AssertionError("Result list must be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      checks++;
    }
  }

  private static void responseBoundaries(byte[] body, Function<ByteBuffer, ?> decode) {
    // Every truncated prefix must fail with our validation exception,
    // not BufferUnderflowException or an allocation/index error.
    for (int length = 0; length < body.length; length++) {
      byte[] prefix = Arrays.copyOf(body, length);
      reject(() -> decode.apply(ByteBuffer.wrap(prefix)));
    }
    reject(() -> decode.apply(ByteBuffer.wrap(Arrays.copyOf(body, body.length + 1))));
    byte[] wrongType = body.clone();
    wrongType[0] = 0x7F;
    reject(() -> decode.apply(ByteBuffer.wrap(wrongType)));

    ByteBuffer positioned = ByteBuffer.allocate(body.length + 8);
    positioned.position(3);
    positioned.put(body);
    positioned.limit(positioned.position());
    positioned.position(3);
    decode.apply(positioned.order(ByteOrder.LITTLE_ENDIAN));
    check(!positioned.hasRemaining(), "Respects current position/limit and forces big endian");
    decode.apply(ByteBuffer.wrap(body).asReadOnlyBuffer());
    ByteBuffer direct = ByteBuffer.allocateDirect(body.length);
    direct.put(body).flip();
    decode.apply(direct);
    checks += 2;
  }

  // Independent fixtures use DataOutputStream rather than Protocol encoders.
  private static byte[] produce(int error, long offset) {
    return wire(
        out -> {
          out.writeByte(0x11);
          string(out, "orders");
          out.writeInt(0);
          out.writeLong(offset);
          out.writeShort(error);
        });
  }

  private static byte[] fetch(int error, byte[]... messages) {
    return wire(
        out -> {
          out.writeByte(0x12);
          string(out, "orders");
          out.writeInt(0);
          out.writeShort(error);
          out.writeInt(messages.length);
          for (int i = 0; i < messages.length; i++) {
            out.writeLong(10L + i);
            out.writeInt(messages[i].length);
            out.write(messages[i]);
          }
        });
  }

  private static byte[] metadata(
      int error, int brokers, int topics, int partitions, int... replicas) {
    return wire(
        out -> {
          out.writeByte(0x13);
          out.writeShort(error);
          out.writeInt(brokers);
          for (int i = 0; i < brokers; i++) {
            out.writeInt(1); // Repeated IDs deliberately exercise duplicate detection.
            string(out, "localhost");
            out.writeInt(9092);
          }
          out.writeInt(topics);
          for (int i = 0; i < topics; i++) {
            string(out, "orders");
            out.writeInt(partitions);
            for (int j = 0; j < partitions; j++) {
              out.writeInt(j);
              out.writeInt(1);
              out.writeInt(replicas.length);
              for (int replica : replicas) {
                out.writeInt(replica);
              }
            }
          }
        });
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeShort(bytes.length);
    out.write(bytes);
  }

  private static byte[] wire(Writer writer) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      writer.write(out);
      out.flush();
      return bytes.toByteArray();
    } catch (IOException exception) {
      throw new AssertionError(exception);
    }
  }

  private static byte[] intAt(byte[] original, int position, int value) {
    byte[] changed = original.clone();
    ByteBuffer.wrap(changed).putInt(position, value);
    return changed;
  }

  private static void checkBytes(ByteBuffer actual, byte[] expected) {
    check(actual.position() == 0 && actual.order() == ByteOrder.BIG_ENDIAN, "Encoder buffer state");
    byte[] bytes = new byte[actual.remaining()];
    actual.get(bytes);
    check(Arrays.equals(bytes, expected), "Encoded byte layout");
  }

  private static void reject(Runnable action) {
    try {
      action.run();
    } catch (IllegalArgumentException expected) {
      checks++;
      return;
    }
    throw new AssertionError("Expected IllegalArgumentException");
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
    checks++;
  }

  @FunctionalInterface
  private interface Writer {
    void write(DataOutputStream out) throws IOException;
  }
}
