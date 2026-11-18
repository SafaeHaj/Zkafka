package com.zkafka.common.protocol;

import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.model.BrokerInfo;
import com.zkafka.config.ZkafkaConfig;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Big-endian message bodies with UTF-8 strings (unsigned two-byte lengths), four-byte list
 * counts/value lengths, and unsigned two-byte error codes.
 *
 * <p>Decoders consume one complete body from the current position to the limit. Networking must
 * assemble and bound that body; TCP framing is separate. Position advances during decoding,
 * including on failure. Unknown error codes are preserved as integers.
 */
public final class Protocol {
  private final int maxRecordBytes;
  private final int maxFrameBytes;
  private final int fetchMaxResponseBytes;

  public Protocol(ZkafkaConfig config) {
    Objects.requireNonNull(config, "config");
    maxRecordBytes = config.storage().maxRecordBytes();
    maxFrameBytes = config.network().maxFrameBytes();
    fetchMaxResponseBytes = config.network().fetchMaxResponseBytes();
  }

  // Client and broker request types
  public static final byte PRODUCE = 0x01;
  public static final byte FETCH = 0x02;
  public static final byte METADATA = 0x03;
  public static final byte CREATE_TOPIC = 0x04;
  public static final byte REPLICATE = 0x05;

  // Broker response types
  public static final byte PRODUCE_RESPONSE = 0x11;
  public static final byte FETCH_RESPONSE = 0x12;
  public static final byte METADATA_RESPONSE = 0x13;

  public record ProduceResult(long offset, int errorCode) {}

  public record FetchedMessage(long offset, byte[] value) {}

  public record FetchResult(List<FetchedMessage> messages, int errorCode) {
    public FetchResult {
      messages = List.copyOf(messages);
    }
  }

  public record PartitionMetadata(
      int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
    public PartitionMetadata {
      replicaBrokerIds = List.copyOf(replicaBrokerIds);
    }
  }

  public record TopicMetadata(String topicName, List<PartitionMetadata> partitionMetadata) {
    public TopicMetadata {
      partitionMetadata = List.copyOf(partitionMetadata);
    }
  }

  public record MetadataResult(
      List<BrokerInfo> brokers, List<TopicMetadata> topics, int errorCode) {
    public MetadataResult {
      brokers = List.copyOf(brokers);
      topics = List.copyOf(topics);
    }
  }

  // Request encoding

  // type | topic | partition | value length | value
  public ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
    byte[] topicBytes = encodeTopic(topic);
    requireNonnegative(partition, "Partition");
    requireMessage(message);

    int size = Math.addExact(1 + 2 + topicBytes.length + 4 + 4, message.length);
    ByteBuffer buffer = allocate(size);
    buffer.put(PRODUCE);
    putString(buffer, topicBytes);
    buffer.putInt(partition);
    buffer.putInt(message.length);
    buffer.put(message);
    return buffer.flip();
  }

  // type | topic | partition | offset | max bytes
  public ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
    byte[] topicBytes = encodeTopic(topic);
    requireNonnegative(partition, "Partition");
    requireNonnegative(offset, "Offset");
    if (maxBytes <= 0) {
      throw new IllegalArgumentException("Max bytes must be positive");
    }
    if (maxBytes > maxFetchBytes(topic)) {
      throw new IllegalArgumentException("Fetch budget exceeds fetch.max.response.bytes");
    }

    ByteBuffer buffer = allocate(1 + 2 + topicBytes.length + 4 + 8 + 4);
    buffer.put(FETCH);
    putString(buffer, topicBytes);
    buffer.putInt(partition);
    buffer.putLong(offset);
    buffer.putInt(maxBytes);
    return buffer.flip();
  }

  // Requests all cluster metadata; no other fields are required.
  public ByteBuffer encodeMetadataRequest() {
    ByteBuffer buffer = allocate(1);
    buffer.put(METADATA);
    return buffer.flip();
  }

  // type | topic | partition count | replication factor
  public ByteBuffer encodeCreateTopicRequest(
      String topic, int numPartitions, short replicationFactor) {
    byte[] topicBytes = encodeTopic(topic);
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be positive");
    }
    if (replicationFactor <= 0) {
      throw new IllegalArgumentException("Replication factor must be positive");
    }

    ByteBuffer buffer = allocate(1 + 2 + topicBytes.length + 4 + 2);
    buffer.put(CREATE_TOPIC);
    putString(buffer, topicBytes);
    buffer.putInt(numPartitions);
    buffer.putShort(replicationFactor);
    return buffer.flip();
  }

  // type | topic | partition | offset | value length | value
  public ByteBuffer encodeReplicateRequest(
      String topic, int partition, long offset, byte[] message) {
    byte[] topicBytes = encodeTopic(topic);
    requireNonnegative(partition, "Partition");
    requireNonnegative(offset, "Offset");
    requireMessage(message);

    int size = Math.addExact(1 + 2 + topicBytes.length + 4 + 8 + 4, message.length);
    ByteBuffer buffer = allocate(size);
    buffer.put(REPLICATE);
    putString(buffer, topicBytes);
    buffer.putInt(partition);
    buffer.putLong(offset);
    buffer.putInt(message.length);
    buffer.put(message);
    return buffer.flip();
  }

  public ByteBuffer encodeTopicNotification(String topic) {
    throw new UnsupportedOperationException("Topic notifications are not implemented");
  }

  public static final byte CREATE_TOPIC_RESPONSE = 0x14;
  public static final byte REPLICATE_RESPONSE = 0x15;

  public record DataRequest(String topic, int partition, long offset, int maxBytes, byte[] value) {}

  public record CreateTopicRequest(String topic, int partitions, short replicationFactor) {}

  public DataRequest decodeDataRequest(ByteBuffer body, byte type) {
    String topic = decodeTopic(body);
    requireRemaining(body, 4 + (type == PRODUCE ? 0 : 8) + 4, "request fields");
    int partition = body.getInt();
    long offset = type == PRODUCE ? 0 : body.getLong();
    int size = body.getInt();
    requireNonnegative(partition, "Partition");
    requireNonnegative(offset, "Offset");
    byte[] value = null;
    if (type == FETCH) {
      if (size <= 0 || size > maxFetchBytes(topic)) {
        throw new IllegalArgumentException("Invalid fetch budget");
      }
    } else {
      if (size < 0 || size != body.remaining()) {
        throw new IllegalArgumentException("Invalid value length");
      }
      value = new byte[size];
      body.get(value);
    }
    requireComplete(body);
    return new DataRequest(topic, partition, offset, size, value);
  }

  public CreateTopicRequest decodeCreateTopicRequest(ByteBuffer body) {
    String topic = decodeTopic(body);
    requireRemaining(body, 6, "topic configuration");
    int partitions = body.getInt();
    short factor = body.getShort();
    requireComplete(body);
    return new CreateTopicRequest(topic, partitions, factor);
  }

  public ByteBuffer encodeStatus(byte type, ErrorCode error) {
    return allocate(3).put(type).putShort((short) error.code()).flip();
  }

  public ByteBuffer encodeFetchResponse(
      String topic, int partition, List<FetchedMessage> messages, ErrorCode error) {
    byte[] name = encodeTopic(topic);
    int size = 13 + name.length;
    for (FetchedMessage message : messages) {
      size = Math.addExact(size, 12 + message.value().length);
    }
    ByteBuffer body = allocate(size).put(FETCH_RESPONSE);
    putString(body, name);
    body.putInt(partition).putShort((short) error.code()).putInt(messages.size());
    for (FetchedMessage message : messages) {
      body.putLong(message.offset()).putInt(message.value().length).put(message.value());
    }
    return body.flip();
  }

  public ByteBuffer encodeMetadataResponse(
      List<BrokerInfo> brokers, List<TopicMetadata> topics, ErrorCode error) {
    long size = 11;
    for (BrokerInfo broker : brokers) {
      size += 10 + encodeTopic(broker.getHost()).length;
    }
    for (TopicMetadata topic : topics) {
      size += 6 + encodeTopic(topic.topicName()).length;
      for (PartitionMetadata partition : topic.partitionMetadata()) {
        size += 12L + 4L * partition.replicaBrokerIds().size();
      }
    }
    if (size > maxFrameBytes) {
      return encodeMetadataResponse(List.of(), List.of(), ErrorCode.INTERNAL_ERROR);
    }
    ByteBuffer body = allocate((int) size).put(METADATA_RESPONSE);
    body.putShort((short) error.code()).putInt(brokers.size());
    for (BrokerInfo broker : brokers) {
      body.putInt(broker.getId());
      putString(body, encodeTopic(broker.getHost()));
      body.putInt(broker.getPort());
    }
    body.putInt(topics.size());
    for (TopicMetadata topic : topics) {
      putString(body, encodeTopic(topic.topicName()));
      body.putInt(topic.partitionMetadata().size());
      for (PartitionMetadata partition : topic.partitionMetadata()) {
        body.putInt(partition.partitionId()).putInt(partition.leaderBrokerId());
        body.putInt(partition.replicaBrokerIds().size());
        partition.replicaBrokerIds().forEach(body::putInt);
      }
    }
    return body.flip();
  }

  // Response encoding

  // type | topic | partition | offset | error code
  public ByteBuffer encodeProduceResponse(
      String topic, int partition, long offset, ErrorCode error) {
    Objects.requireNonNull(error, "Error code must not be null");
    byte[] topicBytes = encodeTopic(topic);
    requireNonnegative(partition, "Partition");
    if (error == ErrorCode.NONE) {
      requireNonnegative(offset, "Successful produce offset");
    }

    ByteBuffer buffer = allocate(1 + 2 + topicBytes.length + 4 + 8 + 2);
    buffer.put(PRODUCE_RESPONSE);
    putString(buffer, topicBytes);
    buffer.putInt(partition);
    buffer.putLong(error == ErrorCode.NONE ? offset : -1L);
    buffer.putShort((short) error.code());
    return buffer.flip();
  }

  // Response decoding. Echoed topics/partitions are validated but not returned
  // the caller must associate each response with its request

  public ProduceResult decodeProduceResponse(ByteBuffer buffer) {
    return decodeProduceResponse(buffer, (ResponseTarget) null);
  }

  public ProduceResult decodeProduceResponse(ByteBuffer buffer, String topic, int partition) {
    return decodeProduceResponse(buffer, expectedTarget(topic, partition));
  }

  private ProduceResult decodeProduceResponse(ByteBuffer buffer, ResponseTarget expected) {
    beginResponse(buffer, PRODUCE_RESPONSE, 1 + 2 + 4 + 8 + 2, "produce response");
    decodeResponseTarget(buffer, 8 + 2, expected);
    long offset = buffer.getLong();
    int errorCode = Short.toUnsignedInt(buffer.getShort());
    if (errorCode == ErrorCode.NONE.code()) {
      requireNonnegative(offset, "Successful produce offset");
    } else if (offset != -1L) {
      throw new IllegalArgumentException("Failed produce offset must be -1");
    }
    requireComplete(buffer);
    return new ProduceResult(offset, errorCode);
  }

  // type | topic | partition | error code | count | (offset, value length,
  // value)*
  public FetchResult decodeFetchResponse(ByteBuffer buffer) {
    return decodeFetchResponse(buffer, (ResponseTarget) null);
  }

  public FetchResult decodeFetchResponse(ByteBuffer buffer, String topic, int partition) {
    return decodeFetchResponse(buffer, expectedTarget(topic, partition));
  }

  private FetchResult decodeFetchResponse(ByteBuffer buffer, ResponseTarget expected) {
    beginResponse(buffer, FETCH_RESPONSE, 1 + 2 + 4 + 2 + 4, "fetch response");
    if (buffer.remaining() + 1 > fetchMaxResponseBytes) {
      throw new IllegalArgumentException("Fetch response exceeds fetch.max.response.bytes");
    }
    decodeResponseTarget(buffer, 2 + 4, expected);
    int errorCode = Short.toUnsignedInt(buffer.getShort());
    List<FetchedMessage> messages =
        decodeList(buffer, 8 + 4, "message", this::decodeFetchedMessage);
    requireEmptyOnError(errorCode, messages, "messages");
    requireComplete(buffer);
    return new FetchResult(messages, errorCode);
  }

  // type | error code | brokers | topics (each topic contains partitions)
  public MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
    beginResponse(buffer, METADATA_RESPONSE, 1 + 2 + 4 + 4, "metadata response");
    int errorCode = Short.toUnsignedInt(buffer.getShort());
    List<BrokerInfo> brokers = decodeList(buffer, 4 + 2 + 4, "broker", Protocol::decodeBroker);
    List<TopicMetadata> topics = decodeList(buffer, 2 + 4, "topic", Protocol::decodeTopicMetadata);
    requireEmptyOnError(errorCode, brokers, "brokers");
    requireEmptyOnError(errorCode, topics, "topics");
    requireComplete(buffer);
    validateMetadata(brokers, topics);
    return new MetadataResult(brokers, topics, errorCode);
  }

  // Entry decoders check their own field boundaries.

  private record ResponseTarget(String topic, int partition) {}

  private static ResponseTarget expectedTarget(String topic, int partition) {
    requireTopic(topic);
    requireNonnegative(partition, "Partition");
    return new ResponseTarget(topic, partition);
  }

  private static void decodeResponseTarget(
      ByteBuffer buffer, int followingBytes, ResponseTarget expected) {
    String topic = decodeTopic(buffer);
    requireRemaining(buffer, 4 + followingBytes, "response fields");
    int partition = buffer.getInt();
    requireNonnegative(partition, "Partition");
    if (expected != null
        && (!topic.equals(expected.topic()) || partition != expected.partition())) {
      throw new IllegalArgumentException("Response topic/partition does not match the request");
    }
  }

  private FetchedMessage decodeFetchedMessage(ByteBuffer buffer) {
    requireRemaining(buffer, 8 + 4, "message header");
    long offset = buffer.getLong();
    requireNonnegative(offset, "Message offset");
    int length = buffer.getInt();
    requireNonnegative(length, "Message length");
    if (length > maxRecordBytes) {
      throw new IllegalArgumentException("Message exceeds storage.max.record.bytes");
    }
    requireRemaining(buffer, length, "message value");
    byte[] value = new byte[length];
    buffer.get(value);
    return new FetchedMessage(offset, value);
  }

  private static BrokerInfo decodeBroker(ByteBuffer buffer) {
    requireRemaining(buffer, 4, "broker ID");
    int id = buffer.getInt();
    String host = decodeString(buffer);
    requireRemaining(buffer, 4, "broker port");
    return new BrokerInfo(id, host, buffer.getInt());
  }

  private static TopicMetadata decodeTopicMetadata(ByteBuffer buffer) {
    String topic = decodeTopic(buffer);
    List<PartitionMetadata> partitions =
        decodeList(buffer, 4 + 4 + 4, "partition", Protocol::decodePartitionMetadata);
    return new TopicMetadata(topic, partitions);
  }

  private static PartitionMetadata decodePartitionMetadata(ByteBuffer buffer) {
    requireRemaining(buffer, 4 + 4 + 4, "partition header");
    int id = buffer.getInt();
    int leader = buffer.getInt();
    requireNonnegative(id, "Partition ID");
    requireNonnegative(leader, "Leader broker ID");
    List<Integer> replicas =
        decodeList(
            buffer,
            4,
            "replica",
            entry -> {
              requireRemaining(entry, 4, "replica ID");
              int replica = entry.getInt();
              requireNonnegative(replica, "Replica broker ID");
              return replica;
            });
    return new PartitionMetadata(id, leader, replicas);
  }

  // This initial protocol requires an assigned leader in every partition.
  private static void validateMetadata(List<BrokerInfo> brokers, List<TopicMetadata> topics) {
    Set<Integer> brokerIds = new HashSet<>();
    for (BrokerInfo broker : brokers) {
      if (!brokerIds.add(broker.getId())) {
        throw new IllegalArgumentException("Duplicate broker ID: " + broker.getId());
      }
    }
    Set<String> topicNames = new HashSet<>();
    for (TopicMetadata topic : topics) {
      if (!topicNames.add(topic.topicName())) {
        throw new IllegalArgumentException("Duplicate topic: " + topic.topicName());
      }
      Set<Integer> partitionIds = new HashSet<>();
      for (PartitionMetadata partition : topic.partitionMetadata()) {
        if (!partitionIds.add(partition.partitionId())) {
          throw new IllegalArgumentException("Duplicate partition in topic " + topic.topicName());
        }
        Set<Integer> replicas = new HashSet<>(partition.replicaBrokerIds());
        if (replicas.size() != partition.replicaBrokerIds().size()
            || !replicas.contains(partition.leaderBrokerId())
            || !brokerIds.containsAll(replicas)) {
          throw new IllegalArgumentException(
              "Invalid replicas or leader in topic " + topic.topicName());
        }
      }
    }
  }

  // Shared buffer and validation operations.

  private ByteBuffer allocate(int size) {
    if (size > maxFrameBytes) {
      throw new IllegalArgumentException("Message exceeds network.max.frame.bytes");
    }
    return ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
  }

  public int maxFetchBytes(String topic) {
    int topicBytes = encodeTopic(topic).length;
    return fetchMaxResponseBytes - (1 + 2 + topicBytes + 4 + 2 + 4);
  }

  // Only accepts already validated UTF-8 bytes from encodeTopic.
  private static void putString(ByteBuffer buffer, byte[] encoded) {
    buffer.putShort((short) encoded.length);
    buffer.put(encoded);
  }

  private static byte[] encodeTopic(String topic) {
    requireTopic(topic);
    byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 65535) {
      throw new IllegalArgumentException("Topic exceeds the two-byte length limit");
    }
    return bytes;
  }

  private static String decodeTopic(ByteBuffer buffer) {
    String topic = decodeString(buffer);
    requireTopic(topic);
    return topic;
  }

  private static String decodeString(ByteBuffer buffer) {
    requireRemaining(buffer, 2, "string length");
    int length = Short.toUnsignedInt(buffer.getShort());
    requireRemaining(buffer, length, "string");
    ByteBuffer bytes = buffer.slice();
    bytes.limit(length);
    try {
      String value =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(bytes)
              .toString();
      buffer.position(buffer.position() + length);
      return value;
    } catch (CharacterCodingException exception) {
      throw new IllegalArgumentException("Invalid UTF-8 string", exception);
    }
  }

  private static <T> List<T> decodeList(
      ByteBuffer buffer, int minimumEntrySize, String field, Function<ByteBuffer, T> decodeEntry) {
    if (minimumEntrySize <= 0) {
      throw new IllegalArgumentException("Minimum entry size must be positive");
    }
    requireRemaining(buffer, 4, field + " count");
    int count = buffer.getInt();
    if (count < 0 || count > buffer.remaining() / minimumEntrySize) {
      throw new IllegalArgumentException("Invalid " + field + " count");
    }
    List<T> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entries.add(decodeEntry.apply(buffer));
    }
    return entries;
  }

  private void beginResponse(ByteBuffer buffer, byte expectedType, int minimumSize, String field) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    if (buffer.remaining() > maxFrameBytes) {
      throw new IllegalArgumentException("Response exceeds network.max.frame.bytes");
    }
    buffer.order(ByteOrder.BIG_ENDIAN);
    requireRemaining(buffer, minimumSize, field);
    byte type = buffer.get();
    if (type != expectedType) {
      throw new IllegalArgumentException(
          "Unexpected response type: 0x" + Integer.toHexString(type & 0xFF));
    }
  }

  private static void requireRemaining(ByteBuffer buffer, int size, String field) {
    if (buffer.remaining() < size) {
      throw new IllegalArgumentException("Truncated " + field);
    }
  }

  private static void requireComplete(ByteBuffer buffer) {
    if (buffer.hasRemaining()) {
      throw new IllegalArgumentException("Trailing bytes after response body");
    }
  }

  private static void requireEmptyOnError(int errorCode, List<?> entries, String field) {
    if (errorCode != ErrorCode.NONE.code() && !entries.isEmpty()) {
      throw new IllegalArgumentException("Failed response must contain no " + field);
    }
  }

  private static void requireTopic(String topic) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Topic must not be null or blank");
    }
  }

  private static void requireNonnegative(long value, String field) {
    if (value < 0) {
      throw new IllegalArgumentException(field + " must be nonnegative: " + value);
    }
  }

  private void requireMessage(byte[] message) {
    if (message == null) {
      throw new IllegalArgumentException("Message must not be null");
    }
    if (message.length > maxRecordBytes) {
      throw new IllegalArgumentException("Message exceeds storage.max.record.bytes");
    }
  }
}
