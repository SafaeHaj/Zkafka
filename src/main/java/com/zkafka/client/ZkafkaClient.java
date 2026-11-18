package com.zkafka.client;

import com.zkafka.broker.core.ClientValidation;
import com.zkafka.client.network.RequestTransport;
import com.zkafka.client.network.SocketTransport;
import com.zkafka.common.exception.BrokerException;
import com.zkafka.common.exception.ClientProtocolException;
import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.model.BrokerInfo;
import com.zkafka.common.model.Record;
import com.zkafka.common.protocol.Protocol;
import com.zkafka.common.protocol.Protocol.PartitionMetadata;
import com.zkafka.common.protocol.Protocol.TopicMetadata;
import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Metadata routing and response handling. Produce requests are never automatically retried. */
public final class ZkafkaClient implements AutoCloseable {
  private final Protocol protocol;
  private final RequestTransport transport;
  private final String bootstrapHost;
  private final int bootstrapPort;
  private final int maxRecordBytes;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object metadataLock = new Object();
  private volatile MetadataSnapshot metadata;

  private record MetadataSnapshot(
      Map<Integer, BrokerInfo> brokers, Map<String, TopicMetadata> topics) {}

  public ZkafkaClient(ZkafkaConfig config) {
    this(config, new SocketTransport(config));
  }

  /** This client owns the injected transport and closes it. */
  public ZkafkaClient(ZkafkaConfig config, RequestTransport transport) {
    Objects.requireNonNull(config, "config");
    this.protocol = new Protocol(config);
    this.transport = Objects.requireNonNull(transport, "transport");
    this.bootstrapHost = config.broker().host();
    this.bootstrapPort = config.broker().port();
    this.maxRecordBytes = config.storage().maxRecordBytes();
  }

  public void initialize() throws IOException {
    refreshMetadata();
  }

  public void refreshMetadata() throws IOException {
    refreshSnapshot();
  }

  private MetadataSnapshot refreshSnapshot() throws IOException {
    synchronized (metadataLock) {
      requireOpen();
      Protocol.MetadataResult result =
          exchange(
              bootstrapHost,
              bootstrapPort,
              protocol.encodeMetadataRequest(),
              protocol::decodeMetadataResponse);
      requireSuccess("Metadata", result.errorCode());
      Map<Integer, BrokerInfo> brokers = new HashMap<>();
      Map<String, TopicMetadata> topics = new HashMap<>();
      for (BrokerInfo broker : result.brokers()) {
        brokers.put(broker.getId(), broker);
      }
      for (TopicMetadata topic : result.topics()) {
        topics.put(topic.topicName(), topic);
      }
      MetadataSnapshot snapshot = new MetadataSnapshot(Map.copyOf(brokers), Map.copyOf(topics));
      metadata = snapshot;
      return snapshot;
    }
  }

  public List<Integer> partitions(String topic) throws IOException {
    ClientValidation.topic(topic);
    TopicMetadata found = snapshot().topics().get(topic);
    if (found == null) {
      found = refreshSnapshot().topics().get(topic);
    }
    if (found == null || found.partitionMetadata().isEmpty()) {
      throw new IOException("No partitions available for topic: " + topic);
    }
    return found.partitionMetadata().stream().map(PartitionMetadata::partitionId).sorted().toList();
  }

  /** A transport failure can leave delivery uncertain; the caller decides whether to retry. */
  public long send(String topic, int partition, byte[] message) throws IOException {
    validateMessage(topic, message);
    ClientValidation.partition(partition);
    ByteBuffer request = protocol.encodeProduceRequest(topic, partition, message);
    BrokerInfo leader = leader(topic, partition);
    Protocol.ProduceResult result =
        exchange(
            leader.getHost(),
            leader.getPort(),
            request,
            body -> protocol.decodeProduceResponse(body, topic, partition));
    requireSuccess("Produce", result.errorCode());
    return result.offset();
  }

  public List<Record> fetch(String topic, int partition, long offset, int maxBytes)
      throws IOException {
    ClientValidation.offset(offset);
    ByteBuffer request = protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
    BrokerInfo leader = leader(topic, partition);
    Protocol.FetchResult result =
        exchange(
            leader.getHost(),
            leader.getPort(),
            request,
            body -> protocol.decodeFetchResponse(body, topic, partition));
    requireSuccess("Fetch", result.errorCode());

    List<Record> records = new ArrayList<>();
    long previous = -1;
    long bytes = 0;
    for (Protocol.FetchedMessage message : result.messages()) {
      if (message.offset() < offset
          || message.offset() <= previous
          || message.offset() == Long.MAX_VALUE) {
        throw new ClientProtocolException("Fetch offsets are outside the requested range or order");
      }
      bytes += 12L + message.value().length;
      if (bytes > maxBytes) {
        throw new ClientProtocolException("Fetch response exceeds the requested byte budget");
      }
      records.add(new Record(message.offset(), message.value()));
      previous = message.offset();
    }
    return List.copyOf(records);
  }

  public int maxFetchBytes(String topic) {
    return protocol.maxFetchBytes(topic);
  }

  public void validateMessage(String topic, byte[] message) {
    ClientValidation.topic(topic);
    ClientValidation.value(message, maxRecordBytes);
  }

  private BrokerInfo leader(String topic, int partition) throws IOException {
    ClientValidation.topic(topic);
    ClientValidation.partition(partition);
    MetadataSnapshot snapshot = snapshot();
    PartitionMetadata assignment = assignment(snapshot, topic, partition);
    if (assignment == null) {
      snapshot = refreshSnapshot();
      assignment = assignment(snapshot, topic, partition);
    }
    if (assignment == null) {
      throw new IOException("Unknown topic/partition in metadata: " + topic + "/" + partition);
    }
    BrokerInfo leader = snapshot.brokers().get(assignment.leaderBrokerId());
    if (leader == null) {
      throw new ClientProtocolException("Partition leader is missing from metadata");
    }
    return leader;
  }

  private static PartitionMetadata assignment(
      MetadataSnapshot snapshot, String topic, int partition) {
    TopicMetadata metadata = snapshot.topics().get(topic);
    if (metadata == null) {
      return null;
    }
    return metadata.partitionMetadata().stream()
        .filter(value -> value.partitionId() == partition)
        .findFirst()
        .orElse(null);
  }

  private MetadataSnapshot snapshot() throws IOException {
    requireOpen();
    MetadataSnapshot snapshot = metadata;
    if (snapshot == null) {
      synchronized (metadataLock) {
        snapshot = metadata;
        if (snapshot == null) {
          snapshot = refreshSnapshot();
        }
      }
    }
    return snapshot;
  }

  private <T> T exchange(String host, int port, ByteBuffer request, Function<ByteBuffer, T> decode)
      throws IOException {
    requireOpen();
    ByteBuffer response = transport.exchange(host, port, request);
    try {
      return decode.apply(response);
    } catch (IllegalArgumentException exception) {
      throw new ClientProtocolException("Invalid response from " + host + ":" + port, exception);
    }
  }

  private void requireSuccess(String operation, int errorCode) throws BrokerException {
    if (errorCode == ErrorCode.NONE.code()) {
      return;
    }
    if (errorCode == ErrorCode.NOT_LEADER.code()
        || errorCode == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION.code()) {
      metadata = null;
    }
    throw new BrokerException(operation, errorCode);
  }

  private void requireOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Client is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      transport.close();
    }
  }
}
