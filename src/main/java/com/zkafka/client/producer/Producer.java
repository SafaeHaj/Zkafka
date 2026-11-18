package com.zkafka.client.producer;

import com.zkafka.broker.core.ClientValidation;
import com.zkafka.client.ZkafkaClient;
import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Synchronous sends: round-robin automatic routing or an explicit partition. No keys. */
public final class Producer implements AutoCloseable {
  private final ZkafkaClient client;
  private final String topic;
  private long nextPartition;

  public record SendResult(String topic, int partition, long offset) {}

  public Producer(ZkafkaConfig config, String topic) {
    this(new ZkafkaClient(config), topic);
  }

  /** This producer owns the supplied client. */
  public Producer(ZkafkaClient client, String topic) {
    this.topic = ClientValidation.topic(topic);
    this.client = Objects.requireNonNull(client, "client");
  }

  public void initialize() throws IOException {
    client.initialize();
  }

  public SendResult send(String message) throws IOException {
    return send(Objects.requireNonNull(message, "message").getBytes(StandardCharsets.UTF_8));
  }

  public synchronized SendResult send(byte[] message) throws IOException {
    client.validateMessage(topic, message);
    List<Integer> partitions = client.partitions(topic);
    int index = (int) Math.floorMod(nextPartition, (long) partitions.size());
    int partition = partitions.get(index);
    nextPartition = (index + 1L) % partitions.size();
    return sendSelected(partition, message);
  }

  public SendResult send(int partition, String message) throws IOException {
    return send(
        partition, Objects.requireNonNull(message, "message").getBytes(StandardCharsets.UTF_8));
  }

  public synchronized SendResult send(int partition, byte[] message) throws IOException {
    ClientValidation.partition(partition);
    return sendSelected(partition, message);
  }

  private SendResult sendSelected(int partition, byte[] message) throws IOException {
    long offset = client.send(topic, partition, message);
    return new SendResult(topic, partition, offset);
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
