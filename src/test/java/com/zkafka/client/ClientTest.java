package com.zkafka.client;

import com.zkafka.client.consumer.Consumer;
import com.zkafka.client.network.RequestTransport;
import com.zkafka.client.producer.Producer;
import com.zkafka.common.exception.BrokerException;
import com.zkafka.common.exception.ClientProtocolException;
import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.model.Record;
import com.zkafka.common.protocol.Protocol;
import com.zkafka.config.ZkafkaConfig;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class ClientTest {
  private static int checks;
  private static ZkafkaConfig config;

  public static void main(String[] args) throws Exception {
    config = settings(true);
    producerRouting();
    errorsAndValidation();
    consumerOffsets();
    backgroundDelivery();
    immutableRecords();
    System.out.println("Client checks passed: " + checks);
  }

  static ZkafkaConfig settings(boolean polling) throws IOException {
    Properties properties = new Properties();
    try (Reader reader =
        Files.newBufferedReader(Path.of("config/zkafka.properties"), StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    properties.setProperty("consumer.poll.interval.ms", polling ? "1" : "");
    properties.setProperty("consumer.shutdown.timeout.ms", "2000");
    return ZkafkaConfig.fromProperties(properties);
  }

  private static void producerRouting() throws Exception {
    FakeTransport transport = new FakeTransport();
    try (Producer producer = new Producer(new ZkafkaClient(config, transport), "events")) {
      Producer.SendResult first = producer.send("one");
      check(
          first.partition() == 2 && first.offset() == 0 && first.topic().equals("events"),
          "Receipt carries destination");
      producer.send("two");
      producer.send("three");
      producer.send(5, "explicit");
      producer.send("four");
      check(
          transport.sent.equals(List.of(2, 5, 9, 5, 2)),
          "Round robin over actual IDs; explicit does not advance it");
      check(transport.metadataRequests == 1, "Metadata reused");
      check(
          transport.lastHost.equals("leader") && transport.lastPort == 29092,
          "Requests route to leader address");
      int before = transport.produces;
      rejectsArgument(() -> producer.send(new byte[config.storage().maxRecordBytes() + 1]));
      check(transport.produces == before, "Oversized payload never sent");
      rejectsArgument(() -> producer.send(-1, "bad"));
      check(
          producer.send("after-validation-error").partition() == 5,
          "Local validation does not advance round robin");
    }
    check(transport.closed, "Producer owns and closes client/transport");
  }

  private static void errorsAndValidation() throws Exception {
    FakeTransport transport = new FakeTransport();
    try (ZkafkaClient client = new ZkafkaClient(config, transport)) {
      transport.produceError = ErrorCode.NOT_LEADER.code();
      BrokerException error =
          rejects(BrokerException.class, () -> client.send("events", 2, new byte[0]));
      check(
          error.knownError().orElseThrow() == ErrorCode.NOT_LEADER && transport.produces == 1,
          "Routing error exposed without resend");
      transport.produceError = 0;
      check(
          client.send("events", 2, new byte[0]) == 1 && transport.metadataRequests == 2,
          "Next operation refreshes stale routing");
      transport.produceError = 65535;
      error = rejects(BrokerException.class, () -> client.send("events", 2, new byte[0]));
      check(
          error.errorCode() == 65535 && error.knownError().isEmpty(),
          "Unknown broker code preserved");
      transport.produceError = 0;
      transport.wrongTarget = true;
      rejects(ClientProtocolException.class, () -> client.send("events", 2, new byte[0]));
      transport.wrongTarget = false;
      transport.failProduce = true;
      int before = transport.produces;
      rejects(IOException.class, () -> client.send("events", 2, new byte[0]));
      check(transport.produces == before + 1, "Uncertain transport outcome is not retried");
      transport.failProduce = false;
      transport.records = List.of(new Record(4, new byte[] {1}), new Record(4, new byte[] {2}));
      rejects(ClientProtocolException.class, () -> client.fetch("events", 2, 0, 100));
      transport.records = List.of(new Record(3, new byte[] {1}));
      rejects(ClientProtocolException.class, () -> client.fetch("events", 2, 4, 100));
      transport.records = List.of(new Record(4, new byte[] {1}));
      rejects(ClientProtocolException.class, () -> client.fetch("events", 2, 0, 12));
      transport.wrongTarget = true;
      rejects(ClientProtocolException.class, () -> client.fetch("events", 2, 0, 100));
      transport.wrongTarget = false;
      transport.records = List.of();
      check(
          client.fetch("events", 2, Long.MAX_VALUE, 100).isEmpty(),
          "Exclusive end offset is representable");
      rejects(IOException.class, () -> client.send("events", 6, new byte[0]));
      client.close();
      rejects(IOException.class, client::initialize);
    }

    FakeTransport metadataFailure = new FakeTransport();
    metadataFailure.metadataError = 8;
    try (ZkafkaClient client = new ZkafkaClient(config, metadataFailure)) {
      rejects(BrokerException.class, client::initialize);
      check(metadataFailure.produces == 0, "Metadata failure prevents produce");
    }
  }

  private static void consumerOffsets() throws Exception {
    FakeTransport transport = new FakeTransport();
    transport.records = List.of(new Record(4, new byte[] {1}), new Record(8, new byte[] {2}));
    try (Consumer consumer =
        new Consumer(config, new ZkafkaClient(config, transport), "events", 2)) {
      List<Record> records = consumer.poll();
      check(
          records.size() == 2 && consumer.getCurrentOffset() == 9,
          "Position uses returned offsets, not record count");
      transport.records = List.of();
      check(
          consumer.poll().isEmpty() && transport.lastFetchOffset == 9,
          "Empty poll retains position");
      consumer.seek(4);
      check(consumer.getCurrentOffset() == 4, "Explicit seek");
      rejectsArgument(() -> consumer.seek(-1));
      consumer.close();
      rejects(IllegalStateException.class, consumer::poll);
    }
    ZkafkaConfig undecided = settings(false);
    try (Consumer consumer =
        new Consumer(undecided, new ZkafkaClient(undecided, new FakeTransport()), "events", 2)) {
      rejects(IllegalStateException.class, () -> consumer.startConsuming(record -> {}));
    }
  }

  private static void backgroundDelivery() throws Exception {
    FakeTransport transport = new FakeTransport();
    transport.records = List.of(new Record(0, new byte[] {1}), new Record(1, new byte[] {2}));
    CountDownLatch delivered = new CountDownLatch(2);
    try (Consumer consumer =
        new Consumer(config, new ZkafkaClient(config, transport), "events", 2)) {
      consumer.startConsuming(
          record -> {
            if (record.offset() == 1) {
              delivered.countDown();
              throw new IOException("Handler rejected record 1");
            }
            delivered.countDown();
          });
      check(delivered.await(2, TimeUnit.SECONDS), "Background handler invoked");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (consumer.lastFailure().isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(1);
      }
      consumer.stopConsuming();
      check(
          consumer.lastFailure().isPresent() && consumer.getCurrentOffset() == 1,
          "Failed handler does not skip its record");
      transport.records = List.of(new Record(1, new byte[] {2}));
      CountDownLatch replayed = new CountDownLatch(1);
      consumer.startConsuming(
          record -> {
            consumer.stopConsuming(); // Self-stop must not self-join.
            replayed.countDown();
          });
      check(replayed.await(2, TimeUnit.SECONDS), "Restart delivers failed record again");
      consumer.stopConsuming();
      check(
          consumer.getCurrentOffset() == 2 && consumer.lastFailure().isEmpty(),
          "Successful replay advances offset");
    }

    FakeTransport paused = new FakeTransport();
    CountDownLatch inHandler = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    paused.records = List.of(new Record(0, new byte[0]));
    try (Consumer consumer = new Consumer(config, new ZkafkaClient(config, paused), "events", 2)) {
      consumer.startConsuming(
          record -> {
            inHandler.countDown();
            release.await();
          });
      check(inHandler.await(2, TimeUnit.SECONDS), "Handler active");
      rejects(IllegalStateException.class, consumer::poll);
      rejects(IllegalStateException.class, () -> consumer.seek(0));
      release.countDown();
      consumer.stopConsuming();
    } finally {
      release.countDown();
    }
  }

  private static void immutableRecords() {
    byte[] bytes = {1};
    Record record = new Record(0, bytes);
    bytes[0] = 2;
    byte[] returned = record.value();
    returned[0] = 3;
    check(
        record.value()[0] == 1 && record.equals(new Record(0, new byte[] {1})),
        "Record payload ownership and value equality");
  }

  private static final class FakeTransport implements RequestTransport {
    private int metadataRequests;
    private int produces;
    private int produceError;
    private int metadataError;
    private boolean wrongTarget;
    private boolean failProduce;
    private boolean closed;
    private String lastHost;
    private int lastPort;
    private long lastFetchOffset;
    private final List<Integer> sent = new ArrayList<>();
    private volatile List<Record> records = List.of();

    @Override
    public synchronized ByteBuffer exchange(String host, int port, ByteBuffer request)
        throws IOException {
      if (closed) {
        throw new IOException("Transport closed");
      }
      ByteBuffer body = request.duplicate();
      byte type = body.get();
      if (type == Protocol.METADATA) {
        metadataRequests++;
        return metadata(metadataError);
      }
      lastHost = host;
      lastPort = port;
      String topic = string(body);
      int partition = body.getInt();
      if (type == Protocol.PRODUCE) {
        sent.add(partition);
        long offset = produces++;
        if (failProduce) {
          throw new IOException("Connection lost after request write");
        }
        return produce(topic, wrongTarget ? partition + 1 : partition, produceError, offset);
      }
      if (type == Protocol.FETCH) {
        lastFetchOffset = body.getLong();
        return fetch(topic, wrongTarget ? partition + 1 : partition, records);
      }
      throw new AssertionError("Unexpected request type");
    }

    @Override
    public synchronized void close() {
      closed = true;
    }
  }

  static ByteBuffer metadata(int error) throws IOException {
    return wire(
        out -> {
          out.writeByte(0x13);
          out.writeShort(error);
          out.writeInt(error == 0 ? 1 : 0);
          if (error == 0) {
            out.writeInt(7);
            string(out, "leader");
            out.writeInt(29092);
          }
          out.writeInt(error == 0 ? 1 : 0);
          if (error == 0) {
            string(out, "events");
            out.writeInt(3);
            for (int partition : new int[] {9, 2, 5}) {
              out.writeInt(partition);
              out.writeInt(7);
              out.writeInt(1);
              out.writeInt(7);
            }
          }
        });
  }

  static ByteBuffer produce(String topic, int partition, int error, long offset)
      throws IOException {
    return wire(
        out -> {
          out.writeByte(0x11);
          string(out, topic);
          out.writeInt(partition);
          out.writeLong(error == 0 ? offset : -1);
          out.writeShort(error);
        });
  }

  private static ByteBuffer fetch(String topic, int partition, List<Record> records)
      throws IOException {
    return wire(
        out -> {
          out.writeByte(0x12);
          string(out, topic);
          out.writeInt(partition);
          out.writeShort(0);
          out.writeInt(records.size());
          for (Record record : records) {
            out.writeLong(record.offset());
            byte[] value = record.value();
            out.writeInt(value.length);
            out.write(value);
          }
        });
  }

  private static String string(ByteBuffer buffer) {
    byte[] value = new byte[Short.toUnsignedInt(buffer.getShort())];
    buffer.get(value);
    return new String(value, StandardCharsets.UTF_8);
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeShort(bytes.length);
    out.write(bytes);
  }

  private static ByteBuffer wire(Writer writer) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    writer.write(out);
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  private static void rejectsArgument(Action action) throws Exception {
    rejects(IllegalArgumentException.class, action);
  }

  private static <T extends Exception> T rejects(Class<T> type, Action action) throws Exception {
    try {
      action.run();
    } catch (Exception exception) {
      if (type.isInstance(exception)) {
        checks++;
        return type.cast(exception);
      }
      throw exception;
    }
    throw new AssertionError("Expected " + type.getSimpleName());
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
    checks++;
  }

  @FunctionalInterface
  private interface Action {
    void run() throws Exception;
  }

  @FunctionalInterface
  private interface Writer {
    void write(DataOutputStream out) throws IOException;
  }
}
