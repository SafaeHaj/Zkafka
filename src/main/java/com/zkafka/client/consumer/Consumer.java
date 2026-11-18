package com.zkafka.client.consumer;

import com.zkafka.broker.core.ClientValidation;
import com.zkafka.client.ZkafkaClient;
import com.zkafka.common.model.Record;
import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** One topic/partition; manual polling and background delivery are mutually exclusive. */
public final class Consumer implements AutoCloseable {
  private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
  private final ZkafkaConfig.ConsumerSettings settings;
  private final ZkafkaClient client;
  private final String topic;
  private final int partition;
  private final Object operationLock = new Object();
  private final Object lifecycleLock = new Object();
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile Thread consumerThread;
  private volatile long currentOffset;
  private volatile Exception lastFailure;

  public Consumer(ZkafkaConfig config, String topic, int partition) {
    this(config, new ZkafkaClient(config), topic, partition);
  }

  /** This consumer owns the supplied client. */
  public Consumer(ZkafkaConfig config, ZkafkaClient client, String topic, int partition) {
    settings = Objects.requireNonNull(config, "config").consumer();
    this.client = Objects.requireNonNull(client, "client");
    this.topic = ClientValidation.topic(topic);
    ClientValidation.partition(partition);
    this.partition = partition;
  }

  public void initialize() throws IOException {
    synchronized (operationLock) {
      requireManualOperation();
      client.initialize();
    }
  }

  /** Advances the local fetch position, not a committed consumer-group offset. */
  public List<Record> poll() throws IOException {
    synchronized (operationLock) {
      requireManualOperation();
      List<Record> records = fetchBatch();
      if (!records.isEmpty()) {
        currentOffset = Math.incrementExact(records.getLast().offset());
      }
      return records;
    }
  }

  public void startConsuming(MessageHandler handler) {
    Objects.requireNonNull(handler, "handler");
    synchronized (operationLock) {
      synchronized (lifecycleLock) {
        requireOpen();
        if (consumerThread != null) {
          throw new IllegalStateException("Background consumer is already active");
        }
        int pollInterval = settings.requiredPollIntervalMillis();
        lastFailure = null;
        running.set(true);
        Thread worker =
            new Thread(
                () -> consume(handler, pollInterval), "zkafka-consumer-" + topic + "-" + partition);
        worker.setDaemon(false);
        consumerThread = worker;
        try {
          worker.start();
        } catch (RuntimeException | Error failure) {
          consumerThread = null;
          running.set(false);
          throw failure;
        }
      }
    }
  }

  private void consume(MessageHandler handler, int pollInterval) {
    try {
      while (running.get() && !closed.get()) {
        List<Record> records = fetchBatch();
        for (Record record : records) {
          if (!running.get() || closed.get()) {
            break;
          }
          handler.handle(record);
          currentOffset = Math.incrementExact(record.offset());
        }
        if (records.isEmpty() && running.get() && !closed.get()) {
          Thread.sleep(pollInterval);
        }
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      reportFailure(exception);
    } catch (Exception exception) {
      reportFailure(exception);
    } finally {
      running.set(false);
      synchronized (lifecycleLock) {
        if (consumerThread == Thread.currentThread()) {
          consumerThread = null;
        }
      }
    }
  }

  private void reportFailure(Exception exception) {
    if (running.get() && !closed.get()) {
      lastFailure = exception;
      LOGGER.log(Level.WARNING, "Consumer stopped at offset " + currentOffset, exception);
    }
  }

  private List<Record> fetchBatch() throws IOException {
    return client.fetch(topic, partition, currentOffset, client.maxFetchBytes(topic));
  }

  public void stopConsuming() throws IOException {
    awaitWorker(requestStop());
  }

  private Thread requestStop() {
    synchronized (lifecycleLock) {
      running.set(false);
      Thread worker = consumerThread;
      if (worker != null) {
        worker.interrupt();
      }
      return worker;
    }
  }

  private void awaitWorker(Thread worker) throws IOException {
    if (worker == null || worker == Thread.currentThread()) {
      return;
    }
    try {
      worker.join(settings.shutdownTimeoutMillis());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      InterruptedIOException failure = new InterruptedIOException("Consumer shutdown interrupted");
      failure.initCause(exception);
      throw failure;
    }
    if (worker.isAlive()) {
      throw new IOException("Consumer did not stop within consumer.shutdown.timeout.ms");
    }
  }

  public void seek(long offset) {
    ClientValidation.offset(offset);
    synchronized (operationLock) {
      requireManualOperation();
      currentOffset = offset;
    }
  }

  public long getCurrentOffset() {
    return currentOffset;
  }

  public Optional<Exception> lastFailure() {
    return Optional.ofNullable(lastFailure);
  }

  private void requireManualOperation() {
    requireOpen();
    if (consumerThread != null) {
      throw new IllegalStateException(
          "Manual polling/seek is unavailable during background consumption");
    }
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Consumer is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Thread worker = requestStop();
    IOException failure = null;
    try {
      client.close();
    } catch (IOException exception) {
      failure = exception;
    }
    try {
      awaitWorker(worker);
    } catch (IOException exception) {
      if (failure == null) {
        failure = exception;
      } else {
        failure.addSuppressed(exception);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @FunctionalInterface
  public interface MessageHandler {
    void handle(Record record) throws Exception;
  }
}
