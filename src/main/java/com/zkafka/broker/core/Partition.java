package com.zkafka.broker.core;

import com.zkafka.broker.storage.LogSegment;
import com.zkafka.common.protocol.RecordCodec;
import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Coordinates one local partition's offsets, ordered segments, and lifecycle. */
public final class Partition implements AutoCloseable {
  private final int id;
  private final Path directory;
  private final ZkafkaConfig.Storage storageConfig;
  private final List<LogSegment.Info> segments = new ArrayList<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final AtomicBoolean failed = new AtomicBoolean();

  private long nextOffset;
  private LogSegment activeSegment;
  private boolean closed;

  public record StoredMessage(long offset, byte[] value) {}

  public Partition(String topic, int id, ZkafkaConfig config) throws IOException {
    this(id, partitionDirectory(config.storage().directory(), topic, id), config.storage());
  }

  public Partition(int id, Path directory, ZkafkaConfig.Storage storageConfig) throws IOException {
    if (id < 0) {
      throw new IllegalArgumentException("Partition ID must be nonnegative");
    }
    this.id = id;
    this.directory = Objects.requireNonNull(directory, "Partition directory must not be null");
    this.storageConfig = Objects.requireNonNull(storageConfig, "storageConfig");
    initialize();
  }

  private static Path partitionDirectory(Path root, String topic, int id) {
    if (!isValidTopic(topic)) {
      throw new IllegalArgumentException("Topic must be a single directory name");
    }
    return root.toAbsolutePath().normalize().resolve(topic).resolve(Integer.toString(id));
  }

  public static boolean isValidTopic(String topic) {
    if (topic == null
        || topic.isBlank()
        || topic.equals(".")
        || topic.equals("..")
        || topic.contains("/")
        || topic.contains("\\")) {
      return false;
    }
    try {
      Path path = Path.of(topic);
      return path.getRoot() == null && path.getNameCount() == 1;
    } catch (java.nio.file.InvalidPathException exception) {
      return false;
    }
  }

  private void initialize() throws IOException {
    Files.createDirectories(directory);
    segments.addAll(LogSegment.discover(directory));
    if (segments.isEmpty()) {
      segments.add(LogSegment.create(directory, 0));
      nextOffset = 0;
    } else {
      nextOffset = recoverNextOffset();
    }
    activeSegment = LogSegment.open(segments.getLast(), storageConfig.maxRecordBytes(), true);
  }

  // Without an authoritative durable/committed boundary, damaged files are not truncated.
  private long recoverNextOffset() throws IOException {
    long expectedOffset = segments.getFirst().baseOffset();
    for (int i = 0; i < segments.size(); i++) {
      try (LogSegment segment =
          LogSegment.open(segments.get(i), storageConfig.maxRecordBytes(), false)) {
        expectedOffset =
            segment.recover(
                expectedOffset, storageConfig.maxSegmentBytes(), i == segments.size() - 1);
      }
    }
    return expectedOffset;
  }

  public long append(byte[] message) throws IOException {
    Objects.requireNonNull(message, "message");
    if (message.length > storageConfig.maxRecordBytes()) {
      throw new IllegalArgumentException("Message exceeds storage.max.record.bytes");
    }
    lock.writeLock().lock();
    try {
      requireUsable();
      long offset = nextOffset;
      long followingOffset = Math.incrementExact(offset);
      ByteBuffer record = RecordCodec.encode(offset, message);
      try {
        long fileSize = activeSegment.size();
        if (fileSize < LogSegment.HEADER_BYTES || fileSize > storageConfig.maxSegmentBytes()) {
          throw new IOException("Invalid active segment size in " + directory);
        }
        if (record.remaining() > storageConfig.maxSegmentBytes() - fileSize) {
          rollSegment(offset);
          fileSize = activeSegment.size();
        }
        activeSegment.append(record, fileSize);
        nextOffset = followingOffset;
        return offset;
      } catch (IOException exception) {
        failed.set(true);
        throw exception;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** maxBytes counts offset (8), length (4), and value bytes; the read limit is exclusive. */
  public List<StoredMessage> readMessages(long offset, int maxBytes, long readLimitExclusive)
      throws IOException {
    if (offset < 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Invalid offset or byte budget");
    }
    lock.readLock().lock();
    try {
      requireUsable();
      long firstOffset = segments.getFirst().baseOffset();
      if (readLimitExclusive < firstOffset
          || readLimitExclusive > nextOffset
          || offset < firstOffset
          || offset > readLimitExclusive) {
        throw new IllegalArgumentException("Read range is outside the log");
      }
      if (offset == readLimitExclusive) {
        return List.of();
      }

      List<StoredMessage> messages = new ArrayList<>();
      int remainingBytes = maxBytes;
      int firstSegment = findSegmentIndex(offset);
      long expectedOffset = segments.get(firstSegment).baseOffset();
      for (int i = firstSegment; i < segments.size() && expectedOffset < readLimitExclusive; i++) {
        LogSegment.Info info = segments.get(i);
        if (info.baseOffset() != expectedOffset) {
          throw new IOException("Nonconsecutive segment offsets: " + info.path());
        }
        try (LogSegment segment = LogSegment.open(info, storageConfig.maxRecordBytes(), false)) {
          long fileSize = segment.size();
          long position = LogSegment.HEADER_BYTES;
          while (position < fileSize && expectedOffset < readLimitExclusive) {
            RecordCodec.DecodedRecord record = segment.read(position, fileSize, expectedOffset);
            if (record.offset() >= offset) {
              long cost = 12L + record.value().length;
              if (cost > remainingBytes) {
                if (messages.isEmpty()) {
                  throw new IllegalArgumentException("Byte budget cannot fit the next record");
                }
                return List.copyOf(messages);
              }
              messages.add(new StoredMessage(record.offset(), record.value()));
              remainingBytes -= (int) cost;
            }
            position += RecordCodec.HEADER_BYTES + (long) record.value().length;
            expectedOffset++;
          }
        }
      }
      if (expectedOffset < readLimitExclusive) {
        throw new IOException("Log ended before the expected offset in " + directory);
      }
      return List.copyOf(messages);
    } catch (IOException exception) {
      failed.set(true);
      throw exception;
    } finally {
      lock.readLock().unlock();
    }
  }

  private int findSegmentIndex(long offset) {
    int low = 0;
    int high = segments.size() - 1;
    int result = -1;
    while (low <= high) {
      int middle = low + (high - low) / 2;
      if (segments.get(middle).baseOffset() <= offset) {
        result = middle;
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    return result;
  }

  // The caller holds the partition write lock.
  private void rollSegment(long baseOffset) throws IOException {
    activeSegment.force();
    LogSegment.Info nextInfo = LogSegment.create(directory, baseOffset);
    LogSegment next = LogSegment.open(nextInfo, storageConfig.maxRecordBytes(), true);
    try {
      activeSegment.close();
    } catch (IOException exception) {
      try {
        next.close();
      } catch (IOException cleanup) {
        exception.addSuppressed(cleanup);
      }
      throw exception;
    }
    segments.add(nextInfo);
    activeSegment = next;
  }

  public long nextOffset() throws IOException {
    lock.readLock().lock();
    try {
      requireUsable();
      return nextOffset;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void flush() throws IOException {
    lock.writeLock().lock();
    try {
      requireUsable();
      try {
        activeSegment.force();
      } catch (IOException exception) {
        failed.set(true);
        throw exception;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    lock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      IOException failure = null;
      try {
        if (!failed.get()) {
          activeSegment.force();
        }
      } catch (IOException exception) {
        failure = exception;
      }
      try {
        activeSegment.close();
      } catch (IOException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
      if (failure != null) {
        failed.set(true);
        throw failure;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void requireUsable() throws IOException {
    if (closed) {
      throw new IllegalStateException("Partition is closed");
    }
    if (failed.get()) {
      throw new IOException("Partition is unavailable: " + directory);
    }
  }
}
