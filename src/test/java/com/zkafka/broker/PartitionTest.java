package com.zkafka.broker;

import com.zkafka.broker.core.Partition;
import com.zkafka.config.ZkafkaConfig;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.zip.CRC32C;

/** Standalone storage regression tests using independent on-disk fixtures. */
public final class PartitionTest {
  private static int checks;
  private static Path root;

  public static void main(String[] args) throws Exception {
    Files.createDirectories(Path.of("target"));
    root = Files.createTempDirectory(Path.of("target"), "partition-tests-");
    appendReadAndRestart();
    rollingAndReadLimits();
    validFixtures();
    rejectDamagedFixtures();
    readFailureDisablesWrites();
    publicationFailurePreservesFiles();
    concurrentAppends();
    System.out.println("Partition checks passed: " + checks);
  }

  private static ZkafkaConfig.Storage settings(Path path, int maxRecord, long segmentSize) {
    return new ZkafkaConfig.Storage(
        path, maxRecord, segmentSize, 1000, ZkafkaConfig.RecoveryMode.TAIL_TRUNCATE);
  }

  private static Path directory() throws IOException {
    return Files.createTempDirectory(root, "partition-");
  }

  private static Path segment(Path directory, long offset) {
    return directory.resolve(String.format(Locale.ROOT, "%020d.log", offset));
  }

  private static void appendReadAndRestart() throws Exception {
    Path directory = directory();
    ZkafkaConfig.Storage config = settings(directory, 64, 256);
    try (Partition partition = new Partition(0, directory, config)) {
      check(partition.readMessages(0, 12, 0).isEmpty(), "Empty log");
      check(partition.append(new byte[0]) == 0, "Empty value offset");
      byte[] payload = {1, 2, 3};
      check(partition.append(payload) == 1, "Second offset");
      payload[0] = 9;
      List<Partition.StoredMessage> result = partition.readMessages(0, 27, 2);
      check(result.size() == 2 && result.get(0).value().length == 0, "Empty value retained");
      check(
          result.get(1).offset() == 1 && Arrays.equals(result.get(1).value(), new byte[] {1, 2, 3}),
          "Stored data independent of caller array");
      check(partition.readMessages(0, 12, 2).size() == 1, "Exact budget");
      check(partition.readMessages(1, 15, 2).size() == 1, "Read from interior offset");
      check(partition.readMessages(1, 12, 1).isEmpty(), "Exclusive read limit");
      rejectsArgument(() -> partition.readMessages(1, 14, 2));
      rejectsArgument(() -> partition.readMessages(-1, 32, 2));
      rejectsArgument(() -> partition.readMessages(0, 0, 2));
      rejectsArgument(() -> partition.readMessages(0, 32, 3));
      rejectsArgument(() -> partition.append(new byte[65]));
      check(partition.append(new byte[64]) == 2, "Maximum value and no offset lost on rejection");
      partition.flush();
      check(
          Arrays.equals(
              Files.readAllBytes(segment(directory, 0)),
              fixture(new long[] {0, 1, 2}, new byte[][] {new byte[0], {1, 2, 3}, new byte[64]})),
          "Independent binary fixture");
    }
    try (Partition reopened = new Partition(0, directory, config)) {
      check(reopened.readMessages(0, 1000, 3).size() == 3, "Recovery reads existing data");
      check(reopened.append(new byte[] {4}) == 3, "Recovery restores next offset");
      reopened.close();
      reopened.close();
      rejectsState(() -> reopened.append(new byte[0]));
      rejectsState(() -> reopened.readMessages(0, 100, 0));
      rejectsState(reopened::flush);
    }
  }

  private static void rollingAndReadLimits() throws Exception {
    Path directory = directory();
    ZkafkaConfig.Storage config = settings(directory, 8, 30);
    try (Partition partition = new Partition(0, directory, config)) {
      for (int i = 0; i < 4; i++) {
        byte[] value = new byte[8];
        Arrays.fill(value, (byte) i);
        check(partition.append(value) == i, "Offset across roll");
      }
      check(Files.size(segment(directory, 0)) == 30, "Exact segment fit");
      for (int i = 0; i < 4; i++) {
        check(Files.exists(segment(directory, i)), "Padded segment name");
      }
      List<Partition.StoredMessage> records = partition.readMessages(1, 40, 4);
      check(
          records.size() == 2 && records.get(0).offset() == 1 && records.get(1).offset() == 2,
          "Cross-segment read budget");
      check(partition.readMessages(0, 100, 2).size() == 2, "Uncommitted suffix not exposed");
    }
    try (Partition reopened = new Partition(0, directory, config)) {
      check(reopened.append(new byte[8]) == 4, "Restart after rolling");
      check(reopened.readMessages(0, 100, 5).size() == 5, "Read across recovered segments");
    }
  }

  private static void validFixtures() throws Exception {
    Path directory = directory();
    Files.write(segment(directory, 0), fixture(new long[] {0}, new byte[][] {{7}}));
    Files.write(segment(directory, 1), fixture(new long[0], new byte[0][]));
    try (Partition partition = new Partition(0, directory, settings(directory, 8, 64))) {
      check(partition.append(new byte[] {8}) == 1, "Empty active segment recovers base offset");
      check(partition.readMessages(0, 100, 2).size() == 2, "Independent fixture decoded");
    }
  }

  private static void rejectDamagedFixtures() throws Exception {
    byte[] valid = fixture(new long[] {0}, new byte[][] {{1, 2, 3}});
    byte[] badMagic = valid.clone();
    badMagic[0] = 0;
    damaged(badMagic);
    byte[] badVersion = valid.clone();
    badVersion[5] = 2;
    damaged(badVersion);
    byte[] badChecksum = valid.clone();
    badChecksum[badChecksum.length - 1] ^= 1;
    damaged(badChecksum);
    for (int length = 0; length < valid.length; length++) {
      // A valid header by itself is an empty segment, not corruption.
      if (length != 6) {
        damaged(Arrays.copyOf(valid, length));
      }
    }
    byte[] negativeLength = valid.clone();
    ByteBuffer.wrap(negativeLength).putInt(14, -1);
    damaged(negativeLength);
    byte[] hugeLength = valid.clone();
    ByteBuffer.wrap(hugeLength).putInt(14, Integer.MAX_VALUE);
    damaged(hugeLength);
    byte[] wrongOffset = valid.clone();
    ByteBuffer.wrap(wrongOffset).putLong(6, 1);
    damaged(wrongOffset);

    Path gap = directory();
    Files.write(segment(gap, 0), valid);
    Files.write(segment(gap, 2), fixture(new long[] {2}, new byte[][] {{4}}));
    rejectsIo(() -> new Partition(0, gap, settings(gap, 8, 64)));
    Path emptySealed = directory();
    Files.write(segment(emptySealed, 0), fixture(new long[0], new byte[0][]));
    Files.write(segment(emptySealed, 1), fixture(new long[] {1}, new byte[][] {{4}}));
    rejectsIo(() -> new Partition(0, emptySealed, settings(emptySealed, 8, 64)));
    Path unpadded = directory();
    Files.write(unpadded.resolve("0.log"), valid);
    rejectsIo(() -> new Partition(0, unpadded, settings(unpadded, 8, 64)));
    Path overflow = directory();
    Files.write(overflow.resolve("99999999999999999999.log"), valid);
    rejectsIo(() -> new Partition(0, overflow, settings(overflow, 8, 64)));
  }

  private static void damaged(byte[] bytes) throws Exception {
    Path directory = directory();
    Path path = segment(directory, 0);
    Files.write(path, bytes);
    rejectsIo(() -> new Partition(0, directory, settings(directory, 8, 64)));
    check(
        Arrays.equals(Files.readAllBytes(path), bytes),
        "Recovery preserves uncertain damaged data");
  }

  private static void readFailureDisablesWrites() throws Exception {
    Path directory = directory();
    try (Partition partition = new Partition(0, directory, settings(directory, 8, 64))) {
      partition.append(new byte[] {1});
      try (var channel =
          java.nio.channels.FileChannel.open(segment(directory, 0), StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(new byte[] {2}), 22);
      }
      rejectsIo(() -> partition.readMessages(0, 100, 1));
      rejectsIo(() -> partition.append(new byte[] {3}));
    }
  }

  private static void concurrentAppends() throws Exception {
    Path directory = directory();
    try (Partition partition = new Partition(0, directory, settings(directory, 8, 128));
        var workers = Executors.newFixedThreadPool(4)) {
      List<Callable<Long>> tasks = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        tasks.add(() -> partition.append(new byte[] {9}));
      }
      List<Long> offsets = new ArrayList<>();
      for (var result : workers.invokeAll(tasks)) {
        offsets.add(result.get());
      }
      offsets.sort(Long::compare);
      for (int i = 0; i < offsets.size(); i++) {
        check(offsets.get(i) == i, "Concurrent append offsets are unique and consecutive");
      }
      check(partition.readMessages(0, 1000, 40).size() == 40, "All concurrent records readable");
    }
  }

  private static void publicationFailurePreservesFiles() throws Exception {
    Path directory = directory();
    try (Partition partition = new Partition(0, directory, settings(directory, 8, 30))) {
      partition.append(new byte[8]);
      byte[] original = Files.readAllBytes(segment(directory, 0));
      byte[] occupied = {9, 8, 7};
      Files.write(segment(directory, 1), occupied);
      rejectsIo(() -> partition.append(new byte[8]));
      check(
          Arrays.equals(Files.readAllBytes(segment(directory, 1)), occupied),
          "Existing destination not overwritten");
      check(
          Arrays.equals(Files.readAllBytes(segment(directory, 0)), original),
          "Old segment preserved on roll failure");
      rejectsIo(() -> partition.append(new byte[0]));
    }
  }

  private static byte[] fixture(long[] offsets, byte[][] values) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(0x5A4B4C47);
    out.writeShort(1);
    for (int i = 0; i < offsets.length; i++) {
      ByteArrayOutputStream covered = new ByteArrayOutputStream();
      DataOutputStream fields = new DataOutputStream(covered);
      fields.writeLong(offsets[i]);
      fields.writeInt(values[i].length);
      fields.write(values[i]);
      CRC32C crc = new CRC32C();
      crc.update(covered.toByteArray());
      out.writeLong(offsets[i]);
      out.writeInt(values[i].length);
      out.writeInt((int) crc.getValue());
      out.write(values[i]);
    }
    return bytes.toByteArray();
  }

  private static void rejectsIo(Action action) throws Exception {
    try {
      action.run();
    } catch (IOException expected) {
      checks++;
      return;
    }
    throw new AssertionError("Expected IOException");
  }

  private static void rejectsArgument(Action action) throws Exception {
    try {
      action.run();
    } catch (IllegalArgumentException expected) {
      checks++;
      return;
    }
    throw new AssertionError("Expected IllegalArgumentException");
  }

  private static void rejectsState(Action action) throws Exception {
    try {
      action.run();
    } catch (IllegalStateException expected) {
      checks++;
      return;
    }
    throw new AssertionError("Expected IllegalStateException");
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
}
