package com.zkafka.broker.storage;

import com.zkafka.common.protocol.RecordCodec;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public final class LogSegment implements AutoCloseable {
  public static final int HEADER_BYTES = 6;
  private static final int MAGIC = 0x5A4B4C47;
  private static final short VERSION = 1;

  public record Info(long baseOffset, Path path) {}

  private final Info info;
  private final FileChannel channel;
  private final int maxRecordBytes;

  private LogSegment(Info info, FileChannel channel, int maxRecordBytes) {
    this.info = info;
    this.channel = channel;
    this.maxRecordBytes = maxRecordBytes;
  }

  public static LogSegment open(Info info, int maxRecordBytes, boolean writable)
      throws IOException {
    FileChannel channel =
        writable
            ? FileChannel.open(info.path(), StandardOpenOption.READ, StandardOpenOption.WRITE)
            : FileChannel.open(info.path(), StandardOpenOption.READ);
    try {
      ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
      FileIO.readFully(channel, header, 0);
      header.flip();
      if (header.getInt() != MAGIC || header.getShort() != VERSION) {
        throw new IOException("Invalid segment header or version: " + info.path());
      }
      return new LogSegment(info, channel, maxRecordBytes);
    } catch (IOException | RuntimeException exception) {
      try {
        channel.close();
      } catch (IOException cleanup) {
        exception.addSuppressed(cleanup);
      }
      throw exception;
    }
  }

  public static List<Info> discover(Path directory) throws IOException {
    List<Info> segments = new ArrayList<>();
    try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, "*.log")) {
      for (Path path : paths) {
        String filename = path.getFileName().toString();
        String stem = filename.substring(0, filename.length() - 4);
        if (!Files.isRegularFile(path) || !stem.matches("[0-9]{20}")) {
          throw new IOException("Invalid segment file: " + path);
        }
        try {
          segments.add(new Info(Long.parseLong(stem), path));
        } catch (NumberFormatException exception) {
          throw new IOException("Segment offset exceeds the supported range: " + path, exception);
        }
      }
    }
    segments.sort(Comparator.comparingLong(Info::baseOffset));
    for (int i = 1; i < segments.size(); i++) {
      if (segments.get(i - 1).baseOffset() == segments.get(i).baseOffset()) {
        throw new IOException("Duplicate segment base offset: " + segments.get(i).baseOffset());
      }
    }
    return segments;
  }

  // The caller owns the partition directory exclusively and holds its write lock.
  public static Info create(Path directory, long baseOffset) throws IOException {
    if (baseOffset < 0) {
      throw new IllegalArgumentException("Negative segment base offset");
    }
    String filename = String.format(Locale.ROOT, "%020d.log", baseOffset);
    Path destination = directory.resolve(filename);
    if (Files.exists(destination)) {
      throw new FileAlreadyExistsException(destination.toString());
    }
    Path temporary = Files.createTempFile(directory, filename + ".", ".tmp");
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(MAGIC).putShort(VERSION).flip();
        FileIO.writeFully(channel, header, 0);
        channel.force(true);
      }
      Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
      return new Info(baseOffset, destination);
    } catch (IOException | RuntimeException exception) {
      try {
        Files.deleteIfExists(temporary);
      } catch (IOException cleanup) {
        exception.addSuppressed(cleanup);
      }
      throw exception;
    }
  }

  public long size() throws IOException {
    return channel.size();
  }

  public void append(ByteBuffer record, long position) throws IOException {
    FileIO.writeFully(channel, record, position);
  }

  public RecordCodec.DecodedRecord read(long position, long fileSize, long expectedOffset)
      throws IOException {
    try {
      if (position < HEADER_BYTES || fileSize - position < RecordCodec.HEADER_BYTES) {
        throw new EOFException("Incomplete record header");
      }
      ByteBuffer bytes = ByteBuffer.allocate(RecordCodec.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
      FileIO.readFully(channel, bytes, position);
      bytes.flip();
      RecordCodec.Header header = RecordCodec.decodeHeader(bytes, expectedOffset, maxRecordBytes);
      if (header.valueLength() > fileSize - position - RecordCodec.HEADER_BYTES) {
        throw new EOFException("Incomplete record value");
      }
      byte[] value = new byte[header.valueLength()];
      FileIO.readFully(channel, ByteBuffer.wrap(value), position + RecordCodec.HEADER_BYTES);
      return RecordCodec.decodeValue(header, value);
    } catch (IOException exception) {
      throw new IOException("Cannot read " + info.path() + " at byte " + position, exception);
    }
  }

  public long recover(long expectedOffset, long maxSegmentBytes, boolean active)
      throws IOException {
    if (info.baseOffset() != expectedOffset) {
      throw new IOException("Nonconsecutive segment offsets: " + info.path());
    }
    long size = size();
    if (size > maxSegmentBytes) {
      throw new IOException("Segment exceeds storage.max.segment.bytes: " + info.path());
    }
    if (size == HEADER_BYTES && !active) {
      throw new IOException("Empty sealed segment: " + info.path());
    }
    long position = HEADER_BYTES;
    while (position < size) {
      RecordCodec.DecodedRecord record = read(position, size, expectedOffset);
      position += RecordCodec.HEADER_BYTES + (long) record.value().length;
      expectedOffset++;
    }
    return expectedOffset;
  }

  public void force() throws IOException {
    channel.force(true);
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
