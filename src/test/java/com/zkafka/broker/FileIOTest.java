package com.zkafka.broker;

import com.zkafka.broker.storage.FileIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Exercises short I/O, progress failure, and positional behavior through a capped real channel. */
public final class FileIOTest {
  private static int checks;

  public static void main(String[] args) throws Exception {
    Files.createDirectories(Path.of("target"));
    Path path = Files.createTempFile(Path.of("target"), "file-io-", ".bin");
    try {
      try (FileChannel real =
              FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
          CappedChannel channel = new CappedChannel(real, 2)) {
        real.position(1);
        byte[] value = {1, 2, 3, 4, 5};
        FileIO.writeFully(channel, ByteBuffer.wrap(value), 4);
        check(channel.writes == 3, "Complete short writes");
        check(real.position() == 1, "Positional writes preserve shared cursor");
        ByteBuffer read = ByteBuffer.allocate(7);
        read.position(2);
        FileIO.readFully(channel, read, 4);
        check(
            channel.reads == 3 && read.position() == 7,
            "Complete short reads with nonzero buffer position");
        check(
            Arrays.equals(Arrays.copyOfRange(read.array(), 2, 7), value),
            "Read bytes match writes");
        check(real.position() == 1, "Positional reads preserve shared cursor");
        rejectsIo(() -> FileIO.readFully(channel, ByteBuffer.allocate(1), 9));
        channel.stall = true;
        rejectsIo(() -> FileIO.readFully(channel, ByteBuffer.allocate(1), 4));
        rejectsIo(() -> FileIO.writeFully(channel, ByteBuffer.allocate(1), 4));
        rejectsArgument(() -> FileIO.readFully(channel, ByteBuffer.allocate(1), -1));
        rejectsArgument(() -> FileIO.writeFully(channel, ByteBuffer.allocate(2), Long.MAX_VALUE));
        FileIO.readFully(channel, ByteBuffer.allocate(0), 0);
        FileIO.writeFully(channel, ByteBuffer.allocate(0), 0);
        checks++;
      }
    } finally {
      Files.deleteIfExists(path);
    }
    System.out.println("File I/O checks passed: " + checks);
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

  private static final class CappedChannel extends FileChannel {
    private final FileChannel delegate;
    private final int chunk;
    private int reads;
    private int writes;
    private boolean stall;

    private CappedChannel(FileChannel delegate, int chunk) {
      this.delegate = delegate;
      this.chunk = chunk;
    }

    @Override
    public int read(ByteBuffer destination, long position) throws IOException {
      reads++;
      if (stall) {
        return 0;
      }
      int limit = destination.limit();
      try {
        destination.limit(destination.position() + Math.min(chunk, destination.remaining()));
        return delegate.read(destination, position);
      } finally {
        destination.limit(limit);
      }
    }

    @Override
    public int write(ByteBuffer source, long position) throws IOException {
      writes++;
      if (stall) {
        return 0;
      }
      int limit = source.limit();
      try {
        source.limit(source.position() + Math.min(chunk, source.remaining()));
        return delegate.write(source, position);
      } finally {
        source.limit(limit);
      }
    }

    @Override
    protected void implCloseChannel() throws IOException {
      delegate.close();
    }

    @Override
    public int read(ByteBuffer destination) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] destinations, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer source) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] sources, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel position(long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel truncate(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metadata) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel source, long position, long count) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }
  }
}
