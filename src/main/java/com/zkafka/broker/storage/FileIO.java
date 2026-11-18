package com.zkafka.broker.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class FileIO {
  private FileIO() {}

  public static void readFully(FileChannel channel, ByteBuffer buffer, long position)
      throws IOException {
    validatePosition(buffer, position);
    while (buffer.hasRemaining()) {
      int read = channel.read(buffer, position);
      if (read < 0) {
        throw new EOFException("Unexpected end of file at byte " + position);
      }
      if (read == 0) {
        throw new IOException("File read made no progress at byte " + position);
      }
      position += read;
    }
  }

  public static void writeFully(FileChannel channel, ByteBuffer buffer, long position)
      throws IOException {
    validatePosition(buffer, position);
    while (buffer.hasRemaining()) {
      int written = channel.write(buffer, position);
      if (written == 0) {
        throw new IOException("File write made no progress at byte " + position);
      }
      position += written;
    }
  }

  private static void validatePosition(ByteBuffer buffer, long position) {
    if (position < 0 || position > Long.MAX_VALUE - buffer.remaining()) {
      throw new IllegalArgumentException("Invalid file position");
    }
  }
}
