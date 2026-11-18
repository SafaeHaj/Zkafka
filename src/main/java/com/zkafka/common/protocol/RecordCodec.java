package com.zkafka.common.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/** Storage record v1: offset (8), length (4), CRC32C (4), value (N). No file I/O. */
public final class RecordCodec {
  public static final int HEADER_BYTES = 16;

  private RecordCodec() {}

  public record Header(long offset, int valueLength, int checksum) {}

  public record DecodedRecord(long offset, byte[] value) {}

  public static ByteBuffer encode(long offset, byte[] value) {
    ByteBuffer record =
        ByteBuffer.allocate(Math.addExact(HEADER_BYTES, value.length)).order(ByteOrder.BIG_ENDIAN);
    record.putLong(offset).putInt(value.length).putInt(0).put(value);
    byte[] bytes = record.array();
    int crc =
        checksum(ByteBuffer.wrap(bytes, 0, 12), ByteBuffer.wrap(bytes, HEADER_BYTES, value.length));
    record.putInt(12, crc);
    return record.flip();
  }

  public static Header decodeHeader(ByteBuffer bytes, long expectedOffset, int maxRecordBytes)
      throws IOException {
    ByteBuffer header = bytes.duplicate().order(ByteOrder.BIG_ENDIAN);
    if (header.remaining() != HEADER_BYTES) {
      throw new IOException("Invalid record header size");
    }
    long offset = header.getLong();
    int length = header.getInt();
    int checksum = header.getInt();
    if (offset < 0 || offset != expectedOffset || offset == Long.MAX_VALUE) {
      throw new IOException("Invalid record offset: " + offset + "; expected " + expectedOffset);
    }
    if (length < 0 || length > maxRecordBytes) {
      throw new IOException("Invalid record length at offset " + offset + ": " + length);
    }
    return new Header(offset, length, checksum);
  }

  public static DecodedRecord decodeValue(Header header, byte[] value) throws IOException {
    if (value.length != header.valueLength()) {
      throw new IOException("Incorrect value length at offset " + header.offset());
    }
    ByteBuffer fields = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN);
    fields.putLong(header.offset()).putInt(header.valueLength()).flip();
    if (checksum(fields, ByteBuffer.wrap(value)) != header.checksum()) {
      throw new IOException("Checksum mismatch at offset " + header.offset());
    }
    return new DecodedRecord(header.offset(), value);
  }

  private static int checksum(ByteBuffer fields, ByteBuffer value) {
    CRC32C checksum = new CRC32C();
    checksum.update(fields.duplicate());
    checksum.update(value.duplicate());
    return (int) checksum.getValue();
  }
}
