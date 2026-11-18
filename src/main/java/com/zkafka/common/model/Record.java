package com.zkafka.common.model;

import java.util.Arrays;
import java.util.Objects;

/** A fetched record. Payload ownership is independent of the response buffer and caller. */
public record Record(long offset, byte[] value) {
  public Record {
    if (offset < 0 || offset == Long.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid consumer offset: " + offset);
    }
    value = Objects.requireNonNull(value, "value").clone();
  }

  @Override
  public byte[] value() {
    return value.clone();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Record record
        && offset == record.offset
        && Arrays.equals(value, record.value);
  }

  @Override
  public int hashCode() {
    return 31 * Long.hashCode(offset) + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return "Record[offset=" + offset + ", valueBytes=" + value.length + "]";
  }
}
