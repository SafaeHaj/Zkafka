package com.zkafka.broker.core;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class ClientValidation {
  private ClientValidation() {}

  public static String topic(String topic) {
    if (topic == null || topic.isBlank() || topic.getBytes(StandardCharsets.UTF_8).length > 65535) {
      throw new IllegalArgumentException("Topic must be nonblank and fit its encoded length");
    }
    return topic;
  }

  public static void partition(int partition) {
    if (partition < 0) {
      throw new IllegalArgumentException("Partition must be nonnegative");
    }
  }

  public static void offset(long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Invalid offset: " + offset);
    }
  }

  public static void value(byte[] value, int maximum) {
    Objects.requireNonNull(value, "value");
    if (value.length > maximum) {
      throw new IllegalArgumentException("Message exceeds storage.max.record.bytes");
    }
  }
}
