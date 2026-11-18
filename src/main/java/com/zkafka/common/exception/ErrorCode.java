package com.zkafka.common.exception;

import java.util.Optional;

public enum ErrorCode {
  NONE(0, "Success"),
  INVALID_REQUEST(1, "Invalid request"),
  UNKNOWN_TOPIC_OR_PARTITION(2, "Unknown topic or partition"),
  NOT_LEADER(3, "Broker is not the partition leader"),
  MESSAGE_TOO_LARGE(4, "Message exceeds the configured limit"),
  OFFSET_OUT_OF_RANGE(5, "Offset is outside the available log range"),
  TOPIC_ALREADY_EXISTS(6, "Topic already exists"),
  INVALID_REPLICATION_FACTOR(7, "Invalid replication factor"),
  INTERNAL_ERROR(8, "Internal broker error");

  private final int code;
  private final String description;

  ErrorCode(int code, String description) {
    this.code = code;
    this.description = description;
  }

  public int code() {
    return code;
  }

  public String description() {
    return description;
  }

  public static Optional<ErrorCode> fromCode(int code) {
    for (ErrorCode error : values()) {
      if (error.code == code) {
        return Optional.of(error);
      }
    }
    return Optional.empty();
  }
}
