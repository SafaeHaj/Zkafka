package com.zkafka.common.exception;

import java.io.IOException;
import java.util.Optional;

/** A valid broker response reported failure; unknown numeric codes are retained. */
public final class BrokerException extends IOException {
  private static final long serialVersionUID = 1L;
  private final int errorCode;

  public BrokerException(String operation, int errorCode) {
    super(
        operation
            + " failed: "
            + ErrorCode.fromCode(errorCode)
                .map(ErrorCode::description)
                .orElse("Unknown broker error")
            + " ("
            + errorCode
            + ")");
    this.errorCode = errorCode;
  }

  public int errorCode() {
    return errorCode;
  }

  public Optional<ErrorCode> knownError() {
    return ErrorCode.fromCode(errorCode);
  }
}
