package com.zkafka.common.exception;

import java.io.IOException;

/** A peer returned an invalid frame or response body. */
public final class ClientProtocolException extends IOException {
  private static final long serialVersionUID = 1L;

  public ClientProtocolException(String message) {
    super(message);
  }

  public ClientProtocolException(String message, Throwable cause) {
    super(message, cause);
  }
}
