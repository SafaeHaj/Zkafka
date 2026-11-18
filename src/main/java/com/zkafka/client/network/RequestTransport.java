package com.zkafka.client.network;

import java.io.IOException;
import java.nio.ByteBuffer;

/** One request body and one response body; implementations own transport framing. */
public interface RequestTransport extends AutoCloseable {
  ByteBuffer exchange(String host, int port, ByteBuffer request) throws IOException;

  @Override
  void close() throws IOException;
}
