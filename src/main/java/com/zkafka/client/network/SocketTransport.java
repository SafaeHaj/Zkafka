package com.zkafka.client.network;

import com.zkafka.common.exception.ClientProtocolException;
import com.zkafka.config.ZkafkaConfig;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fresh connection per exchange. Deadlines cover connect/I/O, not system DNS resolution. */
public final class SocketTransport implements RequestTransport {
  private final int connectTimeoutMillis;
  private final int requestTimeoutMillis;
  private final int maxFrameBytes;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Set<Connection> connections = ConcurrentHashMap.newKeySet();

  private record Connection(SocketChannel channel, Selector selector) {}

  public SocketTransport(ZkafkaConfig config) {
    this(config, config.execution().clientRequestTimeoutMillis());
  }

  public SocketTransport(ZkafkaConfig config, int requestTimeoutMillis) {
    Objects.requireNonNull(config, "config");
    if (requestTimeoutMillis <= 0) {
      throw new IllegalArgumentException("Request timeout must be positive");
    }
    connectTimeoutMillis = config.execution().clientConnectTimeoutMillis();
    this.requestTimeoutMillis = requestTimeoutMillis;
    maxFrameBytes = config.network().maxFrameBytes();
  }

  @Override
  public ByteBuffer exchange(String host, int port, ByteBuffer request) throws IOException {
    Objects.requireNonNull(request, "request");
    if (!request.hasRemaining() || request.remaining() > maxFrameBytes) {
      throw new IllegalArgumentException("Request exceeds the allowed frame size");
    }
    requireOpen();
    if (host == null || host.isBlank() || port < 1 || port > 65535) {
      throw new IllegalArgumentException("Invalid broker address");
    }
    InetSocketAddress address = new InetSocketAddress(host, port);
    if (address.isUnresolved()) {
      throw new UnknownHostException(host);
    }
    try (SocketChannel channel = SocketChannel.open();
        Selector selector = Selector.open()) {
      Connection connection = new Connection(channel, selector);
      connections.add(connection);
      try {
        requireOpen();
        channel.configureBlocking(false);
        SelectionKey key = channel.register(selector, 0);
        long connectDeadline = deadline(connectTimeoutMillis);
        if (!channel.connect(address)) {
          while (!channel.finishConnect()) {
            await(selector, key, SelectionKey.OP_CONNECT, connectDeadline);
          }
        }

        long requestDeadline = deadline(requestTimeoutMillis);
        ByteBuffer prefix = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        prefix.putInt(request.remaining()).flip();
        writeFully(channel, selector, key, prefix, requestDeadline);
        writeFully(channel, selector, key, request.duplicate(), requestDeadline);

        prefix.clear();
        readFully(channel, selector, key, prefix, requestDeadline);
        prefix.flip();
        int length = prefix.getInt();
        if (length <= 0 || length > maxFrameBytes) {
          throw new ClientProtocolException("Invalid response frame length: " + length);
        }
        ByteBuffer response = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, selector, key, response, requestDeadline);
        return response.flip();
      } catch (CancelledKeyException exception) {
        throw new IOException("Connection closed during exchange", exception);
      } finally {
        connections.remove(connection);
      }
    }
  }

  private void writeFully(
      SocketChannel channel, Selector selector, SelectionKey key, ByteBuffer buffer, long deadline)
      throws IOException {
    while (buffer.hasRemaining()) {
      remaining(deadline);
      if (channel.write(buffer) == 0) {
        await(selector, key, SelectionKey.OP_WRITE, deadline);
      }
    }
  }

  private void readFully(
      SocketChannel channel, Selector selector, SelectionKey key, ByteBuffer buffer, long deadline)
      throws IOException {
    while (buffer.hasRemaining()) {
      remaining(deadline);
      int read = channel.read(buffer);
      if (read < 0) {
        throw new EOFException("Connection ended before the complete response frame");
      }
      if (read == 0) {
        await(selector, key, SelectionKey.OP_READ, deadline);
      }
    }
  }

  private void await(Selector selector, SelectionKey key, int operation, long deadline)
      throws IOException {
    key.interestOps(operation);
    selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining(deadline))));
    selector.selectedKeys().clear();
    remaining(deadline);
  }

  private long remaining(long deadline) throws IOException {
    requireOpen();
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Client exchange interrupted");
    }
    long remaining = deadline - System.nanoTime();
    if (remaining <= 0) {
      throw new SocketTimeoutException("Client operation timed out");
    }
    return remaining;
  }

  private static long deadline(int milliseconds) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(milliseconds);
  }

  private void requireOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Client transport is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    IOException failure = null;
    for (Connection connection : connections) {
      connection.selector().wakeup();
      try {
        connection.channel().close();
      } catch (IOException exception) {
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
