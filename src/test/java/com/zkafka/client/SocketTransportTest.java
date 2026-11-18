package com.zkafka.client;

import com.zkafka.client.network.SocketTransport;
import com.zkafka.client.producer.Producer;
import com.zkafka.common.exception.ClientProtocolException;
import com.zkafka.config.ZkafkaConfig;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class SocketTransportTest {
  private static int checks;

  public static void main(String[] args) throws Exception {
    fragmentedFrame();
    invalidFrameLengths();
    truncatedFrame();
    requestDeadline();
    closeCancelsExchange();
    endToEndProducer();
    System.out.println("Socket transport checks passed: " + checks);
  }

  private static ZkafkaConfig settings(int requestTimeout) throws IOException {
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(Path.of("config/zkafka.properties"))) {
      properties.load(reader);
    }
    properties.setProperty("client.request.timeout.ms", Integer.toString(requestTimeout));
    return ZkafkaConfig.fromProperties(properties);
  }

  private static ServerSocket listen() throws IOException {
    ServerSocket server = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
    server.setSoTimeout(3000);
    return server;
  }

  private static void fragmentedFrame() throws Exception {
    try (ServerSocket server = listen();
        var worker = Executors.newSingleThreadExecutor();
        SocketTransport transport = new SocketTransport(settings(2000))) {
      byte[] expected = {1, 2, 3, 4, 5};
      Future<?> response =
          worker.submit(
              () ->
                  serve(
                      server,
                      socket -> {
                        check(
                            Arrays.equals(readRequest(socket), new byte[] {3}),
                            "Request length prefix and body");
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        byte[] frame =
                            ByteBuffer.allocate(4 + expected.length)
                                .putInt(expected.length)
                                .put(expected)
                                .array();
                        for (byte value : frame) {
                          out.writeByte(value);
                          out.flush();
                        }
                      }));
      ByteBuffer request = ByteBuffer.wrap(new byte[] {0, 3});
      request.position(1);
      ByteBuffer actual = transport.exchange("localhost", server.getLocalPort(), request);
      byte[] bytes = new byte[actual.remaining()];
      actual.get(bytes);
      check(Arrays.equals(bytes, expected), "Fragmented frame assembled");
      check(request.position() == 1, "Caller request buffer position preserved");
      response.get(3, TimeUnit.SECONDS);
    }
  }

  private static void invalidFrameLengths() throws Exception {
    ZkafkaConfig config = settings(2000);
    for (int length : new int[] {0, -1, config.network().maxFrameBytes() + 1}) {
      try (ServerSocket server = listen();
          var worker = Executors.newSingleThreadExecutor();
          SocketTransport transport = new SocketTransport(config)) {
        Future<?> response =
            worker.submit(
                () ->
                    serve(
                        server,
                        socket -> {
                          readRequest(socket);
                          new DataOutputStream(socket.getOutputStream()).writeInt(length);
                        }));
        rejects(
            ClientProtocolException.class,
            () ->
                transport.exchange(
                    "localhost", server.getLocalPort(), ByteBuffer.wrap(new byte[] {3})));
        response.get(3, TimeUnit.SECONDS);
      }
    }
  }

  private static void truncatedFrame() throws Exception {
    try (ServerSocket server = listen();
        var worker = Executors.newSingleThreadExecutor();
        SocketTransport transport = new SocketTransport(settings(2000))) {
      Future<?> response =
          worker.submit(
              () ->
                  serve(
                      server,
                      socket -> {
                        readRequest(socket);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeInt(10);
                        out.write(new byte[] {1, 2});
                      }));
      rejects(
          IOException.class,
          () ->
              transport.exchange(
                  "localhost", server.getLocalPort(), ByteBuffer.wrap(new byte[] {3})));
      response.get(3, TimeUnit.SECONDS);
    }
  }

  private static void requestDeadline() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    try (ServerSocket server = listen();
        var worker = Executors.newSingleThreadExecutor();
        SocketTransport transport = new SocketTransport(settings(200))) {
      Future<?> response =
          worker.submit(
              () ->
                  serve(
                      server,
                      socket -> {
                        readRequest(socket);
                        release.await(3, TimeUnit.SECONDS);
                      }));
      try {
        rejects(
            SocketTimeoutException.class,
            () ->
                transport.exchange(
                    "localhost", server.getLocalPort(), ByteBuffer.wrap(new byte[] {3})));
      } finally {
        release.countDown();
      }
      response.get(3, TimeUnit.SECONDS);
    } finally {
      release.countDown();
    }
  }

  private static void closeCancelsExchange() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (ServerSocket server = listen();
        var workers = Executors.newFixedThreadPool(2);
        SocketTransport transport = new SocketTransport(settings(10000))) {
      Future<?> serverTask =
          workers.submit(
              () ->
                  serve(
                      server,
                      socket -> {
                        readRequest(socket);
                        entered.countDown();
                        release.await(3, TimeUnit.SECONDS);
                      }));
      Future<?> clientTask =
          workers.submit(
              () -> {
                try {
                  transport.exchange(
                      "localhost", server.getLocalPort(), ByteBuffer.wrap(new byte[] {3}));
                  throw new AssertionError("Exchange should be cancelled");
                } catch (IOException expected) {
                  return;
                }
              });
      check(entered.await(3, TimeUnit.SECONDS), "Exchange waiting for response");
      try {
        transport.close();
        clientTask.get(2, TimeUnit.SECONDS);
        checks++;
        rejects(
            IOException.class,
            () ->
                transport.exchange(
                    "localhost", server.getLocalPort(), ByteBuffer.wrap(new byte[] {3})));
      } finally {
        release.countDown();
      }
      serverTask.get(3, TimeUnit.SECONDS);
    } finally {
      release.countDown();
    }
  }

  private static void endToEndProducer() throws Exception {
    // Bootstrap and leader can have distinct addresses. This socket fixture supplies both.
    try (ServerSocket bootstrap = listen();
        ServerSocket leader = listen();
        var workers = Executors.newFixedThreadPool(2)) {
      ZkafkaConfig base = settings(2000);
      ZkafkaConfig config =
          new ZkafkaConfig(
              new ZkafkaConfig.Broker(1, "localhost", bootstrap.getLocalPort()),
              base.zooKeeper(),
              base.execution(),
              base.storage(),
              base.replication(),
              base.network(),
              base.consumer());
      Future<?> metadataTask =
          workers.submit(
              () ->
                  serve(
                      bootstrap,
                      socket -> {
                        check(
                            Arrays.equals(readRequest(socket), new byte[] {3}),
                            "Bootstrap metadata request");
                        byte[] host = "localhost".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        byte[] topic = "events".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ByteBuffer body = ByteBuffer.allocate(100);
                        body.put((byte) 0x13).putShort((short) 0).putInt(1).putInt(7);
                        body.putShort((short) host.length).put(host).putInt(leader.getLocalPort());
                        body.putInt(1).putShort((short) topic.length).put(topic).putInt(1);
                        body.putInt(5).putInt(7).putInt(1).putInt(7).flip();
                        respond(socket, body);
                      }));
      Future<?> produceTask =
          workers.submit(
              () ->
                  serve(
                      leader,
                      socket -> {
                        ByteBuffer request = ByteBuffer.wrap(readRequest(socket));
                        check(request.get() == 1, "Leader produce request");
                        byte[] topic = new byte[Short.toUnsignedInt(request.getShort())];
                        request.get(topic);
                        check(
                            request.getInt() == 5,
                            "Real producer selects advertised partition, not zero");
                        check(request.getInt() == 3, "Payload length");
                        respond(socket, ClientTest.produce("events", 5, 0, 42));
                      }));
      try (Producer producer = new Producer(config, "events")) {
        Producer.SendResult result = producer.send("abc");
        check(result.partition() == 5 && result.offset() == 42, "End-to-end framed acknowledgment");
      }
      metadataTask.get(3, TimeUnit.SECONDS);
      produceTask.get(3, TimeUnit.SECONDS);
    }
  }

  private static byte[] readRequest(Socket socket) throws IOException {
    socket.setSoTimeout(2000);
    DataInputStream in = new DataInputStream(socket.getInputStream());
    int length = in.readInt();
    if (length <= 0 || length > 4194304) {
      throw new AssertionError("Invalid request size");
    }
    byte[] body = new byte[length];
    in.readFully(body);
    return body;
  }

  private static void respond(Socket socket, ByteBuffer response) throws IOException {
    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    out.writeInt(response.remaining());
    byte[] bytes = new byte[response.remaining()];
    response.get(bytes);
    out.write(bytes);
    out.flush();
  }

  private static void serve(ServerSocket server, Handler handler) {
    try (Socket socket = server.accept()) {
      handler.handle(socket);
    } catch (Exception exception) {
      throw new AssertionError(exception);
    }
  }

  private static void rejects(Class<? extends Exception> type, Action action) throws Exception {
    try {
      action.run();
    } catch (Exception exception) {
      if (type.isInstance(exception)) {
        checks++;
        return;
      }
      throw exception;
    }
    throw new AssertionError("Expected " + type.getSimpleName());
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

  @FunctionalInterface
  private interface Handler {
    void handle(Socket socket) throws Exception;
  }
}
