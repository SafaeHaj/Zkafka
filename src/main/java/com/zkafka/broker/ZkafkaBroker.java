package com.zkafka.broker;

import com.zkafka.broker.cluster.ZookeeperClient;
import com.zkafka.broker.core.Partition;
import com.zkafka.client.network.SocketTransport;
import com.zkafka.common.exception.ErrorCode;
import com.zkafka.common.model.BrokerInfo;
import com.zkafka.common.protocol.Protocol;
import com.zkafka.common.protocol.Protocol.DataRequest;
import com.zkafka.common.protocol.Protocol.PartitionMetadata;
import com.zkafka.common.protocol.Protocol.TopicMetadata;
import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;

public final class ZkafkaBroker {
  private static final Logger LOGGER = Logger.getLogger(ZkafkaBroker.class.getName());
  private static final String BROKERS = "/brokers";
  private static final String TOPICS = "/topics";
  private static final String CONTROLLER = "/controller";

  private final ZkafkaConfig config;
  private final Protocol protocol;
  private final BrokerInfo self;
  private final ZookeeperClient zkClient;
  private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor();
  private final Semaphore requestSlots;
  private final Set<SocketChannel> activeClients = ConcurrentHashMap.newKeySet();
  private final Map<ReplicaKey, LocalReplica> partitions = new HashMap<>();
  private ServerSocketChannel serverChannel;
  private volatile boolean running;
  private boolean startAttempted;
  private boolean stopped;

  private record ReplicaKey(String topic, int id) {}

  private record LocalReplica(Partition partition, ReentrantLock lock) {
    LocalReplica(Partition partition) {
      this(partition, new ReentrantLock());
    }
  }

  public ZkafkaBroker(ZkafkaConfig config) {
    this.config = Objects.requireNonNull(config, "config");
    protocol = new Protocol(config);
    self = new BrokerInfo(config.broker().id(), config.broker().host(), config.broker().port());
    zkClient = new ZookeeperClient(config.zooKeeper());
    requestSlots = new Semaphore(config.execution().requestThreads());
  }

  public void start() throws IOException, KeeperException, InterruptedException {
    synchronized (this) {
      if (startAttempted || stopped) {
        throw new IllegalStateException("Create a new broker instance for each startup");
      }
      startAttempted = true;
      try {
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(self.getHost(), self.getPort()));
        zkClient.connect();
        zkClient.createPersistentNode(BROKERS, "");
        zkClient.createPersistentNode(TOPICS, "");
        if (!zkClient.createEphemeralNode(
            BROKERS + "/" + self.getId(), self.getPort() + "\n" + self.getHost())) {
          throw new IOException("Broker ID is already registered: " + self.getId());
        }
        running = true;
        int flushInterval = config.storage().flushIntervalMillis();
        maintenance.scheduleWithFixedDelay(
            this::flushPartitions, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
        maintenance.execute(this::electController);
      } catch (IOException | KeeperException | InterruptedException | RuntimeException exception) {
        stop();
        throw exception;
      }
    }

    try {
      while (running) {
        SocketChannel client = serverChannel.accept();
        synchronized (this) {
          if (running) {
            activeClients.add(client);
            connections.execute(() -> handleClient(client));
          } else {
            closeChannel(client);
          }
        }
      }
    } catch (IOException exception) {
      if (running) {
        throw exception;
      }
    } finally {
      stop();
    }
  }

  public void stop() {
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      running = false;
      closeChannel(serverChannel);
      activeClients.forEach(ZkafkaBroker::closeChannel);
      connections.shutdownNow();
      maintenance.shutdownNow();
    }

    boolean interrupted = Thread.interrupted();
    try {
      zkClient.close();
    } catch (InterruptedException exception) {
      interrupted = true;
    } finally {
      synchronized (partitions) {
        partitions
            .values()
            .forEach(
                partition -> {
                  try {
                    partition.partition().close();
                  } catch (IOException exception) {
                    LOGGER.log(Level.WARNING, "Failed to close partition", exception);
                  }
                });
        partitions.clear();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void electController() {
    if (!running) {
      return;
    }
    try {
      if (!zkClient.createEphemeralNode(CONTROLLER, Integer.toString(self.getId()))
          && !zkClient.watchNode(
              CONTROLLER,
              () -> {
                synchronized (this) {
                  if (running) {
                    maintenance.execute(this::electController);
                  }
                }
              })) {
        retryElection();
      }
    } catch (KeeperException exception) {
      LOGGER.log(Level.WARNING, "Controller election failed", exception);
      retryElection();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private synchronized void retryElection() {
    if (running) {
      maintenance.schedule(
          this::electController,
          config.execution().controllerRetryDelayMillis(),
          TimeUnit.MILLISECONDS);
    }
  }

  private Map<Integer, BrokerInfo> liveBrokers() throws KeeperException, InterruptedException {
    Map<Integer, BrokerInfo> brokers = new TreeMap<>();
    for (String child : zkClient.children(BROKERS)) {
      try {
        int id = Integer.parseInt(child);
        String[] endpoint = zkClient.getData(BROKERS + "/" + child).split("\n", 2);
        brokers.put(id, new BrokerInfo(id, endpoint[1], Integer.parseInt(endpoint[0])));
      } catch (KeeperException.NoNodeException ignored) {
        // The broker left after the directory was listed.
      }
    }
    return brokers;
  }

  private static String topicPath(String topic) {
    return TOPICS
        + "/"
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(topic.getBytes(StandardCharsets.UTF_8));
  }

  private TopicMetadata loadTopic(String topic) throws KeeperException, InterruptedException {
    String[] assignments = zkClient.getData(topicPath(topic)).split(";");
    List<PartitionMetadata> metadata = new ArrayList<>(assignments.length);
    for (int id = 0; id < assignments.length; id++) {
      List<Integer> replicas =
          Arrays.stream(assignments[id].split(",")).map(Integer::valueOf).toList();
      metadata.add(new PartitionMetadata(id, replicas.getFirst(), replicas));
    }
    return new TopicMetadata(topic, metadata);
  }

  private ErrorCode createTopic(Protocol.CreateTopicRequest request)
      throws KeeperException, InterruptedException {
    if (request.partitions() <= 0 || !Partition.isValidTopic(request.topic())) {
      return ErrorCode.INVALID_REQUEST;
    }
    List<Integer> brokers = List.copyOf(liveBrokers().keySet());
    int factor = request.replicationFactor();
    if (factor < config.replication().minInSyncReplicas() || factor > brokers.size()) {
      return ErrorCode.INVALID_REPLICATION_FACTOR;
    }
    if ((long) request.partitions() * factor > 50_000) {
      return ErrorCode.INVALID_REQUEST;
    }
    StringBuilder assignments = new StringBuilder();
    for (int partition = 0; partition < request.partitions(); partition++) {
      if (partition > 0) {
        assignments.append(';');
      }
      for (int replica = 0; replica < factor; replica++) {
        if (replica > 0) {
          assignments.append(',');
        }
        assignments.append(brokers.get((partition + replica) % brokers.size()));
      }
    }
    return zkClient.createPersistentNode(topicPath(request.topic()), assignments.toString())
        ? ErrorCode.NONE
        : ErrorCode.TOPIC_ALREADY_EXISTS;
  }

  private ByteBuffer metadata() throws KeeperException, InterruptedException {
    Map<Integer, BrokerInfo> brokers = liveBrokers();
    List<TopicMetadata> topics = new ArrayList<>();
    for (String child : zkClient.children(TOPICS)) {
      String topic = new String(Base64.getUrlDecoder().decode(child), StandardCharsets.UTF_8);
      TopicMetadata stored = loadTopic(topic);
      // The wire format requires a live leader and advertised replica endpoints.
      List<PartitionMetadata> available =
          stored.partitionMetadata().stream()
              .filter(partition -> brokers.containsKey(partition.leaderBrokerId()))
              .map(
                  partition ->
                      new PartitionMetadata(
                          partition.partitionId(),
                          partition.leaderBrokerId(),
                          partition.replicaBrokerIds().stream()
                              .filter(brokers::containsKey)
                              .toList()))
              .toList();
      topics.add(new TopicMetadata(topic, available));
    }
    return protocol.encodeMetadataResponse(List.copyOf(brokers.values()), topics, ErrorCode.NONE);
  }

  private LocalReplica localPartition(DataRequest request) throws IOException {
    synchronized (partitions) {
      if (!running) {
        throw new IOException("Broker is stopping");
      }
      ReplicaKey key = new ReplicaKey(request.topic(), request.partition());
      LocalReplica partition = partitions.get(key);
      if (partition == null) {
        partition = new LocalReplica(new Partition(request.topic(), request.partition(), config));
        partitions.put(key, partition);
      }
      return partition;
    }
  }

  private void flushPartitions() {
    List<Partition> snapshot;
    synchronized (partitions) {
      snapshot = partitions.values().stream().map(LocalReplica::partition).toList();
    }
    for (Partition partition : snapshot) {
      try {
        partition.flush();
      } catch (IOException | IllegalStateException exception) {
        if (running) {
          LOGGER.log(Level.WARNING, "Failed to flush partition", exception);
        }
      }
    }
  }

  private void handleClient(SocketChannel client) {
    try (client) {
      ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);
      while (running) {
        header.clear();
        if (!readFully(client, header)) {
          return;
        }
        int size = header.flip().getInt();
        if (size <= 0 || size > config.network().maxFrameBytes()) {
          return;
        }
        ByteBuffer request = ByteBuffer.allocate(size);
        if (!readFully(client, request)) {
          return;
        }
        ByteBuffer response = dispatch(request.flip());
        header.clear().putInt(response.remaining()).flip();
        while (header.hasRemaining()) {
          client.write(header);
        }
        while (response.hasRemaining()) {
          client.write(response);
        }
      }
    } catch (IOException | IllegalArgumentException exception) {
      LOGGER.log(Level.FINE, "Client connection closed", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    } finally {
      activeClients.remove(client);
    }
  }

  private static boolean readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        return false;
      }
    }
    return true;
  }

  private ByteBuffer dispatch(ByteBuffer body) throws InterruptedException {
    byte type = body.get();
    // Replication must be able to complete while request workers wait for remote appends.
    if (type == Protocol.REPLICATE) {
      return handleData(type, protocol.decodeDataRequest(body, type));
    }
    requestSlots.acquire();
    try {
      return switch (type) {
        case Protocol.PRODUCE, Protocol.FETCH ->
            handleData(type, protocol.decodeDataRequest(body, type));
        case Protocol.METADATA -> {
          if (body.hasRemaining()) {
            yield protocol.encodeMetadataResponse(List.of(), List.of(), ErrorCode.INVALID_REQUEST);
          }
          try {
            yield metadata();
          } catch (KeeperException exception) {
            yield protocol.encodeMetadataResponse(List.of(), List.of(), ErrorCode.INTERNAL_ERROR);
          }
        }
        case Protocol.CREATE_TOPIC -> {
          ErrorCode error;
          try {
            error = createTopic(protocol.decodeCreateTopicRequest(body));
          } catch (KeeperException exception) {
            error = ErrorCode.INTERNAL_ERROR;
          } catch (IllegalArgumentException exception) {
            error = ErrorCode.INVALID_REQUEST;
          }
          yield protocol.encodeStatus(Protocol.CREATE_TOPIC_RESPONSE, error);
        }
        default -> throw new IllegalArgumentException("Unknown request type: " + type);
      };
    } finally {
      requestSlots.release();
    }
  }

  private ByteBuffer handleData(byte type, DataRequest request) throws InterruptedException {
    if (request.value() != null && request.value().length > config.storage().maxRecordBytes()) {
      return dataError(type, request, ErrorCode.MESSAGE_TOO_LARGE);
    }
    try {
      List<PartitionMetadata> assignments = loadTopic(request.topic()).partitionMetadata();
      if (request.partition() >= assignments.size()) {
        return dataError(type, request, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
      }
      PartitionMetadata assignment = assignments.get(request.partition());
      boolean leader = assignment.leaderBrokerId() == self.getId();
      if (type == Protocol.REPLICATE
          ? leader || !assignment.replicaBrokerIds().contains(self.getId())
          : !leader) {
        return dataError(type, request, ErrorCode.NOT_LEADER);
      }
      LocalReplica replica = localPartition(request);
      replica.lock().lockInterruptibly();
      try {
        Partition partition = replica.partition();
        if (type == Protocol.REPLICATE) {
          return protocol.encodeStatus(Protocol.REPLICATE_RESPONSE, replicate(partition, request));
        }
        String watermarkPath = topicPath(request.topic()) + "/" + request.partition();
        zkClient.createPersistentNode(watermarkPath, "0");
        long committed = Long.parseLong(zkClient.getData(watermarkPath));
        if (committed > partition.nextOffset()) {
          return dataError(type, request, ErrorCode.INTERNAL_ERROR);
        }
        if (type == Protocol.FETCH) {
          return fetch(request, partition, committed);
        }
        long offset = produce(request, partition, assignment, watermarkPath);
        return protocol.encodeProduceResponse(
            request.topic(), request.partition(), offset, ErrorCode.NONE);
      } finally {
        replica.lock().unlock();
      }
    } catch (KeeperException.NoNodeException exception) {
      return dataError(type, request, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
    } catch (KeeperException | IOException | IllegalStateException exception) {
      LOGGER.log(Level.WARNING, "Partition request failed", exception);
      return dataError(type, request, ErrorCode.INTERNAL_ERROR);
    }
  }

  private ByteBuffer fetch(DataRequest request, Partition partition, long committed)
      throws IOException {
    if (request.offset() > committed) {
      return dataError(Protocol.FETCH, request, ErrorCode.OFFSET_OUT_OF_RANGE);
    }
    try {
      List<Protocol.FetchedMessage> messages =
          partition.readMessages(request.offset(), request.maxBytes(), committed).stream()
              .map(record -> new Protocol.FetchedMessage(record.offset(), record.value()))
              .toList();
      return protocol.encodeFetchResponse(
          request.topic(), request.partition(), messages, ErrorCode.NONE);
    } catch (IllegalArgumentException exception) {
      return dataError(Protocol.FETCH, request, ErrorCode.MESSAGE_TOO_LARGE);
    }
  }

  private long produce(
      DataRequest request, Partition partition, PartitionMetadata assignment, String watermarkPath)
      throws IOException, KeeperException, InterruptedException {
    Map<Integer, BrokerInfo> brokers = liveBrokers();
    List<BrokerInfo> followers =
        assignment.replicaBrokerIds().stream()
            .filter(id -> id != self.getId())
            .map(brokers::get)
            .filter(Objects::nonNull)
            .toList();
    if (followers.size() + 1 < config.replication().minInSyncReplicas()) {
      throw new IOException("Insufficient live replicas");
    }
    long offset = partition.append(request.value());
    int acknowledgments = 1;
    for (BrokerInfo follower : followers) {
      try (SocketTransport transport =
          new SocketTransport(config, config.execution().replicationRequestTimeoutMillis())) {
        int status = sendReplica(transport, follower, request, offset, request.value());
        if (status == ErrorCode.OFFSET_OUT_OF_RANGE.code()) {
          // Replay in bounded batches when a returning follower missed earlier writes.
          long next = 0;
          while (next <= offset) {
            for (Partition.StoredMessage record :
                partition.readMessages(next, config.storage().maxRecordBytes() + 12, offset + 1)) {
              status = sendReplica(transport, follower, request, record.offset(), record.value());
              if (status != ErrorCode.NONE.code()) {
                throw new IOException("Follower rejected offset " + record.offset());
              }
              next = record.offset() + 1;
            }
          }
        }
        if (status != ErrorCode.NONE.code()) {
          throw new IOException("Follower rejected offset " + offset);
        }
        acknowledgments++;
      } catch (IOException exception) {
        LOGGER.log(Level.FINE, "Replication to broker " + follower.getId() + " failed", exception);
      }
    }
    if (acknowledgments < config.replication().minInSyncReplicas()) {
      throw new IOException("Insufficient replica acknowledgments");
    }
    zkClient.updateNode(watermarkPath, Long.toString(offset + 1), -1);
    return offset;
  }

  private int sendReplica(
      SocketTransport transport,
      BrokerInfo follower,
      DataRequest request,
      long offset,
      byte[] value)
      throws IOException {
    ByteBuffer response =
        transport.exchange(
            follower.getHost(),
            follower.getPort(),
            protocol.encodeReplicateRequest(request.topic(), request.partition(), offset, value));
    if (response.remaining() != 3 || response.get() != Protocol.REPLICATE_RESPONSE) {
      throw new IOException("Invalid replication acknowledgment");
    }
    return Short.toUnsignedInt(response.getShort());
  }

  private ErrorCode replicate(Partition partition, DataRequest request) throws IOException {
    long end = partition.nextOffset();
    if (request.offset() > end) {
      return ErrorCode.OFFSET_OUT_OF_RANGE;
    }
    if (request.offset() == end) {
      partition.append(request.value());
      return ErrorCode.NONE;
    }
    byte[] existing =
        partition
            .readMessages(
                request.offset(), config.storage().maxRecordBytes() + 12, request.offset() + 1)
            .getFirst()
            .value();
    return Arrays.equals(existing, request.value()) ? ErrorCode.NONE : ErrorCode.INVALID_REQUEST;
  }

  private ByteBuffer dataError(byte type, DataRequest request, ErrorCode error) {
    return switch (type) {
      case Protocol.PRODUCE ->
          protocol.encodeProduceResponse(request.topic(), request.partition(), -1, error);
      case Protocol.FETCH ->
          protocol.encodeFetchResponse(request.topic(), request.partition(), List.of(), error);
      case Protocol.REPLICATE -> protocol.encodeStatus(Protocol.REPLICATE_RESPONSE, error);
      default -> throw new IllegalArgumentException("Unknown data request type: " + type);
    };
  }

  private static void closeChannel(Channel channel) {
    if (channel != null) {
      try {
        channel.close();
      } catch (IOException exception) {
        LOGGER.log(Level.WARNING, "Failed to close channel", exception);
      }
    }
  }
}
