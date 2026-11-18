package com.zkafka.broker.cluster;

import com.zkafka.config.ZkafkaConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public final class ZookeeperClient implements Watcher, AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
  private final String connectString;
  private final int sessionTimeoutMillis;
  private final int connectTimeoutMillis;

  private final CountDownLatch connectedSignal = new CountDownLatch(1);
  private boolean connectAttempted;
  private boolean closed;

  private volatile IOException sessionFailure;

  private volatile ZooKeeper zooKeeper;

  public ZookeeperClient(ZkafkaConfig.ZooKeeperSettings config) {
    Objects.requireNonNull(config, "config");
    this.connectString = config.connect();
    this.sessionTimeoutMillis = config.sessionTimeoutMillis();
    this.connectTimeoutMillis = config.connectTimeoutMillis();
  }

  public void connect() throws IOException, InterruptedException {
    if (connectAttempted || closed) {
      throw new IllegalStateException("Create a new client for each connection attempt");
    }

    connectAttempted = true;

    zooKeeper = new ZooKeeper(connectString, sessionTimeoutMillis, this);

    boolean signalled = connectedSignal.await(connectTimeoutMillis, TimeUnit.MILLISECONDS);

    if (!signalled) {
      throw new IOException("Timed out connecting to ZooKeeper at " + connectString);
    }
    if (sessionFailure != null) {
      throw sessionFailure;
    }
  }

  public boolean createPersistentNode(String path, String data)
      throws KeeperException, InterruptedException {

    return createNode(path, data, CreateMode.PERSISTENT);
  }

  public boolean createEphemeralNode(String path, String data)
      throws KeeperException, InterruptedException {

    return createNode(path, data, CreateMode.EPHEMERAL);
  }

  public String getData(String path) throws KeeperException, InterruptedException {
    return new String(zooKeeper.getData(path, false, null), StandardCharsets.UTF_8);
  }

  public List<String> children(String path) throws KeeperException, InterruptedException {
    return List.copyOf(zooKeeper.getChildren(path, false));
  }

  public void updateNode(String path, String data, int expectedVersion)
      throws KeeperException, InterruptedException {

    Objects.requireNonNull(data, "Node data must not be null");

    zooKeeper.setData(path, data.getBytes(StandardCharsets.UTF_8), expectedVersion);
  }

  public List<String> watchChildren(String path, Runnable onChanged)
      throws KeeperException, InterruptedException {

    Objects.requireNonNull(onChanged, "Change callback must not be null");

    List<String> children =
        zooKeeper.getChildren(
            path,
            event -> {
              switch (event.getType()) {
                case NodeChildrenChanged, NodeDeleted -> onChanged.run();

                default -> {
                  // Session events are handled separately.
                }
              }
            });

    return List.copyOf(children);
  }

  public boolean watchNode(String path, Runnable onChanged)
      throws KeeperException, InterruptedException {

    Objects.requireNonNull(onChanged, "Change callback must not be null");

    Stat stat =
        zooKeeper.exists(
            path,
            event -> {
              switch (event.getType()) {
                case NodeCreated, NodeDeleted, NodeDataChanged -> onChanged.run();

                default -> {
                  // Events outside this node watch's responsibility.
                }
              }
            });

    return stat != null;
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Watcher.Event.EventType.None) {
      return;
    }

    switch (event.getState()) {
      case SyncConnected -> {
        LOGGER.info("Connected to ZooKeeper");
        connectedSignal.countDown();
      }

      case Disconnected -> LOGGER.warning("Disconnected from ZooKeeper");

      case Expired, AuthFailed -> {
        sessionFailure = new IOException("ZooKeeper session failed: " + event.getState());

        LOGGER.severe(sessionFailure.getMessage());
        connectedSignal.countDown();
      }

      default -> {
        // Other session states do not complete initial connection.
      }
    }
  }

  @Override
  public void close() throws InterruptedException {
    if (closed) {
      return;
    }

    if (zooKeeper != null) {
      zooKeeper.close();
    }

    closed = true;
  }

  private boolean createNode(String path, String data, CreateMode mode)
      throws KeeperException, InterruptedException {

    Objects.requireNonNull(data, "Node data must not be null");

    try {
      zooKeeper.create(
          path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);

      LOGGER.info("Created " + mode + " node: " + path);
      return true;

    } catch (KeeperException.NodeExistsException exception) {
      return false;
    }
  }
}
