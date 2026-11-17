package com.zkafka.broker;

import java.nio.charset.StandardCharsets;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.Objects;

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

    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private boolean connectAttempted;
    private boolean closed;

    private volatile IOException sessionFailure;

    private ZooKeeper zooKeeper;

    public ZookeeperClient(String connectString, int sessionTimeoutMillis) {
        if (connectString == null || connectString.isBlank()) {
            throw new IllegalArgumentException(
                    "ZooKeeper connection string must not be blank");
        }
        if (sessionTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Session timeout must be positive");
        }

        this.connectString = connectString;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }

    public void connect(int connectionTimeoutMillis) throws IOException, InterruptedException {
        if (connectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Connection timeout must be positive");
        }
        if (connectAttempted || closed) {
            throw new IllegalStateException(
                    "Create a new client for each connection attempt");
        }

        connectAttempted = true;

        zooKeeper = new ZooKeeper(
                connectString, sessionTimeoutMillis, this);

        boolean signalled = connectedSignal.await(
                connectionTimeoutMillis, TimeUnit.MILLISECONDS);

        if (!signalled) {
            throw new IOException(
                    "Timed out connecting to ZooKeeper at " + connectString);
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

    public void updateNode(String path, String data, int expectedVersion)
            throws KeeperException, InterruptedException {

        Objects.requireNonNull(data, "Node data must not be null");

        zooKeeper.setData(
                path,
                data.getBytes(StandardCharsets.UTF_8),
                expectedVersion);
    }

    public List<String> watchChildren(String path, Runnable onChanged)
            throws KeeperException, InterruptedException {

        Objects.requireNonNull(onChanged, "Change callback must not be null");

        List<String> children = zooKeeper.getChildren(path, event -> {
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

        Stat stat = zooKeeper.exists(path, event -> {
            switch (event.getType()) {
                case NodeCreated, NodeDeleted, NodeDataChanged ->
                    onChanged.run();

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

            case Disconnected ->
                LOGGER.warning("Disconnected from ZooKeeper");

            case Expired, AuthFailed -> {
                sessionFailure = new IOException(
                        "ZooKeeper session failed: " + event.getState());

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
            zooKeeper = null;
        }

        closed = true;
    }

    private boolean createNode(String path, String data, CreateMode mode)
            throws KeeperException, InterruptedException {

        Objects.requireNonNull(data, "Node data must not be null");

        try {
            zooKeeper.create(
                    path,
                    data.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    mode);

            LOGGER.info("Created " + mode + " node: " + path);
            return true;

        } catch (KeeperException.NodeExistsException exception) {
            return false;
        }
    }
}
