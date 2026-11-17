package com.zkafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;

import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;

import com.zkafka.broker.ZookeeperClient;
import com.zkafka.broker.Protocol.TopicMetadata;

public final class ZkafkaBroker {
    private final BrokerInfo self;
    private final ZookeeperClient zkClient;

    private final ConcurrentMap<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();

    private final ExecutorService requestExecutor;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isController = new AtomicBoolean(false);

    private ServerSocketChannel serverChannel;
    private final Set<SocketChannel> activeClients = ConcurrentHashMap.newKeySet();

    private final Object lifecycleLock = new Object();

    private boolean startAttempted;
    private boolean stopped;

    private static final Logger LOGGER = Logger.getLogger(ZkafkaBroker.class.getName());

    public ZkafkaBroker(int brokerId, String host, int port, String zkConnectString) {
        this.self = new BrokerInfo(brokerId, host, port);
        this.zkClient = new ZookeeperClient(zkConnectString, 5000);
        this.requestExecutor = Executors.newFixedThreadPool(10);
    }

    public void start() throws IOException {
        synchronized (lifecycleLock) {
            if (startAttempted || stopped) {
                throw new IllegalStateException(
                        "Create a new broker instance for each startup");
            }

            startAttempted = true;

            try {
                serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(true);
                serverChannel.bind(
                        new InetSocketAddress(self.getHost(), self.getPort()));

                zkClient.connect(10000);
                registerWithZookeeper();

                isRunning.set(true);

            } catch (IOException | KeeperException
                    | InterruptedException | RuntimeException exception) {

                stop();
                throw exception;
            }
        }

        try {
            acceptConnections();
        } finally {
            stop();
        }
    }

    public void stop() { ... }

    private void registerWithZookeeper() { ... }

    private void onBrokersChanged(List<String> brokerIds) {
        // Update cluster metadata
        // Remove brokers that have disappeared
        // Re-elect controller if needed
        // As controller, rebalance partitions due to cluster changes
        // Re-attempt controller election
    }

    private void electController() {
        try {
            String controllerPath = "/controller";

            // Check if node exists and is valid
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }
            // Try to become controller
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }
            if (becameController) {
                isController.set(true);
                rebalancePartitions();
            } else {
                // Watch for controller changes
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            // Retry election after delay
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        // Create topic directory
        // Create topic in ZooKeeper
        // Create partitions
        // Select leader and followers
        // Create partition
        // Store partition metadata in ZooKeeper
        // Add topic to broker's metadata
        // Notify all brokers to load the topic
    }

    private void loadTopic(String topic) throws Exception { ... }

    private void rebalancePartitions() { ... }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Parse request data
        // Check if topic exists
        // Find the partition
        // Check if this broker is the leader for the partition
        // Forward to leader
        // Append message to log
        // Replicate to followers
        // Send acknowledgment to client
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Parse request data
        // Check if topic exists
        // Find the partition
        // Check if the offset is valid
        // No messages available at this offset
        // Read messages from log
        // Send response
        // 1 byte for response type, 4 bytes for message count
        // 8 bytes for offset, 4 bytes for length, plus message bytes
    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        // Prepare replication request
        // Send request to follower
        // Read acknowledgment
    }

    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Parse request data
        // Verify topic and partition
        // Find the partition
        // Append message to log (as follower)
        // Send acknowledgment
    }

    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);

                    // Handle client connection in a separate thread
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100); // Small pause to prevent CPU spin
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    // Process the message based on protocol
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // Connection closed by client
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50); // Small pause to prevent CPU spin
            }
        } catch (Exception e) {
            // Handle errors
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    private void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }

        try {
            channel.close();
        } catch (IOException exception) {
            LOGGER.log(Level.WARNING, "Failed to close channel", exception);
        }
    }
}