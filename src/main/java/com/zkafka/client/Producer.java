package com.zkafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Producer {
    private final ZkafkaClient client;
    private final String topic;

    public Producer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new ZkafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    public long send(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, 0, data); // Default to partition 0
    }
    // Additional methods
}
