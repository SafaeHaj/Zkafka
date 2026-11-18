import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import com.zkafka.config.ZkafkaConfig;
import com.zkafka.common.protocol.Protocol;
import com.zkafka.client.network.SocketTransport;
import com.zkafka.client.ZkafkaClient;

var config = ZkafkaConfig.load(Path.of("config/zkafka-local.properties"));
var topic = "demo-" + System.currentTimeMillis();

try (var transport = new SocketTransport(config)) {
    var request = new Protocol(config).encodeCreateTopicRequest(topic, 1, (short) 1);
    var reply = transport.exchange("localhost", 9092, request);
    reply.get();
    System.out.println("Create status: " + Short.toUnsignedInt(reply.getShort()));
}

var client = new ZkafkaClient(config);
client.initialize();
System.out.println("Published at offset: " +
    client.send(topic, 0, "hello".getBytes(StandardCharsets.UTF_8)));

client.fetch(topic, 0, 0, 1024).forEach(r ->
    System.out.println(r.offset() + ": " + new String(r.value(), StandardCharsets.UTF_8)));