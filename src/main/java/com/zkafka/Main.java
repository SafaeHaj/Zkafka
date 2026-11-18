package com.zkafka;

import com.zkafka.broker.ZkafkaBroker;
import com.zkafka.config.ZkafkaConfig;
import java.nio.file.Path;

public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: java -jar zkafka.jar <properties-file>");
    }
    ZkafkaConfig config = ZkafkaConfig.load(Path.of(args[0]));
    ZkafkaBroker broker = new ZkafkaBroker(config);
    Thread shutdownHook = new Thread(broker::stop, "zkafka-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    try {
      broker.start();
    } finally {
      broker.stop();
    }
  }
}
