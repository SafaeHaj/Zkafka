package com.zkafka.common.model;

public class BrokerInfo {
  private final int id;
  private final String host;
  private final int port;

  public BrokerInfo(int id, String host, int port) {
    if (id < 0) {
      throw new IllegalArgumentException("Broker ID must be nonnegative: " + id);
    }

    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("Broker host must not be null or blank");
    }

    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("Broker port must be between 1 and 65535: " + port);
    }

    this.id = id;
    this.host = host;
    this.port = port;
  }

  public int getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "BrokerInfo[id=" + id + ", host=" + host + ", port=" + port + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BrokerInfo other)) {
      return false;
    }
    return id == other.id;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(id);
  }
}
