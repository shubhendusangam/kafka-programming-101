package com.practice.kafkaprogramming;

public class Constants {
  private Constants() {
    throw new IllegalStateException("Constant internal class");
  }

  public static final String SERVER_NAME = "bootstrap.servers";
  public static final String URL_PORT = "127.0.0.1:9092";
  public static final String KEY_SERIALIZER = "key.serializer";
  public static final String KEY_DESERIALIZER = "key.deserializer";
  public static final String VALUE_SERIALIZER = "value.serializer";
  public static final String VALUE_DESERIALIZER = "value.deserializer";
  public static final String KAFKA_TOPIC = "first_topic";
  public static final String WIKIMEDIA_TOPIC = "wikimedia.recentchanges";
  public static final String GROUP_ID = "group.id";
  public static final String CONSUMER_GROUPID = "my-first-application";
  public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  public static final String AUTO_OFFSET_RESET_STATUS = "earliest";
  public static final String PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";
  public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange/";
  public static final String ENABLE_IDEMPOTENCE = "enable.idempotence";
  public static final String ACKS_CONFIGS = "acks";
  public static final String RETRIES_CONFIGS = "retries";
  public static final String MAX_FLIGHT_REQUEST_PER_CONN = "max.in.flight.requests.per.connection";
  public static final String LINGER_CONFIG = "linger.ms";
  public static final String COMPRESSION_CONFIG = "compression.type";
  public static final String BATCH_CONFIG = "batch.size";
}
