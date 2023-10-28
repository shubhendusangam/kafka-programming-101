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
  public static final String GROUP_ID = "group.id";
  public static final String CONSUMER_GROUPID = "my-first-application";
  public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  public static final String AUTO_OFFSET_RESET_STATUS = "earliest";
  public static final String PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";
}
