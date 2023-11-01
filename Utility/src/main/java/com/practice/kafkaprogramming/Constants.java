package com.practice.kafkaprogramming;

public class Constants {
  private Constants() {
    throw new IllegalStateException("Constant internal class");
  }

  public static final String URL_PORT = "127.0.0.1:9092";
  public static final String KAFKA_TOPIC = "first_topic";
  public static final String WIKIMEDIA_TOPIC = "wikimedia.recentchanges";
  public static final String CONSUMER_GROUPID = "my-first-application";
  public static final String AUTO_OFFSET_RESET_STATUS = "latest";
  public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange/";
  public static final String OPENSEARCH_HOST = "http://localhost:9200";
  public static final String OPENSEARCH_INDEX = "wikimedia";
}
