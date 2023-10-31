package com.practice.kafkaprogramming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

  private Consumer() {
  }
  private static final KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();

  private static KafkaConsumer<String, String> getKafkaConsumer() {
    // server properties
    Properties properties = new Properties();
    properties.setProperty(Constants.SERVER_NAME, Constants.URL_PORT);

    // deserializer key and value properties
    properties.setProperty(Constants.KEY_DESERIALIZER, StringDeserializer.class.getName());
    properties.setProperty(Constants.VALUE_DESERIALIZER, StringDeserializer.class.getName());
    // consumer group id
    properties.setProperty(Constants.GROUP_ID, Constants.CONSUMER_GROUPID);
    // read data from offset
    properties.setProperty(Constants.AUTO_OFFSET_RESET, Constants.AUTO_OFFSET_RESET_STATUS);
    // partition assignment strategy for load balancing
    properties.setProperty(Constants.PARTITION_ASSIGNMENT_STRATEGY, CooperativeStickyAssignor.class.getName());

    // Create a consumer and poll data
    return new KafkaConsumer<>(properties);
  }

  public static KafkaConsumer<String, String> getConsumer() {
    return kafkaConsumer;
  }
}
