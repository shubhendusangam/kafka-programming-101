package com.practice.kafkaprogramming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.URL_PORT);

    // deserializer key and value properties
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // consumer group id
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONSUMER_GROUPID);
    // read data from offset
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_STATUS);
    // partition assignment strategy for load balancing
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // Create a consumer and poll data
    return new KafkaConsumer<>(properties);
  }

  public static KafkaConsumer<String, String> getConsumer() {
    return kafkaConsumer;
  }
}
