package com.practice.kafkaprogramming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
  private Producer() {
  }
  private static final KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

  private static KafkaProducer<String, String> getKafkaProducer() {
    // server properties
    Properties properties = new Properties();
    properties.setProperty(Constants.SERVER_NAME, Constants.URL_PORT);

    // serializer key and value properties
    properties.setProperty(Constants.KEY_SERIALIZER, StringSerializer.class.getName());
    properties.setProperty(Constants.VALUE_SERIALIZER, StringSerializer.class.getName());

    //set safe producer configs (kafka <= 2.8)
    properties.setProperty(Constants.ENABLE_IDEMPOTENCE, "true");
    properties.setProperty(Constants.ACKS_CONFIGS, "all"); // same as settings -1
    properties.setProperty(Constants.RETRIES_CONFIGS, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(Constants.MAX_FLIGHT_REQUEST_PER_CONN, "5");

    // high throughput producer(at the expense of a bit of latency and cpu usage)
    properties.setProperty(Constants.COMPRESSION_CONFIG, "snappy");
    properties.setProperty(Constants.LINGER_CONFIG, "20");
    properties.setProperty(Constants.BATCH_CONFIG, Integer.toString(32*1024));


    return new KafkaProducer<>(properties);
  }

  public static KafkaProducer<String, String> getProducer() {
    return kafkaProducer;
  }
}
