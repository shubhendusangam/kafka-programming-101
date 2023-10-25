package com.practice.kafkaprogramming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerClient.class);
  public static void main(String[] args) {

    LOGGER.info("Producer Started.....");

    // server properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

    // serializer key and value properties
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());

    // Create a producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // Record to produce
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("first_topic", "name", "Shubhendu");

    // send data
    producer.send(producerRecord);

    // Tell the producer to send all data and block until done - synchronous
    producer.flush();

    // flush and close the producer
    producer.close();
  }
}
