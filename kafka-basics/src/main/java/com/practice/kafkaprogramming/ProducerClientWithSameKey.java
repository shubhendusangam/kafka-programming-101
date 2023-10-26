package com.practice.kafkaprogramming;

import java.text.MessageFormat;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;

public class ProducerClientWithSameKey {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerClientWithSameKey.class);

  public static void main(String[] args) {

    LOGGER.info("Producer Call back Started.....");

    // server properties
    Properties properties = new Properties();
    properties.setProperty(Constants.SERVER_NAME, Constants.URL_PORT);

    // serializer key and value properties
    properties.setProperty(Constants.KEY_SERIALIZER, StringSerializer.class.getName());
    properties.setProperty(Constants.VALUE_SERIALIZER, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    produceMessages(producer);

    // Tell the producer to send all data and block until done - synchronous
    producer.flush();

    // flush and close the producer
    producer.close();
  }

  // same key always go to same partition in topic
  private static void produceMessages(KafkaProducer<String, String> producer) {
    for (int i = 0; i < 2; i++) {
      LOGGER.info(MessageFormat.format("batch started {0}", i));
      for (int j = 0; j < 10; j++) {
        String key = "Student_" + j;
        String value = new Faker().name().firstName();

        // Record to produce
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(Constants.KAFKA_TOPIC, key, value);

        // send data
        producer.send(
            producerRecord,
            (recordMetadata, e) -> {
              if (e == null) {
                LOGGER.info(
                    MessageFormat.format(
                        "Topic : {0}\n Partition : {1}\n Key : {2}\n value : {3}\n",
                        recordMetadata.topic(), recordMetadata.partition(), key, value));
              } else {
                LOGGER.error("error occurred%s".formatted(e.getMessage()));
              }
            });
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage());
      }
    }
  }
}
