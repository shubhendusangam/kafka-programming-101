package com.practice.kafkaprogramming;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerClientWithCallBack {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerClientWithCallBack.class);
  public static void main(String[] args) {

    LOGGER.info("Producer Call back Started.....");

    // server properties
    Properties properties = new Properties();
    properties.setProperty(Constants.SERVER_NAME, Constants.URL_PORT);

    // serializer key and value properties
    properties.setProperty(Constants.KEY_SERIALIZER, StringSerializer.class.getName());
    properties.setProperty(Constants.VALUE_SERIALIZER, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = getKafkaProducer(properties);

    // Tell the producer to send all data and block until done - synchronous
    producer.flush();

    // flush and close the producer
    producer.close();
  }

  private static KafkaProducer<String, String> getKafkaProducer(Properties properties) {
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // Record to produce
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(Constants.KAFKA_TOPIC, "programming", "Java | Python | Node.Js");

    // send data
    producer.send(producerRecord,
          (recordMetadata, e) -> {
            if (e == null) {
              LOGGER.info(MessageFormat.format("record meta data :\n Topic : {0}\n Partition : {1}\n Offset : {2}\n Time Stamp: {3}",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), LocalDateTime.now()));
            }
          });
    return producer;
  }
}
