package com.practice.kafkaprogramming;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerClient.class);

  public static void main(String[] args) {

    LOGGER.info("Consumer Started.....");

    // Create a consumer and poll data
    try (KafkaConsumer<String, String> consumer = Consumer.getConsumer()) {
      final Thread thread = Thread.currentThread();

      // adding shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOGGER.info("Detected a shutdown, call consumer.wakeup()");
        consumer.wakeup();

        // join the main thread to allow the execution of the code in the main thread
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage());
        }
      }));

      // subscribe to a topic
      consumer.subscribe(List.of(Constants.KAFKA_TOPIC));

      try {
        while (true) {
          LOGGER.info("fetching data....");
          ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            LOGGER.info(MessageFormat.format("Key {0}\nvalue {1}\ntopic {2}\noffset {3}", consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.offset()));
          }
        }
      } catch (WakeupException e) {
        LOGGER.info("consumer is starting to shutdown");
      } catch (Exception e) {
        LOGGER.error("Unexpected exception in consumer {}" , e.getMessage());
      }
    }
  }
}
