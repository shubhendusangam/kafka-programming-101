package com.practice.kafkaprogramming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikiMediaChangeHandler implements EventHandler {

   private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangeHandler.class);

   private final KafkaProducer<String, String> producer;
   private final String topic;
   public WikiMediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String wikiMediaTopic) {
      this.producer = kafkaProducer;
      this.topic = wikiMediaTopic;
   }

   @Override
   public void onOpen() {
      // nothing to open
   }

   @Override
   public void onClosed() {
      producer.close();
   }

   @Override
   public void onMessage(String s, MessageEvent messageEvent) {
      LOGGER.info(messageEvent.getData());
      producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
   }

   @Override
   public void onComment(String s) {
      // nothing to open
   }

   @Override
   public void onError(Throwable throwable) {
      LOGGER.error("error while streaming data" + throwable.getMessage());
   }
}
