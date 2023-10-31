package com.practice.kafkaprogramming;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;

public class WikiMediaChangesProducer {

   public static void main(String[] args) throws InterruptedException {
      KafkaProducer<String, String> kafkaProducer = Producer.getProducer();

      EventHandler eventHandler = new WikiMediaChangeHandler(kafkaProducer, Constants.WIKIMEDIA_TOPIC);
      EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(Constants.WIKIMEDIA_URL)).build();

      // will create a separate thread
      eventSource.start();

      // we produce events for 10min and block the program until then
      TimeUnit.SECONDS.sleep(10);
   }
}
