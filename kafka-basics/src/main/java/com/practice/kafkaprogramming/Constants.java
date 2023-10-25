package com.practice.kafkaprogramming;

public class Constants {
   private Constants() {
      throw new IllegalStateException("Constant internal class");
   }

   public static final String SERVER_NAME = "bootstrap.servers";
   public static final String URL_PORT = "127.0.0.1:9092";
   public static final String KEY_SERIALIZER = "key.serializer";
   public static final String VALUE_SERIALIZER = "value.serializer";
   public static final String KAFKA_TOPIC = "first_topic";
}
