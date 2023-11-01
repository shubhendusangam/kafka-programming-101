package com.practice.kafkaprogramming;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import static com.practice.kafkaprogramming.Constants.*;
import static java.text.MessageFormat.*;

public class OpenSearchConsumer {
   private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchConsumer.class);
   public static RestHighLevelClient createOpenSearchClient() {

      // we build a URI from the connection string
      RestHighLevelClient restHighLevelClient;
      URI connUri = URI.create(OPENSEARCH_HOST);
      // extract login information if it exists
      String userInfo = connUri.getUserInfo();

      if (userInfo == null) {
         // REST client without security
         restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

      } else {
         // REST client with security
         String[] auth = userInfo.split(":");

         CredentialsProvider cp = new BasicCredentialsProvider();
         cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

         restHighLevelClient = new RestHighLevelClient(
               RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                     .setHttpClientConfigCallback(
                           httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                 .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

      }
      return restHighLevelClient;
   }

   private static String extractId(String json){
      // gson library
      return JsonParser.parseString(json)
            .getAsJsonObject()
            .get("meta")
            .getAsJsonObject()
            .get("id")
            .getAsString();
   }

   public static void main(String[] args) throws IOException {
      // first create an OpenSearch Client
      RestHighLevelClient openSearchClient = createOpenSearchClient();

      // create our Kafka Client
      KafkaConsumer<String, String> consumer = Consumer.getConsumer();

      // get a reference to the main thread
      final Thread mainThread = Thread.currentThread();

      // adding the shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         LOGGER.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
         consumer.wakeup();
         // join the main thread to allow the execution of the code in the main thread
         try {
            mainThread.join();
         } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
         }
      }));

      // we need to create the index on OpenSearch if it doesn't exist already
      try (openSearchClient; consumer) {
         boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(OPENSEARCH_INDEX), RequestOptions.DEFAULT);
         if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(OPENSEARCH_INDEX);
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            LOGGER.info("The Wikimedia Index has been created!");
         } else {
            LOGGER.info("The Wikimedia Index already exits");
         }

         // we subscribe the consumer
         consumer.subscribe(Collections.singleton(WIKIMEDIA_TOPIC));

         while (true) {
            sendDataToIndex(consumer, openSearchClient);
         }

      } catch (WakeupException e) {
         LOGGER.info("Consumer is starting to shut down");
      } catch (Exception e) {
         LOGGER.error("Unexpected exception in the consumer", e);
      } finally {
         consumer.close(); // close the consumer, this will also commit offsets
         openSearchClient.close();
         LOGGER.info("The consumer is now gracefully shut down");
      }
   }

   private static void sendDataToIndex(KafkaConsumer<String, String> consumer, RestHighLevelClient openSearchClient) throws IOException {
         ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));
         int recordCount = consumerRecords.count();
         LOGGER.info(format("Received {0} consumerRecords: ", recordCount));
         BulkRequest bulkRequest = new BulkRequest();
         for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            try {
               // strategy 2
               // we extract the ID from the JSON value
               String id = extractId(consumerRecord.value());
               IndexRequest indexRequest = new IndexRequest(OPENSEARCH_INDEX)
                     .source(consumerRecord.value(), XContentType.JSON)
                     .id(id);
               bulkRequest.add(indexRequest);
            } catch (Exception e) {
               LOGGER.error(e.getMessage());
            }
         }

         if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            LOGGER.info(format("Inserted {0} records.", bulkResponse.getItems().length));

            try {
               Thread.sleep(1000);
            } catch (InterruptedException e) {
               LOGGER.error(e.getMessage());
            }

            // commit offsets after the batch is consumed
            consumer.commitSync();
            LOGGER.info("Offsets have been committed!");
         }
      }
}
