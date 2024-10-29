package org.conduktor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // connect to localhost
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");  //consumer from beginning
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detecting shutting down...\n let's exit by calling consumer.wakeup()");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.warn("Main thread interrupted", e);
                    Thread.currentThread().interrupt();  // Clean up whatever needs to be handled before interrupting
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(List.of(topic)); //consumer from a list of topics

            // poll for data
            while (true) {
                log.info("Waiting for messages. Polling data...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    log.info("Key: {},  Value: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition: {},  Offsets: {}", consumerRecord.partition(), consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        } catch (Exception e) {
            log.error("Unexpected error in the consumer", e);
        } finally {
            consumer.close();   // close the consumer and also will commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }
}