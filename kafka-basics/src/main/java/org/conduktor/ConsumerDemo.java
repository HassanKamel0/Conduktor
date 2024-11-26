package org.conduktor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm Kafka Consumer!");

        String groupId = "my-wikimedia-application";
        String topic = "wikimedia_recentChange";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // connect to localhost
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");  //consumer from beginning

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe to a topic
        consumer.subscribe(List.of(topic)); //consumer from a list of topics

        // poll for data
        while (true) {
            log.info("Waiting for messages. Beginning data received...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : records) {
                log.info("Key: {},  Value: {}", consumerRecord.key(), consumerRecord.value());
                log.info("Partition: {},  Offsets: {}", consumerRecord.partition(), consumerRecord.offset());
            }
        }
    }
}