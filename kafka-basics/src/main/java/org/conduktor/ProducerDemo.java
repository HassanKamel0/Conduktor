package org.conduktor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm Kafka Producer!");
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // connect to localhost
        properties.setProperty("security.protocol", "PLAINTEXT");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> producerRecord=
                new ProducerRecord<>("demo_java","hello_world");
        // send data
        producer.send(producerRecord);
        // tell the producer to send all data and block until done --sync
        producer.flush();
        // flush and close the producer
        producer.close();
    }
}