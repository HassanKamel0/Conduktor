package org.conduktor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181"); // connect to localhost
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "new_java";
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                String key = "Key" + i;
                String value = "hello_world " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Key: {} | Partition: {}", key, recordMetadata.partition());
                        } else
                            log.error("Error while sending message", e);
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.warn("Main thread interrupted", e);
                Thread.currentThread().interrupt();  // Clean up whatever needs to be handled before interrupting
            }
        }
        // tell the producer to send all data and block until done --sync
        producer.flush();
        // flush and close the producer
        producer.close();
    }
}