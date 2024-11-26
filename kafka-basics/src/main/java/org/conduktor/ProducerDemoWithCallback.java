package org.conduktor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Hello world!");
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181"); // connect to localhost
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("security.protocol", "PLAINTEXT");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");
        properties.setProperty("batch.size", "400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "key" + i, "helloWorld " + i);
                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record successfully sent or an exception is thrown
                        if (e == null)
                            log.info("Successfully sent message\nTopic: {}\nKey: {}\nPartition: {}\nOffset: {}\nTimestamp: {}"
                                    , recordMetadata.topic(), producerRecord.key(), recordMetadata.partition(),
                                    recordMetadata.offset(), recordMetadata.timestamp());
                        else {
                            log.error("Error while sending message", e);
                            // rollback
                        }
                    }
                });
            }
            Thread.sleep(500);
        }
        // tell the producer to send all data and block until done --sync
        producer.flush();
        // flush and close the producer
        producer.close();
    }
}