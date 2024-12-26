package org.conduktor;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


@Slf4j
@RequiredArgsConstructor
public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    @Override
    public void onOpen() {
        // nothing here when the stream is open
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        // async
        log.info("On message streaming: {}", messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) {
        // nothing
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading: {}", throwable.getMessage());
    }
}
