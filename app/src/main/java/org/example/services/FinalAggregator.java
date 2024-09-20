package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.core.AbstractKafkaConsumerProducer;
import org.example.core.KeyValue;

import java.util.List;

public class FinalAggregator extends AbstractKafkaConsumerProducer {
    public static final String INPUT_TOPIC = "correlated-partial-windows";
    public static final String OUTPUT_TOPIC = "end-of-line";

    public FinalAggregator() {
        super(INPUT_TOPIC, OUTPUT_TOPIC, new IOSpec(
                StringDeserializer.class, StringDeserializer.class, StringSerializer.class, StringSerializer.class
        ));
    }

    public static void main(String[] args) {
        FinalAggregator finalAggregator = new FinalAggregator();
        while (true) {
            finalAggregator.poll();
        }
    }

    @Override
    protected List<KeyValue> transform(ConsumerRecords records) {
        return List.of();
    }
}
