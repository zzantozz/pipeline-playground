package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;

public class PartialAggregator extends AbstractKafkaConsumerProducer {
    public static final String INPUT_TOPIC = "random-filtered-data";

    public PartialAggregator() {
        super(INPUT_TOPIC, FinalAggregator.INPUT_TOPIC, new IOSpec(
                StringDeserializer.class, StringDeserializer.class, StringSerializer.class, StringSerializer.class
        ));
    }

    public static void main(String[] args) {
        PartialAggregator partialAggregator = new PartialAggregator();
        while (true) {
            partialAggregator.poll();
        }
    }

    @Override
    protected List<KeyValue> transform(ConsumerRecords records) {
        return List.of();
    }
}
