package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.core.AbstractKafkaConsumerProducer;
import org.example.core.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class PartialAggregator extends AbstractKafkaConsumerProducer<String, String, String, String> {
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
    protected List<KeyValue<String, String>> transform(ConsumerRecords<String, String> records) {
        List<KeyValue<String, String>> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            result.add(new KeyValue<>(record.key(), record.value()));
        }
        return result;
    }
}
