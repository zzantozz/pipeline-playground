package org.example.tools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.FinalAggregator;
import org.example.Ingestor;
import org.example.PartialAggregator;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "basic-consumer-" + Consumer.class.getSimpleName());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC, PartialAggregator.INPUT_TOPIC, FinalAggregator.INPUT_TOPIC, FinalAggregator.OUTPUT_TOPIC);
        consumer.subscribe(topicNames);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("%s:%d %s=%s%n", record.topic(), record.offset(), record.key(), record.value());
        }

    }
}
