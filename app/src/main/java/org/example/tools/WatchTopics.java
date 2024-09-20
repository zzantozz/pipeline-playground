package org.example.tools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.core.Settings;
import org.example.services.ControlApi;
import org.example.services.FinalAggregator;
import org.example.services.Ingestor;
import org.example.services.PartialAggregator;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WatchTopics {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        props.setProperty("group.id", "topic-watcher");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC,
                PartialAggregator.INPUT_TOPIC,
                FinalAggregator.INPUT_TOPIC,
                FinalAggregator.OUTPUT_TOPIC,
                ControlApi.DROP_RULES_TOPIC,
                ControlApi.QUERIES_TOPIC);
        consumer.subscribe(topicNames);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("%s:%d %s=%s%n", record.topic(), record.offset(), record.key(), record.value());
        }
    }
}
