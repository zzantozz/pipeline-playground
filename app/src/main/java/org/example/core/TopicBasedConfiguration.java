package org.example.core;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class TopicBasedConfiguration<K, V> implements Runnable {

    private final KafkaConsumer<K, V> consumer;
    private final Map<K, V> configuration = new HashMap<>();

    public TopicBasedConfiguration(String topicName, String description, String keyDeserializer, String valueDeserializer) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        // Use a random group id to always consume the entire topic at startup, building the configuration.
        props.setProperty("group.id", "config-consumer-" + description + UUID.randomUUID());
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", keyDeserializer);
        props.setProperty("value.deserializer", valueDeserializer);
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topicName));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<K, V> record : records) {
                if (record.value() == null) {
                    configuration.remove(record.key());
                } else {
                    configuration.put(record.key(), record.value());
                }
            }
        }
    }

    public V get(K key) {
        return configuration.get(key);
    }

    public Collection<V> getValues() {
        return configuration.values();
    }
}
