package org.example.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.example.FinalAggregator;
import org.example.Ingestor;
import org.example.PartialAggregator;

import java.util.List;
import java.util.Properties;

public class DeleteTopics {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC, PartialAggregator.INPUT_TOPIC, FinalAggregator.INPUT_TOPIC,
                FinalAggregator.OUTPUT_TOPIC, "__consumer_offsets");
        try (Admin admin = Admin.create(props)) {
            admin.deleteTopics(topicNames);
        }
    }
}
