package org.example.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.example.core.Settings;
import org.example.services.ControlApi;
import org.example.services.FinalAggregator;
import org.example.services.Ingestor;
import org.example.services.PartialAggregator;

import java.util.List;
import java.util.Properties;

public class DeleteTopics {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.KAFKA_HOST + ":9092");
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC,
                PartialAggregator.INPUT_TOPIC,
                FinalAggregator.INPUT_TOPIC,
                FinalAggregator.OUTPUT_TOPIC,
                ControlApi.DROP_RULES_TOPIC,
                ControlApi.QUERIES_TOPIC,
                "__consumer_offsets");
        try (Admin admin = Admin.create(props)) {
            admin.deleteTopics(topicNames);
        }
    }
}
