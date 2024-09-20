package org.example.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.example.core.Settings;
import org.example.services.ControlApi;
import org.example.services.FinalAggregator;
import org.example.services.Ingestor;
import org.example.services.PartialAggregator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CreateTopics {

    public static final int PARTITION_COUNT = 128;
    public static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.KAFKA_HOST + ":9092");
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC,
                PartialAggregator.INPUT_TOPIC,
                FinalAggregator.INPUT_TOPIC,
                FinalAggregator.OUTPUT_TOPIC);
        List<String> compactedTopicNames = List.of(
                ControlApi.DROP_RULES_TOPIC,
                ControlApi.QUERIES_TOPIC);
        Map<String, String> compactedTopicConfig = Collections.singletonMap(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        );
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(topicNames.stream()
                    .map((n) -> new NewTopic(n, PARTITION_COUNT, REPLICATION_FACTOR))
                    .toList());
            admin.createTopics(compactedTopicNames.stream()
                    .map((n) -> new NewTopic(n, PARTITION_COUNT, REPLICATION_FACTOR).configs(compactedTopicConfig))
                    .toList());
        }
    }
}
