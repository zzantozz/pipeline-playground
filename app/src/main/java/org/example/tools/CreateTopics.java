package org.example.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.example.FinalAggregator;
import org.example.Ingestor;
import org.example.PartialAggregator;

import java.util.List;
import java.util.Properties;

public class CreateTopics {

    public static final int PARTITION_COUNT = 128;
    public static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        List<String> topicNames = List.of(
                Ingestor.INGEST_TOPIC, PartialAggregator.INPUT_TOPIC, FinalAggregator.INPUT_TOPIC, FinalAggregator.OUTPUT_TOPIC);
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(topicNames.stream()
                    .map((n) -> new NewTopic(n, PARTITION_COUNT, REPLICATION_FACTOR))
                    .toList());
        }
    }
}
