package org.example.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DroppingAndFiltering extends AbstractKafkaConsumerProducer<String, String, String, String> {

    private final TopicBasedConfiguration<String, DropRule> dropRules;
    private final TopicBasedConfiguration<String, TelemetryQuery> queries;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DataPointArchiver dataPointArchiver = new DataPointArchiver();

    public DroppingAndFiltering() {
        super(Ingestor.INGEST_TOPIC, PartialAggregator.INPUT_TOPIC, new IOSpec(
                StringDeserializer.class, StringDeserializer.class, StringSerializer.class, StringSerializer.class
        ));
        dropRules = new TopicBasedConfiguration<>(ControlApi.DROP_RULES_TOPIC, "drop-rules-for-DD-XXX",
                StringDeserializer.class.getName(), DropRuleDeserializer.class.getName());
        queries = new TopicBasedConfiguration<>(ControlApi.QUERIES_TOPIC, "queries-for-DD-XXX",
                StringDeserializer.class.getName(), TelemetryQueryDeserializer.class.getName());
    }

    public static void main(String[] args) {
        DroppingAndFiltering droppingAndFiltering = new DroppingAndFiltering();
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(droppingAndFiltering.dropRules);
        executorService.submit(droppingAndFiltering.queries);
        while (true) {
            droppingAndFiltering.poll();
        }
    }

    @Override
    protected List<KeyValue<String, String>> transform(ConsumerRecords<String, String> records) {
        List<KeyValue<String, String>> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            final String value = record.value();
            try {
                final DataPoint dp = objectMapper.readValue(value, DataPoint.class);
                // Only archive valid things
                dataPointArchiver.archive(dp, value);
                List<DropRule> matchingDropRules = dropRules.getValues().stream().filter(r -> r.match(dp)).toList();
                if (!matchingDropRules.isEmpty()) {
                    System.out.println("Dropping due to matching drop rules: " + matchingDropRules);
                } else {
                    List<TelemetryQuery> matchingQueries = queries.getValues().stream().filter(q -> q.match(dp)).toList();
                    if (matchingQueries.isEmpty()) {
                        System.out.println("No queries, not passing to aggregation: " + dp);
                    } else {
                        Envelope envelope = new Envelope();
                        envelope.setDataPoint(dp);
                        envelope.setAssociatedQueries(matchingQueries);
                        result.add(new KeyValue<>(null, objectMapper.writeValueAsString(envelope)));
                    }
                }
            } catch (JsonProcessingException e) {
                System.out.println("Failed to unmarshal message, dropping: " + value);
            }
        }
        return result;
    }
}
