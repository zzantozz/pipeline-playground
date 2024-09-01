package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;

public class DroppingDebatcher extends AbstractKafkaConsumerProducer<Void, String, String, String> {

    private final TopicBasedConfiguration<String, DropRule> dropRules;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DroppingDebatcher() {
        super(Ingestor.INGEST_TOPIC, PartialAggregator.INPUT_TOPIC, new IOSpec(
                StringDeserializer.class, StringDeserializer.class, StringSerializer.class, StringSerializer.class
        ));
        dropRules = new TopicBasedConfiguration<>("drop-rules", "need-a-unique-DD-id",
                StringDeserializer.class.getName(), DropRuleDeserializer.class.getName());
    }

    public static void main(String[] args) {
        DroppingDebatcher droppingDebatcher = new DroppingDebatcher();
        while (true) {
            droppingDebatcher.poll();
        }
    }

    @Override
    protected List<KeyValue<String, String>> transform(ConsumerRecords<Void, String> records) {
        List<KeyValue<String, String>> result = new ArrayList<>();
        for (ConsumerRecord<Void, String> record : records) {
            final DataPoint dp;
            try {
                dp = objectMapper.readValue(record.value(), DataPoint.class);
                if (dropRules.getValues().stream().anyMatch(r -> r.match(dp))) {
                    System.out.println("Dropping!");
                } else {
                    result.add(new KeyValue<>(null, record.value()));
                }
            } catch (JsonProcessingException e) {
                System.out.println("Failed to unmarshal message, dropping: " + record.value());
            }
        }
        return result;
    }
}
