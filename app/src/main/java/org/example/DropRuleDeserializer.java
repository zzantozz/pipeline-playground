package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DropRuleDeserializer implements Deserializer<DropRule> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public DropRule deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), DropRule.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize a drop rule", e);
        }
    }
}
