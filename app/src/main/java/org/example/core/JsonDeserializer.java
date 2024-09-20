package org.example.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonDeserializer<T> implements Deserializer<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), clazz);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize a drop rule", e);
        }
    }
}
