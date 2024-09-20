package org.example.globalstate.core;

import java.util.HashMap;
import java.util.Map;

public class InMemoryStorageBackend implements StorageBackend {
    private final Map<String, Map<Object, Object>> storage = new HashMap<>();
    @Override
    public void initialize() {

    }

    @Override
    public void put(String type, KeyValue kv) {
        Map<Object, Object> typeStorage = storage.computeIfAbsent(type, t -> new HashMap<>());
        typeStorage.put(kv.key(), kv.value());
    }

    @Override
    public KeyValue get(String type, String key) {
        Map<Object, Object> typeStorage = storage.computeIfAbsent(type, t -> new HashMap<>());
        return new KeyValue(key, typeStorage.get(key));
    }
}
