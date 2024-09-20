package org.example.globalstate.core;

public interface StorageBackend {
    void initialize();

    void put(String type, KeyValue kv);

    KeyValue get(String type, String key);
}
