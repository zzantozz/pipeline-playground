package org.example.globalstate;

public interface DbRegionSerializer {
    KeyValue<byte[], byte[]> serialize(KeyValue keyValue);
    KeyValue deserialize(KeyValue keyValue);
}
