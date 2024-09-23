package org.example.globalstate;

public class IdentityDbRegionSerializer implements DbRegionSerializer {
    @Override
    public KeyValue<byte[], byte[]> serialize(KeyValue keyValue) {
        return keyValue;
    }

    @Override
    public KeyValue deserialize(KeyValue keyValue) {
        return keyValue;
    }
}
