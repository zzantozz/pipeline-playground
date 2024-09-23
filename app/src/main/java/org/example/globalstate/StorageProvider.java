package org.example.globalstate;

public interface StorageProvider {
    void put(byte[] key, byte[] value);

    void put(String region, byte[] key, byte[] value);

    byte[] get(byte[] key);

    byte[] get(String region, byte[] key);

    void remove(String storageRegion, byte[] key);

    boolean containsKey(byte[] bytes);

    boolean containsKey(String region, byte[] key);
}
