package org.example.globalstate;

import java.util.HashMap;
import java.util.Map;

import static org.example.globalstate.GlobalStateStore.DEFAULT_REGION;

public class InMemoryStorageProvider implements StorageProvider {
    private Map<String, Map<String, byte[]>> storage = new HashMap<>();

    @Override
    public void put(byte[] key, byte[] value) {
        put(DEFAULT_REGION, key, value);
    }

    @Override
    public void put(String region, byte[] key, byte[] value) {
        storage.computeIfAbsent(region, n -> new HashMap<>())
                // Put String keys, not byte[] keys. The latter don't have meaningful hash codes!
                .put(new String(key), value);
    }

    @Override
    public byte[] get(byte[] key) {
        return get(DEFAULT_REGION, key);
    }

    public byte[] get(String region, byte[] key) {
        Map<String, byte[]> regionStorage = storage.get(region);
        if (regionStorage == null) {
            return null;
        }
        byte[] bytes = regionStorage.get(new String(key));
        return bytes;
    }

    @Override
    public void remove(String region, byte[] key) {
        Map<String, byte[]> regionStorage = storage.get(region);
        if (regionStorage != null) {
            regionStorage.remove(new String(key));
        }
    }

    @Override
    public boolean containsKey(byte[] key) {
        String region = DEFAULT_REGION;
        return containsKey(region, key);
    }

    @Override
    public boolean containsKey(String region, byte[] key) {
        return storage.get(region).containsKey(new String(key));
    }
}
