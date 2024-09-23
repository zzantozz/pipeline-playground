package org.example.globalstate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalStateStore {
    public static final String DEFAULT_REGION = "default";
    private StorageProvider storageProvider = new InMemoryStorageProvider();
    private MessageParser messageParser = (kv) -> kv;
    private AppMemoryTopology appMemoryTopology;
    private MessageSplitter messageSplitter;
    private final Map<String, DbRegionSerializer> regionSerializers = new HashMap<>();

    public GlobalStateStore(Map appMemory) {
        this(new AppMemoryTopology(Map.of(DEFAULT_REGION, appMemory)));
    }

    public GlobalStateStore(AppMemoryTopology appMemoryTopology) {
        this.appMemoryTopology = appMemoryTopology;
    }

    public void setMessageParser(MessageParser messageParser) {
        this.messageParser = messageParser;
    }

    public void setMessageSplitter(MessageSplitter messageSplitter) {
        this.messageSplitter = messageSplitter;
    }

    void ingest(byte[] key, byte[] value) {
        final KeyValue parsedMessage = messageParser.parse(new KeyValue<>(key, value));
        final List<StorageOperation> storageOperations;
        if (messageSplitter == null) {
            if (parsedMessage.value() == null) {
                storageOperations = List.of(new RemoveStorageOperation(DEFAULT_REGION, new KeyValue<>(key, null)));
            } else {
                storageOperations = List.of(new PutStorageOperation(DEFAULT_REGION, new KeyValue<>(key, value)));
            }
        } else {
            storageOperations = messageSplitter.split(parsedMessage);
        }
        for (StorageOperation storageOperation : storageOperations) {
            applyOperation(storageOperation, parsedMessage);
        }
    }

    private void applyOperation(StorageOperation storageOperation, KeyValue parsedMessage) {
        final String storageRegion = storageOperation.region();
        final Map appRegion = appMemoryTopology.get(storageRegion);
        final DbRegionSerializer dbRegionSerializer = regionSerializers.get(storageRegion);
        final KeyValue<byte[], byte[]> storableKv;
        final KeyValue<byte[], byte[]> appRegionKv;
        if (messageSplitter == null) {
            // If operating in simple mode, with just a message parser, then we simply deal with raw data in storage
            // and parsed objects in app memory.
            storableKv = storageOperation.keyValue();
            appRegionKv = parsedMessage;
        } else if (dbRegionSerializer == null) {
            throw new UnsupportedOperationException("You must register a DbRegionSerializer for region '" + storageRegion + "'");
        } else {
            storableKv = dbRegionSerializer.serialize(storageOperation.keyValue());
            appRegionKv = storageOperation.keyValue();
        }
        if (storageOperation.op() == StorageOperation.Type.REMOVE) {
            storageProvider.remove(storageRegion, storableKv.key());
            appRegion.remove(appRegionKv.key());
        } else if (storageOperation.op() == StorageOperation.Type.PUT) {
            storageProvider.put(storageRegion, storableKv.key(), storableKv.value());
            appRegion.put(appRegionKv.key(), appRegionKv.value());
        } else {
            throw new IllegalStateException("Unknown StorageOperation: '" + storageOperation.op() + "'");
        }
    }

    byte[] get(byte[] key) {
        return get(DEFAULT_REGION, key);
    }

    byte[] get(String region, byte[] key) {
        byte[] bytes = storageProvider.get(region, key);
        return bytes;
    }

    KeyValue getParsed(byte[] key) {
        return getParsed(DEFAULT_REGION, key);
    }

    KeyValue getParsed(String region, byte[] key) {
        final KeyValue keyValue = new KeyValue(key, get(region, key));
        final DbRegionSerializer dbRegionSerializer = regionSerializers.get(region);
        // Like on ingest, if we're in the default region with no serializer registered, it's a
        if (dbRegionSerializer == null) {
            return messageParser.parse(keyValue);
        } else {
            return dbRegionSerializer.deserialize(keyValue);
        }
    }

    void addRegion(String region, DbRegionSerializer dbRegionSerializer) {
        regionSerializers.put(region, dbRegionSerializer);
    }

    boolean containsKey(byte[] key) {
        return containsKey(DEFAULT_REGION, key);
    }

    boolean containsKey(String region, byte[] key) {
        return storageProvider.containsKey(region, key);
    }
}
