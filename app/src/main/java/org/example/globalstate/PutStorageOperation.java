package org.example.globalstate;

public record PutStorageOperation(String region, KeyValue keyValue) implements StorageOperation {
    @Override
    public Type op() {
        return Type.PUT;
    }
}
