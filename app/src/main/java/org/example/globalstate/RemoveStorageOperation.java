package org.example.globalstate;

public record RemoveStorageOperation(String region, KeyValue keyValue) implements StorageOperation {
    @Override
    public Type op() {
        return Type.REMOVE;
    }
}
