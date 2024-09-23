package org.example.globalstate;

import com.fasterxml.jackson.annotation.ObjectIdGenerator;

public interface StorageOperation {
    String region();

    KeyValue keyValue();

    Type op();

    enum Type { PUT, REMOVE }
}
