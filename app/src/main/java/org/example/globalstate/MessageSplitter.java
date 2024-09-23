package org.example.globalstate;

import java.util.List;

public interface MessageSplitter {
    List<StorageOperation> split(KeyValue keyValue);
}
