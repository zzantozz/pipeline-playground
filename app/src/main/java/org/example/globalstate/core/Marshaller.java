package org.example.globalstate.core;

import java.util.Map;

public interface Marshaller {
    KeyValue marshal(Map m);

    Object unmarshal(KeyValue kv);
}
