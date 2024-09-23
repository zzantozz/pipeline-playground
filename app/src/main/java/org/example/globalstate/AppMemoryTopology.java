package org.example.globalstate;

import java.util.Map;

public class AppMemoryTopology {
    private Map<String, Map> mappings;

    public AppMemoryTopology(Map<String, Map> mappings) {
        this.mappings = mappings;
    }

    public Map get(String region) {
        return mappings.get(region);
    }
}
