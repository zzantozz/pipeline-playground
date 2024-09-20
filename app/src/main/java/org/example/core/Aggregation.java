package org.example.core;

public enum Aggregation {
    SUM;

    public long aggregate(long value, DataPoint dataPoint) {
        if (this == SUM) {
            return value + dataPoint.getValue();
        } else {
            throw new IllegalStateException("Invalid aggregation!");
        }
    }
}
