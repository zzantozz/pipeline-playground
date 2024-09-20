package org.example.core;

import java.util.List;

public class Envelope {
    private DataPoint dataPoint;
    private List<TelemetryQuery> associatedQueries;
    private long enteredDroppingDebatcherMillis;

    public DataPoint getDataPoint() {
        return dataPoint;
    }

    public void setDataPoint(DataPoint dataPoint) {
        this.dataPoint = dataPoint;
    }

    public List<TelemetryQuery> getAssociatedQueries() {
        return associatedQueries;
    }

    public void setAssociatedQueries(List<TelemetryQuery> associatedQueries) {
        this.associatedQueries = associatedQueries;
    }

    public long getEnteredDroppingDebatcherMillis() {
        return enteredDroppingDebatcherMillis;
    }

    public void setEnteredDroppingDebatcherMillis(long enteredDroppingDebatcherMillis) {
        this.enteredDroppingDebatcherMillis = enteredDroppingDebatcherMillis;
    }
}
