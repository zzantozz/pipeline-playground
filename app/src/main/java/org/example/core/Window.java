package org.example.core;

public class Window {
    private final String id;
    private final long startMillis;
    private final long endMillis;
    private final long createTime;
    private final long closeTime;
    private Aggregation aggregation;
    private long value;
    private final Lag lag;
    public enum Status { OPEN, CLOSED };
    private Status status = Status.OPEN;

    public Window(String id, long startMillis, long endMillis, long createTime, long closeTime) {
        this.id = id;
        this.startMillis = startMillis;
        this.endMillis = endMillis;
        this.createTime = createTime;
        this.closeTime = closeTime;
        lag = new Lag();
    }

    public String getId() {
        return id;
    }

    public long getStartMillis() {
        return startMillis;
    }

    public long getEndMillis() {
        return endMillis;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getCloseTime() {
        return closeTime;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void aggregate(DataPoint dataPoint) {
        if (status == Status.CLOSED) {
            throw new IllegalStateException("Can't aggregate in a closed window");
        }
        value = aggregation.aggregate(value, dataPoint);
        lag.update(dataPoint, endMillis);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Window{" +
                "id=" + id +
                ", startMillis=" + startMillis +
                ", endMillis=" + endMillis +
                ", createTime=" + createTime +
                ", closeTime=" + closeTime +
                ", aggregation=" + aggregation +
                ", value=" + value +
                '}';
    }

    public double getAverageLag() {
        return lag.getAverageLag();
    }
}
