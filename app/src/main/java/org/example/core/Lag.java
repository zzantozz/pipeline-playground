package org.example.core;

public class Lag {
    private long lagSum;
    private long count;
    private double lagStandardDeviationAcc = 0.0;

    public void update(DataPoint dataPoint, long windowEndMillis) {
        final var eventTime = dataPoint.getTimestamp();
        var lag = eventTime - windowEndMillis;
        if (lag > 0) {
            lagSum += lag;
        }
        count++;
        double mean = getAverageLag();
        lagStandardDeviationAcc += Math.pow(dataPoint.getValue() - mean, 2);
    }

    public double getStandardDeviation() {
        return Math.sqrt(lagStandardDeviationAcc / count);
    }

//    public long estimateWatermark() {
//        double avgLag = getAverageLag();
//        long l = (long) (window.getEndMillis() + avgLag);
//        return l;
//    }

    public double getAverageLag() {
        if (count == 0) {
            return 0;
        }
        return lagSum / (double) count;
    }
}
