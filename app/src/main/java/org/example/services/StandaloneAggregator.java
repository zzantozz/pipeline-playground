package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.core.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StandaloneAggregator {

    private final KafkaConsumer<String, Envelope> consumer;
    private final Map<Window, Lag> lagsByWindows = new HashMap<>();

    public static void main(String[] args) {
        new StandaloneAggregator().run();
    }

    public StandaloneAggregator() {
        consumer = getConsumer();
    }

    private KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, Envelope> getConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        props.setProperty("group.id", "app-consumer-" + getClass().getSimpleName() + "-" + UUID.randomUUID());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", EnvelopeDeserializer.class.getName());
        KafkaConsumer<String, Envelope> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(PartialAggregator.INPUT_TOPIC));
        return consumer;
    }

    public void run() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(windowCompletionTrigger(), 2, 2, TimeUnit.SECONDS);
        while (true) {
            ConsumerRecords<String, Envelope> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Envelope> record : records) {
                for (TelemetryQuery query : record.value().getAssociatedQueries()) {
                    Aggregation aggregation = getAggregationForQuery(query);
                    aggregate(query, aggregation, record.value().getDataPoint());
                }
            }
        }
    }

    private Runnable windowCompletionTrigger() {
        return () -> {
            try {
                closeEligibleWindows();
            } catch (Exception e) {
                System.err.println("Error in window close!");
                e.printStackTrace();
            }
//            watermarkCalculator.getWatermarks().entrySet().stream().forEach((entry) -> {
//                String timeSeries = entry.getKey();
//                Watermark watermark = entry.getValue();
//                windowRegistry.closeWindows(timeSeries, watermark);
//            });
        };
    }

    public static final int WINDOW_DURATION = 10000;
    private Map<String, List<Window>> windows = new HashMap<>();
    private final AtomicInteger windowId = new AtomicInteger();

    public Window getOrCreateWindow(DataPoint dataPoint, Aggregation aggregation, long eventTimestamp) {
        final var mapKey = getMapKey(dataPoint, aggregation);
        final var windowStart = eventTimestamp - (eventTimestamp % WINDOW_DURATION);
        final var candidates = windows.computeIfAbsent(mapKey, k -> new ArrayList<>());
        Optional<Window> first = candidates
                .stream()
                .filter(w -> w.getStartMillis() == windowStart)
                .findFirst();
        if (first.isPresent()) {
            return first.get();
        } else {
            final var now = System.currentTimeMillis();
            Window window = new Window(
                    Integer.toString(windowId.incrementAndGet()),
                    windowStart,
                    windowStart + WINDOW_DURATION,
                    now,
                    now + WINDOW_DURATION);
            window.setAggregation(aggregation);
            candidates.add(window);
            System.out.println("New window: " + window);
            return window;
        }
    }

    private static String getMapKey(DataPoint dataPoint, Aggregation aggregation) {
        return dataPoint.getName() + aggregation;
    }

    public void closeEligibleWindows() {
        final var now = System.currentTimeMillis();
        for (Map.Entry<String, List<Window>> entry : windows.entrySet()) {
            for (Window window : entry.getValue()) {
                Lag lag = getOrCreateLag(window);
                if (window.getStatus() == Window.Status.OPEN && now > window.getCloseTime() + lag.getAverageLag()) {
                    System.out.printf("Close: %s value=%d now=%d end=%d lag=%.3f%n",
                            window.getId(), window.getValue(), now, window.getEndMillis(), window.getAverageLag());
                    window.setStatus(Window.Status.CLOSED);
                }
            }
        }
    }

    private void aggregate(TelemetryQuery query, Aggregation aggregation, DataPoint dataPoint) {
        Window window = getOrCreateWindow(dataPoint, aggregation, dataPoint.getTimestamp());
        if (window.getStatus() == Window.Status.CLOSED) {
            System.out.printf("Missed window: %s - %d !<> %d,%d%n",
                    window.getId(), dataPoint.getTimestamp(), window.getStartMillis(), window.getEndMillis());
//            ok, here's where lag becomes significant. i can use missed windows to start figuring lag across the time
//            series and keep windows open longer
            long lag = dataPoint.getTimestamp() - window.getEndMillis();
            if (lag <= 0) {
                System.out.printf("Missed window with negative lag: %s - %d. What do I do?%n", window.getId(), lag);
            } else {
                Lag lagThing = getOrCreateLag(window);
                lagThing.update(dataPoint, window.getEndMillis());
            }
        } else {
            window.aggregate(dataPoint);
        }
    }

    private Lag getOrCreateLag(Window window) {
        return lagsByWindows.computeIfAbsent(window, k -> {
            Lag result = new Lag();
            return result;
        });
    }

    private Aggregation getAggregationForQuery(TelemetryQuery query) {
        // At least some queries should have one or more aggregation functions. What if there are zero? What if there
        // are more than one? For now, just a placeholder to get things moving.
        return Aggregation.SUM;
    }
}
