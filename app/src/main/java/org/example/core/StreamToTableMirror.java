package org.example.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public abstract class StreamToTableMirror {
    private final String topicName;
    private final String description;

    public StreamToTableMirror(String topicName, String description) {
        this.topicName = topicName;
        this.description = description;
    }

    public void run() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        props.setProperty("group.id", getClass().getSimpleName() + "-" + description);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        PGConnectionPoolDataSource dataSource = new PGConnectionPoolDataSource();
        dataSource.setServerNames(new String[]{Settings.POSTGRES_HOST});
        dataSource.setUser("postgres");
        dataSource.setPassword("admin");

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(getSql());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        addRecordToStatement(record, stmt);
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            System.err.println("DB connection failed");
            e.printStackTrace();
            throw new RuntimeException("DB connection failed", e);
        }
    }

    protected abstract void addRecordToStatement(ConsumerRecord<String, String> record, PreparedStatement stmt) throws SQLException;

    protected abstract String getSql();
}
