package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.core.StreamToTableMirror;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseMirror {
    public static void main(String[] args) {
        StreamToTableMirror dropRulesMirror = new StreamToTableMirror(ControlApi.DROP_RULES_TOPIC, "control-api-drop-rules") {
            @Override
            protected void addRecordToStatement(ConsumerRecord<String, String> record, PreparedStatement stmt) throws SQLException {
                stmt.setString(1, record.key());
                stmt.setString(2, record.value());
                stmt.setString(3, record.value());
            }

            @Override
            protected String getSql() {
                return "insert into drop_rule (id, value) values (?, ?) on conflict (id) do update set value = ?";
            }
        };
        StreamToTableMirror queriesMirror = new StreamToTableMirror(ControlApi.QUERIES_TOPIC, "control-api-queries") {
            @Override
            protected void addRecordToStatement(ConsumerRecord<String, String> record, PreparedStatement stmt) throws SQLException {
                stmt.setString(1, record.key());
                stmt.setString(2, record.value());
                stmt.setString(3, record.value());
            }

            @Override
            protected String getSql() {
                return "insert into query (id, value) values (?, ?) on conflict (id) do update set value = ?";
            }
        };
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(dropRulesMirror::run);
        executorService.submit(queriesMirror::run);
    }
}
