package org.example.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.core.DropRule;
import org.example.core.Settings;
import org.example.core.TelemetryQuery;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class ControlApi {
    public static final String INSERT_DROP_RULE_SQL = "insert into drop_rule (value) values (?)";
    public static final String INSERT_QUERY_SQL = "insert into query (value) values (?)";
    public static final String DROP_RULES_TOPIC = "drop-rule-updates";
    public static final String QUERIES_TOPIC = "query-updates";
    private static final Logger log = LoggerFactory.getLogger(ControlApi.class);
    private static HttpServer server;
    private static PGConnectionPoolDataSource dataSource;

    public static void main(String[] args) throws Exception {
        dataSource = new PGConnectionPoolDataSource();
        dataSource.setServerNames(new String[]{Settings.POSTGRES_HOST});
        dataSource.setUser("postgres");
        dataSource.setPassword("admin");

        KafkaProducer<String, String> producer = getProducer();

        server = HttpServer.create(new InetSocketAddress(8001), 0);
        final ObjectMapper objectMapper = new ObjectMapper();

        server.createContext("/v2/dropRules", t -> {
            int code;
            String message;
            final String rawBody = IOUtils.toString(t.getRequestBody(), StandardCharsets.UTF_8);
            try {
                DropRule dropRule = objectMapper.readValue(rawBody, DropRule.class);
                if (Strings.isNullOrEmpty(dropRule.getId())) {
                    code = 400;
                    message = "Drop rule must have an ID that uniquely identifies it.";
                } else {
                    String json = objectMapper.writeValueAsString(dropRule);
                    producer.send(new ProducerRecord<>(DROP_RULES_TOPIC, dropRule.getId(), json));
                    code=202;
                    message="Drop rule queued";
                }
            } catch (IOException e) {
                code = 500;
                StringWriter messageWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(messageWriter);
                writer.println("Failed to process rule: " + rawBody);
                e.printStackTrace(writer);
                writer.close();
                message = messageWriter.toString();
            }
            t.sendResponseHeaders(code, message.length());
            final OutputStream os = t.getResponseBody();
            os.write(message.getBytes());
            os.close();
        });

        server.createContext("/v2/queries", t -> {
            int code;
            String message;
            final String rawBody = IOUtils.toString(t.getRequestBody(), StandardCharsets.UTF_8);
            try {
                TelemetryQuery query = objectMapper.readValue(rawBody, TelemetryQuery.class);
                if (Strings.isNullOrEmpty(query.getId())) {
                    code = 400;
                    message = "Query must have an ID that uniquely identifies it.";
                } else {
                    String json = objectMapper.writeValueAsString(query);
                    producer.send(new ProducerRecord<>(QUERIES_TOPIC, query.getId(), json));
                    code=202;
                    message="Query queued";
                }
            } catch (JsonProcessingException e) {
                code = 500;
                StringWriter messageWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(messageWriter);
                writer.println("Failed to process query: " + rawBody);
                e.printStackTrace(writer);
                writer.close();
                message = messageWriter.toString();
            }
            t.sendResponseHeaders(code, message.length());
            final OutputStream os = t.getResponseBody();
            os.write(message.getBytes());
            os.close();
        });

        server.createContext("/dropRules", t -> {
            final String value = IOUtils.toString(t.getRequestBody(), StandardCharsets.UTF_8);
            final OutputStream os = t.getResponseBody();
            int code;
            String message;
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(INSERT_DROP_RULE_SQL)) {
                stmt.setString(1, value);
                int result = stmt.executeUpdate();
                if (result != 1) {
                    log.warn("Inserting row didn't mutate database");
                }
                code = 200;
                message = "Insert success";
            } catch (SQLException e) {
                code = 500;
                StringWriter messageWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(messageWriter);
                writer.println("Insert failed\n");
                e.printStackTrace(writer);
                writer.close();
                message = messageWriter.toString();
            }
            t.sendResponseHeaders(code, message.length());
            os.write(message.getBytes());
            os.close();
        });
        server.createContext("/queries", t -> {
            final String value = IOUtils.toString(t.getRequestBody(), StandardCharsets.UTF_8);
            final OutputStream os = t.getResponseBody();
            int code;
            String message;
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(INSERT_QUERY_SQL)) {
                stmt.setString(1, value);
                int result = stmt.executeUpdate();
                if (result != 1) {
                    log.warn("Inserting row didn't mutate database");
                }
                code = 200;
                message = "Insert success";
            } catch (SQLException e) {
                code = 500;
                StringWriter messageWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(messageWriter);
                writer.println("Insert failed\n");
                e.printStackTrace(writer);
                writer.close();
                message = messageWriter.toString();
            }
            t.sendResponseHeaders(code, message.length());
            os.write(message.getBytes());
            os.close();
        });
        new Thread(() -> server.start(), "HTTP Server").start();
    }

    private static KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Settings.KAFKA_HOST + ":9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
