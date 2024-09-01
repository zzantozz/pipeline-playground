package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractKafkaConsumerProducer<IK, IV, OK, OV> {

    private final KafkaConsumer<IK, IV> consumer;
    private final KafkaProducer<OK, OV> producer;
    private final String inputTopic;
    private final String outputTopic;

    public AbstractKafkaConsumerProducer(String inputTopic, String outputTopic, IOSpec ioSpec) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        consumer = getConsumer(ioSpec);
        producer = getProducer(ioSpec);
    }

    public static class IOSpec {
        public final String inputKeyDeserializer;
        public final String inputValueDeserializer;
        public final String outputKeySerializer;
        public final String outputValueSerializer;

        public IOSpec(Class<? extends Deserializer> inputKeyDeserializer, Class<? extends Deserializer> inputValueDeserializer,
                      Class<? extends Serializer> outputKeySerializer, Class<? extends Serializer> outputValueSerializer) {
            this.inputKeyDeserializer = inputKeyDeserializer.getName();
            this.inputValueDeserializer = inputValueDeserializer.getName();
            this.outputKeySerializer = outputKeySerializer.getName();
            this.outputValueSerializer = outputValueSerializer.getName();
        }
    }

    private KafkaProducer<OK, OV> getProducer(IOSpec ioSpec) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", ioSpec.outputKeySerializer);
        props.put("value.serializer", ioSpec.outputValueSerializer);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<IK, IV> getConsumer(IOSpec ioSpec) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "app-consumer-" + getClass().getSimpleName());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", ioSpec.inputKeyDeserializer);
        props.setProperty("value.deserializer", ioSpec.inputValueDeserializer);
        KafkaConsumer<IK, IV> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(inputTopic));
        return consumer;
    }

    public void poll() {
        while (true) {
            ConsumerRecords<IK, IV> records = consumer.poll(Duration.ofMillis(100));
            List<KeyValue<OK, OV>> transformedRecords = transform(records);
            for (KeyValue<OK, OV> keyValue : transformedRecords) {
                final Future<RecordMetadata> sendFuture = producer.send(new ProducerRecord<>(outputTopic, keyValue.key, keyValue.value));
                final RecordMetadata recordMetadata;
                try {
                    recordMetadata = sendFuture.get(5000, TimeUnit.SECONDS);
                } catch (InterruptedException | TimeoutException | ExecutionException e) {
                    throw new RuntimeException("Failed to produce record", e);
                }
            }
        }
    }

    protected abstract List<KeyValue<OK, OV>> transform(ConsumerRecords<IK, IV> records);
}
