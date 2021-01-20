package br.com.lhpc.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaConsumer<T> implements Closeable {

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaConsumer(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumer(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaConsumer(String groupId, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(getProperties(groupId, type, properties));
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("I found messages: " + records.count());
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (SQLException | ExecutionException | InterruptedException e) {
                    // simple implementation for now, just logging
                    e.printStackTrace();
                }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Class<T> type,  Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }


    @Override
    public void close() {
        consumer.close();
    }
}
