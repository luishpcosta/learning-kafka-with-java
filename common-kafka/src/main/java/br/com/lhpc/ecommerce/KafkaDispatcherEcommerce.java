package br.com.lhpc.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcherEcommerce<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcherEcommerce() {
        this.producer = new KafkaProducer<>(getProperties());
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);

        Callback callback = (data, err) -> {
            if (err != null) {
                err.printStackTrace();
                return;
            }
            System.out.println("was sent successfully " + data.topic() + ":::" + data.partition() + "/" + data.offset() + "/" + data.timestamp());
        };

        producer.send(record, callback).get();

    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
