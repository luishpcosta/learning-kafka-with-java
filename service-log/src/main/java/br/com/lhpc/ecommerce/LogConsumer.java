package br.com.lhpc.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Map;
import java.util.regex.Pattern;

public class LogConsumer {

    public static void main(String[] args) {
        LogConsumer logConsumer = new LogConsumer();
        try(var kafkaConsumer =  new KafkaConsumer(
                LogConsumer.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logConsumer::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName()))) {

            kafkaConsumer.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------------------");
        System.out.println("LOG " + record.topic());
        System.out.println("key:" + record.key() + " - value:" + record.value());
        System.out.println("Partition:" + record.partition() + " - offset:" + record.value());
    }
}
