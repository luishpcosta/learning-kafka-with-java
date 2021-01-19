package br.com.lhpc.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailConsumer {

    public static void main(String[] args) {
        var emailConsumer = new EmailConsumer();
        try (var service = new KafkaConsumerEcommerce(EmailConsumer.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailConsumer::parse,
                String.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------------------");
        System.out.println("Send Email");
        System.out.println("key:" + record.key() + " - value:" + record.value());
        System.out.println("Partition:" + record.partition() + " - offset:" + record.value());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email Sent");
    }
}
