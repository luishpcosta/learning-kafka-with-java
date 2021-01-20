package br.com.lhpc.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorConsumer {

    public static void main(String[] args) {
        var fraudDetectorConsumer = new FraudDetectorConsumer();
        try (var service = new KafkaConsumer<>(
                FraudDetectorConsumer.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                fraudDetectorConsumer::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> dispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key:" + record.key() + " - value:" + record.value());
        System.out.println("Partition:" + record.partition() + " - offset:" + record.value());
        var order = record.value();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (order.isFraud()) {
            System.out.println("Order is a fraud");
            dispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Order was accepted");
            dispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }
    }
}
