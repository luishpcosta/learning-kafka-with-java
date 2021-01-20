package br.com.lhpc.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var dispatcher = new KafkaDispatcher<String>()) {
                for (int i = 0; i < 10; i++) {
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var email = Math.random() + "@email.com";
                    var order = new Order(UUID.randomUUID().toString(), amount, email);

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                    var content = "Thank you for your order! We are processing your order";
                    dispatcher.send("ECOMMERCE_SEND_EMAIL", email, content);
                }
            }
        }

    }

}
