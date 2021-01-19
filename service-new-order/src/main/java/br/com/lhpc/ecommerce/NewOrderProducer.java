package br.com.lhpc.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcherEcommerce<Order>()) {
            try (var dispatcher = new KafkaDispatcherEcommerce<String>()) {
                for (int i = 0; i < 10; i++) {
                    var userID = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(userID, UUID.randomUUID().toString(), amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userID, order);

                    var email = "Thank you for your order! We are processing your order";
                    dispatcher.send("ECOMMERCE_SEND_EMAIL", userID, email);
                }
            }
        }

    }

}
