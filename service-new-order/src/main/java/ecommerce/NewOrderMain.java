package ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            try(var emailDispatcher = new KafkaDispatcher<Email>()) {
                for (int i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var userEmail = Math.random() + "@email.com";
                    var order = new Order(orderId, amount, userEmail);
                    var email = new Email("First contact!", "Thank you for your order! We're processing your order!");

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order);
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, email);
                }
            }
        }
    }
}
