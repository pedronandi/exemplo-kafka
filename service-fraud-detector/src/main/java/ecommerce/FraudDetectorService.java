package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

    public static void main(String[] args) throws IOException {
        var fraudService = new FraudDetectorService();

        try(var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key " + record.key());
        System.out.println("Value " + record.value());
        System.out.println("Part. " + record.partition());
        System.out.println("Off " + record.offset());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var order = record.value();

        if(isFraud(order)) {
            System.out.println("Order is a fraud! " + order.toString());

            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order.toString());

            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}