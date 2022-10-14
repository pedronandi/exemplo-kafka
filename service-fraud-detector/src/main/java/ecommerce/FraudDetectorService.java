package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var fraudService = new FraudDetectorService();

        try(var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var message = record.value();
        var order = message.getPayload();

        System.out.println("----------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key " + record.key());
        System.out.println("Value " + order);
        System.out.println("Part. " + record.partition());
        System.out.println("Off " + record.offset());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(isFraud(order)) {
            System.out.println("Order is a fraud! " + order.toString());

            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    message.getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            System.out.println("Approved: " + order.toString());

            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    message.getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}