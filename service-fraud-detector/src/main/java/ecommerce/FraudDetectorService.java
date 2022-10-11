package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;

public class FraudDetectorService {

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

    private void parse(ConsumerRecord<String, Order> record) {
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

        System.out.println("Order processed");
    }
}