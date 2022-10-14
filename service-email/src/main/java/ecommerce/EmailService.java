package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;

public class EmailService {

    public static void main(String[] args) throws IOException {
        var emailService = new EmailService();

        try(var service = new KafkaService<>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Email>> record) {
        var message = record.value();

        System.out.println("----------------------------------------");
        System.out.println("Sending e-mail");
        System.out.println("Key " + record.key());
        System.out.println("Value " + message.getPayload());
        System.out.println("Part. " + record.partition());
        System.out.println("Off " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("E-mail sent!");
    }
}
