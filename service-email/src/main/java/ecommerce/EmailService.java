package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;

public class EmailService {

    public static void main(String[] args) throws IOException {
        var emailService = new EmailService();

        try(var service = new KafkaService<Email>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("----------------------------------------");
        System.out.println("Sending e-mail");
        System.out.println("Key " + record.key());
        System.out.println("Value " + record.value());
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
