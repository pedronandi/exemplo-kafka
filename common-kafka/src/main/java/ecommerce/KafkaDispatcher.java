package ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<String, Message<T>>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //OK do líder vem depois que está tudo sincronizado junto às réplicas (ISR = In Sync Replicas)

        return properties;
    }

    void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        var value = new Message<T>(id, payload);
        var record = new ProducerRecord<String, Message<T>>(topic, key, value);

        /*
         * send() é assíncrono pq retorna um Future (vai ser executado no futuro)
         * mas é possível implementar um callback
         * */
        producer.send(record, getCallback()).get();
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("Success sending " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
