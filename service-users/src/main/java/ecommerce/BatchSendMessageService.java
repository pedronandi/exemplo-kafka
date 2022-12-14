package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);

        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, IOException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();

        try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("----------------------------------------");
        System.out.println("Processing new batch");

        var message = record.value();

        System.out.println("Topic: " + message.getPayload());

        for(User user : getAllUsers()) {
            userDispatcher.sendAsync(message.getPayload(),
                    user.getUuid(),
                    message.getCorrelationId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);

            System.out.println("Sent to " + user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();

        while(results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}
