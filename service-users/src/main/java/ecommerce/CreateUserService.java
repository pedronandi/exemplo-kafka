package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
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

    public static void main(String[] args) throws SQLException, IOException {
        var userService = new CreateUserService();

        try(var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                userService::parse,
                new HashMap<>())) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("----------------------------------------");
        System.out.println("Processing new order, checking for new user");

        var message = record.value();
        var order = message.getPayload();

        System.out.println("Value " + order);

        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?,?)");
        var uuid = UUID.randomUUID().toString();

        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();

        System.out.println("User " + uuid + " and " + email + " added!");
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users " +
                "where email = ? limit 1");

        exists.setString(1, email);

        var results = exists.executeQuery();

        return !results.next();
    }
}
