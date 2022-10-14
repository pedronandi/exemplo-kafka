package ecommerce;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();

        try {
            orderDispatcher.close();
            emailDispatcher.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var orderId = UUID.randomUUID().toString();
            var amount = BigDecimal.valueOf(Long.parseLong(req.getParameter("amount")));
            var userEmail = req.getParameter("email");
            var order = new Order(orderId, amount, userEmail);
            var email = new Email("First contact!", "Thank you for your order! We're processing your order!");

            orderDispatcher.send("ECOMMERCE_NEW_ORDER",
                    userEmail,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    order);
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL",
                    userEmail,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    email);

            System.out.println("New order sent successfully!");
            resp.getWriter().println(String.format("New order sent from %s!", userEmail));
            resp.setStatus(HttpServletResponse.SC_OK);
        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
