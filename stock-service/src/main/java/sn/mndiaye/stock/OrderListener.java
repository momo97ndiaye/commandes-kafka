package sn.mndiaye.stock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private final StockService stock;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    public OrderListener(StockService stock, KafkaTemplate<String, String> kafka, ObjectMapper objectMapper) {
        this.stock = stock;
        this.kafka = kafka;
        this.objectMapper = objectMapper;
    }

    /** La réponse n'est envoyée qu'après la validation de la transaction de réservation. */
    @KafkaListener(topics = Topics.ORDERS_CREATED)
    public void onOrderCreated(String message) throws JsonProcessingException {
        Events.OrderCreated order = objectMapper.readValue(message, Events.OrderCreated.class);
        Events.StockResult result = stock.reserve(order);
        kafka.send(Topics.STOCK_RESULTS, String.valueOf(order.orderId()), objectMapper.writeValueAsString(result));
    }
}
