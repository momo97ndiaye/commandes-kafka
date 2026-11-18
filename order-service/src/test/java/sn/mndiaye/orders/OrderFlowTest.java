package sn.mndiaye.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDERS_CREATED, Topics.STOCK_RESULTS})
class OrderFlowTest {

    @Autowired MockMvc mvc;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper objectMapper;

    private Consumer<String, String> created;

    @BeforeEach
    void listenToCreatedOrders() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "true", broker);
        created = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(created, Topics.ORDERS_CREATED);
    }

    @AfterEach
    void close() {
        created.close();
    }

    @Test
    void orderIsPublishedThenConfirmedWhenStockIsReserved() throws Exception {
        long id = createOrder("SKU-001", 2);

        ConsumerRecord<String, String> record = awaitCreatedEvent(id);
        assertThat(record.key()).isEqualTo("SKU-001");
        Events.OrderCreated event = objectMapper.readValue(record.value(), Events.OrderCreated.class);
        assertThat(event.quantity()).isEqualTo(2);

        sendResult(new Events.StockResult(id, Events.StockResult.RESERVED, null));

        awaitStatus(id, "CONFIRMED");
    }

    @Test
    void orderIsRejectedWithTheReasonGivenByStock() throws Exception {
        long id = createOrder("SKU-002", 1);
        awaitCreatedEvent(id);

        sendResult(new Events.StockResult(id, Events.StockResult.REJECTED, "Stock insuffisant"));

        awaitStatus(id, "REJECTED");
        mvc.perform(get("/api/orders/" + id)).andExpect(jsonPath("$.reason").value("Stock insuffisant"));
    }

    @Test
    void invalidQuantityIsRejected() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sku\":\"SKU-001\",\"quantity\":0}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unknownOrderReturns404() throws Exception {
        mvc.perform(get("/api/orders/999999")).andExpect(status().isNotFound());
    }

    private long createOrder(String sku, int quantity) throws Exception {
        String body = mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sku\":\"%s\",\"quantity\":%d}".formatted(sku, quantity)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("id").asLong();
    }

    private void sendResult(Events.StockResult result) throws Exception {
        kafka.send(Topics.STOCK_RESULTS, String.valueOf(result.orderId()), objectMapper.writeValueAsString(result))
                .get();
    }

    private void awaitStatus(long id, String expected) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                mvc.perform(get("/api/orders/" + id)).andExpect(jsonPath("$.status").value(expected)));
    }

    /** Lit les événements publiés jusqu'à trouver celui de la commande demandée. */
    private ConsumerRecord<String, String> awaitCreatedEvent(long orderId) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> record : created.poll(Duration.ofMillis(500))) {
                Events.OrderCreated event = objectMapper.readValue(record.value(), Events.OrderCreated.class);
                if (event.orderId() == orderId) {
                    return record;
                }
            }
        }
        throw new AssertionError("Aucun événement publié pour la commande " + orderId);
    }
}
