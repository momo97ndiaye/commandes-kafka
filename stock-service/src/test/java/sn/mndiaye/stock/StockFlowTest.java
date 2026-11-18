package sn.mndiaye.stock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDERS_CREATED, Topics.STOCK_RESULTS})
class StockFlowTest {

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper objectMapper;
    @Autowired StockItemRepository items;

    private Consumer<String, String> results;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    @BeforeEach
    void listenToResults() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "true", broker);
        results = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(results, Topics.STOCK_RESULTS);
    }

    @AfterEach
    void close() {
        results.close();
    }

    @Test
    void reservesStockWhenAvailable() throws Exception {
        items.save(new StockItem("TEST-A", 5));

        send(new Events.OrderCreated(101L, "TEST-A", 3));

        assertThat(awaitResult(101L).status()).isEqualTo(Events.StockResult.RESERVED);
        assertThat(items.findById("TEST-A").orElseThrow().getAvailable()).isEqualTo(2);
    }

    @Test
    void replayedOrderIsNotReservedTwice() throws Exception {
        items.save(new StockItem("TEST-B", 5));
        Events.OrderCreated order = new Events.OrderCreated(201L, "TEST-B", 2);

        send(order);
        send(order);

        assertThat(awaitResult(201L).status()).isEqualTo(Events.StockResult.RESERVED);
        assertThat(awaitResult(201L).status()).isEqualTo(Events.StockResult.RESERVED);
        assertThat(items.findById("TEST-B").orElseThrow().getAvailable()).isEqualTo(3);
    }

    @Test
    void rejectsWhenStockIsInsufficient() throws Exception {
        items.save(new StockItem("TEST-C", 1));

        send(new Events.OrderCreated(301L, "TEST-C", 2));

        Events.StockResult result = awaitResult(301L);
        assertThat(result.status()).isEqualTo(Events.StockResult.REJECTED);
        assertThat(result.reason()).isEqualTo("Stock insuffisant");
        assertThat(items.findById("TEST-C").orElseThrow().getAvailable()).isEqualTo(1);
    }

    @Test
    void rejectsUnknownProduct() throws Exception {
        send(new Events.OrderCreated(401L, "INCONNU", 1));

        Events.StockResult result = awaitResult(401L);
        assertThat(result.status()).isEqualTo(Events.StockResult.REJECTED);
        assertThat(result.reason()).isEqualTo("Produit inconnu");
    }

    private void send(Events.OrderCreated order) throws Exception {
        kafka.send(Topics.ORDERS_CREATED, order.sku(), objectMapper.writeValueAsString(order)).get();
    }

    /** Renvoie la prochaine réponse reçue pour la commande demandée (chaque réponse n'est lue qu'une fois). */
    private Events.StockResult awaitResult(long orderId) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            Iterator<ConsumerRecord<String, String>> it = received.iterator();
            while (it.hasNext()) {
                ConsumerRecord<String, String> record = it.next();
                if (String.valueOf(orderId).equals(record.key())) {
                    it.remove();
                    return objectMapper.readValue(record.value(), Events.StockResult.class);
                }
            }
            results.poll(Duration.ofMillis(500)).forEach(received::add);
        }
        throw new AssertionError("Aucune réponse pour la commande " + orderId);
    }
}
