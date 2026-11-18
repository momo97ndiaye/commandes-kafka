package sn.mndiaye.orders;

import java.time.Clock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.server.ResponseStatusException;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final PurchaseOrderRepository orders;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OrderService(PurchaseOrderRepository orders, KafkaTemplate<String, String> kafka,
                        ObjectMapper objectMapper, Clock clock) {
        this.orders = orders;
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /**
     * Enregistre la commande, puis publie l'événement une fois la transaction validée : le service du stock
     * ne peut pas répondre sur une commande qui n'est pas encore visible en base.
     * La clé du message est la référence produit, pour que les commandes d'un même produit restent ordonnées.
     */
    @Transactional
    public PurchaseOrder create(String sku, int quantity) {
        PurchaseOrder order = orders.save(new PurchaseOrder(sku, quantity, clock.instant()));
        String payload = toJson(new Events.OrderCreated(order.getId(), sku, quantity));
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafka.send(Topics.ORDERS_CREATED, sku, payload);
            }
        });
        return order;
    }

    @Transactional(readOnly = true)
    public PurchaseOrder get(Long id) {
        return orders.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Commande introuvable"));
    }

    @KafkaListener(topics = Topics.STOCK_RESULTS)
    @Transactional
    public void onStockResult(String message) throws JsonProcessingException {
        Events.StockResult result = objectMapper.readValue(message, Events.StockResult.class);
        orders.findById(result.orderId()).ifPresentOrElse(order -> {
            if (order.getStatus() != OrderStatus.PENDING) {
                return; // résultat déjà appliqué (message rejoué)
            }
            if (Events.StockResult.RESERVED.equals(result.status())) {
                order.confirm();
            } else {
                order.reject(result.reason());
            }
        }, () -> log.warn("Résultat de stock pour une commande inconnue : {}", result.orderId()));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
