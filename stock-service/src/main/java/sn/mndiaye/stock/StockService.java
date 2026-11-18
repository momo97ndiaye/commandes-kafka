package sn.mndiaye.stock;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class StockService {

    private final StockItemRepository items;
    private final ProcessedOrderRepository processed;

    public StockService(StockItemRepository items, ProcessedOrderRepository processed) {
        this.items = items;
        this.processed = processed;
    }

    /**
     * Réserve le stock pour une commande. Si la commande a déjà été traitée (message rejoué par Kafka),
     * renvoie la même réponse sans toucher au stock.
     */
    @Transactional
    public Events.StockResult reserve(Events.OrderCreated order) {
        return processed.findById(order.orderId())
                .map(ProcessedOrder::toResult)
                .orElseGet(() -> {
                    Events.StockResult result = items.findById(order.sku())
                            .map(item -> {
                                if (!item.canReserve(order.quantity())) {
                                    return Events.StockResult.rejected(order.orderId(), "Stock insuffisant");
                                }
                                item.reserve(order.quantity());
                                return Events.StockResult.reserved(order.orderId());
                            })
                            .orElse(Events.StockResult.rejected(order.orderId(), "Produit inconnu"));
                    processed.save(new ProcessedOrder(result));
                    return result;
                });
    }

    @Transactional(readOnly = true)
    public StockItem get(String sku) {
        return items.findById(sku)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Produit inconnu"));
    }
}
