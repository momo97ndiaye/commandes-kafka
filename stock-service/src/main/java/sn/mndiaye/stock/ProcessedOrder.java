package sn.mndiaye.stock;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Commande déjà traitée, avec la réponse envoyée : un message rejoué ne réserve pas deux fois. */
@Entity
@Table(name = "processed_order")
public class ProcessedOrder {

    @Id
    @Column(name = "order_id")
    private Long orderId;

    @Column(nullable = false)
    private String status;

    private String reason;

    protected ProcessedOrder() {
    }

    public ProcessedOrder(Events.StockResult result) {
        this.orderId = result.orderId();
        this.status = result.status();
        this.reason = result.reason();
    }

    public Events.StockResult toResult() {
        return new Events.StockResult(orderId, status, reason);
    }
}
