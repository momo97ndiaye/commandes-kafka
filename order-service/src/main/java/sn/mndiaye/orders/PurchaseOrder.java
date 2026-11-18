package sn.mndiaye.orders;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "purchase_order")
public class PurchaseOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PurchaseOrder() {
    }

    public PurchaseOrder(String sku, int quantity, Instant createdAt) {
        this.sku = sku;
        this.quantity = quantity;
        this.status = OrderStatus.PENDING;
        this.createdAt = createdAt;
    }

    public void confirm() {
        this.status = OrderStatus.CONFIRMED;
        this.reason = null;
    }

    public void reject(String reason) {
        this.status = OrderStatus.REJECTED;
        this.reason = reason;
    }

    public Long getId() { return id; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public OrderStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getCreatedAt() { return createdAt; }
}
