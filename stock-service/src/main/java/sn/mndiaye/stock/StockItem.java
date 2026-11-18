package sn.mndiaye.stock;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock_item")
public class StockItem {

    @Id
    private String sku;

    @Column(nullable = false)
    private int available;

    protected StockItem() {
    }

    public StockItem(String sku, int available) {
        this.sku = sku;
        this.available = available;
    }

    public boolean canReserve(int quantity) {
        return available >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException("Stock insuffisant pour " + sku);
        }
        available -= quantity;
    }

    public String getSku() { return sku; }
    public int getAvailable() { return available; }
}
