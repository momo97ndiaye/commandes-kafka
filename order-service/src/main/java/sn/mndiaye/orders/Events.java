package sn.mndiaye.orders;

/** Messages échangés avec le service du stock (JSON sur Kafka). */
public final class Events {

    private Events() {
    }

    public record OrderCreated(Long orderId, String sku, int quantity) {
    }

    public record StockResult(Long orderId, String status, String reason) {
        public static final String RESERVED = "RESERVED";
        public static final String REJECTED = "REJECTED";
    }
}
