package sn.mndiaye.stock;

/** Messages échangés avec le service des commandes (JSON sur Kafka). */
public final class Events {

    private Events() {
    }

    public record OrderCreated(Long orderId, String sku, int quantity) {
    }

    public record StockResult(Long orderId, String status, String reason) {
        public static final String RESERVED = "RESERVED";
        public static final String REJECTED = "REJECTED";

        static StockResult reserved(Long orderId) {
            return new StockResult(orderId, RESERVED, null);
        }

        static StockResult rejected(Long orderId, String reason) {
            return new StockResult(orderId, REJECTED, reason);
        }
    }
}
