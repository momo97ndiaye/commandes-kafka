package sn.mndiaye.stock;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/** Données de démonstration chargées au démarrage si le stock est vide. */
@Component
public class DemoData implements CommandLineRunner {

    private final StockItemRepository items;

    public DemoData(StockItemRepository items) {
        this.items = items;
    }

    @Override
    public void run(String... args) {
        if (items.count() == 0) {
            items.save(new StockItem("SKU-001", 10));
            items.save(new StockItem("SKU-002", 0));
        }
    }
}
