package sn.mndiaye.stock;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stock")
public class StockController {

    private final StockService service;

    public StockController(StockService service) {
        this.service = service;
    }

    @GetMapping("/{sku}")
    public StockResponse get(@PathVariable String sku) {
        StockItem item = service.get(sku);
        return new StockResponse(item.getSku(), item.getAvailable());
    }

    public record StockResponse(String sku, int available) {
    }
}
