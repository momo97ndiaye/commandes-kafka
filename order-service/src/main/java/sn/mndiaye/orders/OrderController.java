package sn.mndiaye.orders;

import java.time.Instant;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    /** 202 : la commande est enregistrée, la réservation du stock se fait ensuite de façon asynchrone. */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
        return OrderResponse.from(service.create(request.sku(), request.quantity()));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable Long id) {
        return OrderResponse.from(service.get(id));
    }

    public record CreateOrderRequest(@NotBlank String sku, @Min(1) int quantity) {
    }

    public record OrderResponse(Long id, String sku, int quantity, OrderStatus status, String reason,
                                Instant createdAt) {
        static OrderResponse from(PurchaseOrder o) {
            return new OrderResponse(o.getId(), o.getSku(), o.getQuantity(), o.getStatus(), o.getReason(),
                    o.getCreatedAt());
        }
    }
}
