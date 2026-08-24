package dev.fgutierrez.dsplayground.kafkaorders.api;

import dev.fgutierrez.dsplayground.kafkaorders.order.Order;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderLine;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Create-only: this example's teaching point is what happens between the commit and the broker, not
 * the REST contract (already covered in synchronous-processing). Kept anyway, rather than driving
 * OrderService only from tests, so `scripts/run-example.sh outbox` gives a real `curl`-able demo —
 * create an order here, watch the event land in kafka-ui.
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

  private final OrderService orderService;

  public OrderController(OrderService orderService) {
    this.orderService = orderService;
  }

  @PostMapping
  public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    List<OrderLine> lines =
        request.lines().stream()
            .map(line -> new OrderLine(line.productId(), line.quantity(), line.unitPrice()))
            .toList();
    Order created = orderService.createOrder(request.customerId(), lines);
    return ResponseEntity.created(URI.create("/api/v1/orders/" + created.getId()))
        .body(OrderResponse.from(created));
  }
}
