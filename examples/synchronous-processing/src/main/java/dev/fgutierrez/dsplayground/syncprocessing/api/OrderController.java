package dev.fgutierrez.dsplayground.syncprocessing.api;

import dev.fgutierrez.dsplayground.syncprocessing.order.Order;
import dev.fgutierrez.dsplayground.syncprocessing.order.OrderLine;
import dev.fgutierrez.dsplayground.syncprocessing.order.OrderService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

  @GetMapping("/{id}")
  public OrderResponse get(@PathVariable UUID id) {
    return OrderResponse.from(orderService.getOrder(id));
  }
}
