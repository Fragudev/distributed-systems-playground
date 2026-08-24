package dev.fgutierrez.dsplayground.resilience.api;

import dev.fgutierrez.dsplayground.resilience.order.Order;
import dev.fgutierrez.dsplayground.resilience.order.OrderLine;
import dev.fgutierrez.dsplayground.resilience.order.OrderService;
import dev.fgutierrez.dsplayground.resilience.shipping.ShippingConfirmation;
import dev.fgutierrez.dsplayground.resilience.shipping.ShippingGateway;
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
  private final ShippingGateway shippingGateway;

  public OrderController(OrderService orderService, ShippingGateway shippingGateway) {
    this.orderService = orderService;
    this.shippingGateway = shippingGateway;
  }

  @PostMapping
  public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    List<OrderLine> lines =
        request.lines().stream()
            .map(line -> new OrderLine(line.productId(), line.quantity(), line.unitPrice()))
            .toList();
    // Deliberately outside OrderService's transaction: the order is already durably committed by
    // the time this call happens, so a slow or failing carrier never holds a DB connection open —
    // and can't roll back an order that already exists.
    Order created = orderService.createOrder(request.customerId(), lines);
    ShippingConfirmation confirmation = requestShipmentGracefully(created.getId());
    return ResponseEntity.created(URI.create("/api/v1/orders/" + created.getId()))
        .body(OrderResponse.from(created, confirmation.status()));
  }

  /**
   * ResilientShippingGateway's own fallbackMethod only catches what reaches the CircuitBreaker's
   * instrumentation — a ThreadPoolBulkhead rejection (queue genuinely full under concurrent load)
   * is thrown synchronously by the outer Bulkhead aspect, before the CircuitBreaker aspect (and
   * hence its fallback) is ever entered, and surfaced a real 500 here under load. This is the
   * actual boundary where "no request ever fails outright" has to be guaranteed, regardless of
   * which Resilience4j aspect a given failure mode happens to originate from.
   */
  private ShippingConfirmation requestShipmentGracefully(UUID orderId) {
    try {
      return shippingGateway.requestShipment(orderId).join();
    } catch (RuntimeException e) {
      return ShippingConfirmation.pending();
    }
  }

  @GetMapping("/{id}")
  public OrderResponse get(@PathVariable UUID id) {
    return OrderResponse.from(orderService.getOrder(id), null);
  }
}
