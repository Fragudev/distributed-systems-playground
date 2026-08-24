package dev.fgutierrez.dsplayground.resilience.api;

import dev.fgutierrez.dsplayground.resilience.order.Order;
import dev.fgutierrez.dsplayground.resilience.shipping.ShippingConfirmation;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
    UUID id,
    String customerId,
    String status,
    BigDecimal totalAmount,
    List<OrderLineResponse> lines,
    Instant createdAt,
    // PENDING_CONFIRMATION here doesn't mean anything failed — it's the graceful-degradation
    // outcome (see the example README §3): the order is still created and returned successfully
    // even when the shipping carrier couldn't be reached in time. Null on GET: this example
    // doesn't persist the outcome (accepted scope-limiting debt, see README §6) — it's only known
    // at the moment of creation.
    String shippingStatus) {

  static OrderResponse from(Order order, ShippingConfirmation.Status shippingStatus) {
    return new OrderResponse(
        order.getId(),
        order.getCustomerId(),
        order.getStatus().name(),
        order.getTotalAmount(),
        order.getLines().stream().map(OrderLineResponse::from).toList(),
        order.getCreatedAt(),
        shippingStatus == null ? null : shippingStatus.name());
  }
}
