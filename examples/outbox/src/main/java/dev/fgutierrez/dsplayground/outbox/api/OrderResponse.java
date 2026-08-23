package dev.fgutierrez.dsplayground.outbox.api;

import dev.fgutierrez.dsplayground.outbox.order.Order;
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
    Instant createdAt) {

  static OrderResponse from(Order order) {
    return new OrderResponse(
        order.getId(),
        order.getCustomerId(),
        order.getStatus().name(),
        order.getTotalAmount(),
        order.getLines().stream().map(OrderLineResponse::from).toList(),
        order.getCreatedAt());
  }
}
