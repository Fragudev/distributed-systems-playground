package dev.fgutierrez.dsplayground.kafkaorders.order;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** The `payload` of an `order.created.v1` EventEnvelope. */
public record OrderCreatedPayload(
    UUID orderId, String customerId, BigDecimal totalAmount, List<Line> lines) {

  public record Line(String productId, int quantity, BigDecimal unitPrice) {}

  public static OrderCreatedPayload from(Order order) {
    List<Line> lines =
        order.getLines().stream()
            .map(l -> new Line(l.getProductId(), l.getQuantity(), l.getUnitPrice()))
            .toList();
    return new OrderCreatedPayload(
        order.getId(), order.getCustomerId(), order.getTotalAmount(), lines);
  }
}
