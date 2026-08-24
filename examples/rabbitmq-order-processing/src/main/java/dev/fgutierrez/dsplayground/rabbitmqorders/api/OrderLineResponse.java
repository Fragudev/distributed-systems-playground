package dev.fgutierrez.dsplayground.rabbitmqorders.api;

import dev.fgutierrez.dsplayground.rabbitmqorders.order.OrderLine;
import java.math.BigDecimal;

public record OrderLineResponse(String productId, int quantity, BigDecimal unitPrice) {

  static OrderLineResponse from(OrderLine line) {
    return new OrderLineResponse(line.getProductId(), line.getQuantity(), line.getUnitPrice());
  }
}
