package dev.fgutierrez.dsplayground.kafkaorders.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "order_line")
public class OrderLine {

  @Id private UUID id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "order_id", nullable = false)
  private Order order;

  @Column(name = "product_id", nullable = false)
  private String productId;

  @Column(nullable = false)
  private int quantity;

  @Column(name = "unit_price", nullable = false)
  private BigDecimal unitPrice;

  protected OrderLine() {}

  public OrderLine(String productId, int quantity, BigDecimal unitPrice) {
    this.id = UUID.randomUUID();
    this.productId = productId;
    this.quantity = quantity;
    this.unitPrice = unitPrice;
  }

  void assignTo(Order order) {
    this.order = order;
  }

  BigDecimal subtotal() {
    return unitPrice.multiply(BigDecimal.valueOf(quantity));
  }

  public String getProductId() {
    return productId;
  }

  public int getQuantity() {
    return quantity;
  }

  public BigDecimal getUnitPrice() {
    return unitPrice;
  }
}
