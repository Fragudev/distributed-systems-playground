package dev.fgutierrez.dsplayground.saga.order;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

  @Id private UUID id;

  @Column(name = "customer_id", nullable = false)
  private String customerId;

  // EAGER for the same reason as synchronous-processing: open-in-view is disabled, and here the
  // outbox payload is built from these lines after OrderService's transaction has returned.
  @OneToMany(
      mappedBy = "order",
      cascade = CascadeType.ALL,
      orphanRemoval = true,
      fetch = FetchType.EAGER)
  private List<OrderLine> lines = new ArrayList<>();

  @Column(name = "total_amount", nullable = false)
  private BigDecimal totalAmount;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private OrderStatus status;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  protected Order() {}

  private Order(UUID id, String customerId, List<OrderLine> lines, Instant createdAt) {
    this.id = id;
    this.customerId = customerId;
    this.status = OrderStatus.CREATED;
    this.createdAt = createdAt;
    lines.forEach(this::addLine);
    this.totalAmount =
        lines.stream().map(OrderLine::subtotal).reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  public static Order create(String customerId, List<OrderLine> lines, Clock clock) {
    return new Order(UUID.randomUUID(), customerId, lines, Instant.now(clock));
  }

  /**
   * Idempotent on purpose: SagaCoordinator may see the same finalize event more than once
   * (redelivery), and re-applying the same terminal status must be a safe no-op, not an error.
   */
  public void markConfirmed() {
    if (status == OrderStatus.CREATED) {
      status = OrderStatus.CONFIRMED;
    }
  }

  public void markCancelled() {
    if (status == OrderStatus.CREATED) {
      status = OrderStatus.CANCELLED;
    }
  }

  private void addLine(OrderLine line) {
    lines.add(line);
    line.assignTo(this);
  }

  public UUID getId() {
    return id;
  }

  public String getCustomerId() {
    return customerId;
  }

  public List<OrderLine> getLines() {
    return Collections.unmodifiableList(lines);
  }

  public BigDecimal getTotalAmount() {
    return totalAmount;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }
}
