package dev.fgutierrez.dsplayground.saga.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * payment-service's own local model of a payment — its existence for a given order is what makes
 * onOrderCreated idempotent (see PaymentEventListener); its status is what makes compensation
 * idempotent too (a second order.cancelled.v1 delivery finds it already REFUNDED).
 */
@Entity
@Table(name = "payment")
public class Payment {

  @Id private UUID id;

  @Column(name = "order_id", nullable = false, unique = true)
  private UUID orderId;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private PaymentStatus status;

  @Column(nullable = false)
  private BigDecimal amount;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  protected Payment() {}

  public Payment(UUID orderId, PaymentStatus status, BigDecimal amount, Instant now) {
    this.id = UUID.randomUUID();
    this.orderId = orderId;
    this.status = status;
    this.amount = amount;
    this.createdAt = now;
    this.updatedAt = now;
  }

  public void refund(Instant now) {
    if (status == PaymentStatus.COMPLETED) {
      status = PaymentStatus.REFUNDED;
      updatedAt = now;
    }
  }

  public UUID getOrderId() {
    return orderId;
  }

  public PaymentStatus getStatus() {
    return status;
  }

  public BigDecimal getAmount() {
    return amount;
  }
}
