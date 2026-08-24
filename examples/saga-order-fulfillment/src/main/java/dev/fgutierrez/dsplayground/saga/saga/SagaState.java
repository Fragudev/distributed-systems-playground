package dev.fgutierrez.dsplayground.saga.saga;

import dev.fgutierrez.dsplayground.saga.inventory.InventoryStatus;
import dev.fgutierrez.dsplayground.saga.payment.PaymentStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * The coordinator's own read model of a saga in flight — one row per order, tracking which legs
 * have reported in. Nothing here drives the participants; it only observes their outcome events
 * and, once both legs are known, decides what the order itself should become. This is what makes it
 * choreography-with-a-watcher rather than orchestration: the coordinator never tells payment or
 * inventory what to do, only order (see ADR 0008).
 */
@Entity
@Table(name = "saga_state")
public class SagaState {

  @Id
  @Column(name = "order_id")
  private UUID orderId;

  @Enumerated(EnumType.STRING)
  @Column(name = "payment_status")
  private PaymentStatus paymentStatus;

  @Enumerated(EnumType.STRING)
  @Column(name = "inventory_status")
  private InventoryStatus inventoryStatus;

  @Enumerated(EnumType.STRING)
  private SagaOutcome outcome;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  protected SagaState() {}

  public SagaState(UUID orderId, Instant now) {
    this.orderId = orderId;
    this.updatedAt = now;
  }

  public void recordPaymentOutcome(PaymentStatus status, Instant now) {
    if (this.paymentStatus == null) {
      this.paymentStatus = status;
      this.updatedAt = now;
    }
  }

  public void recordInventoryOutcome(InventoryStatus status, Instant now) {
    if (this.inventoryStatus == null) {
      this.inventoryStatus = status;
      this.updatedAt = now;
    }
  }

  /**
   * Both legs succeed -> CONFIRMED. Either leg fails -> CANCELLED, even if the other leg is still
   * pending (see README §5: the saga does not wait for a doomed order to finish failing before
   * compensating what already succeeded). Returns null while the outcome isn't decidable yet.
   */
  public SagaOutcome decideOutcome(Instant now) {
    if (outcome != null) {
      return null; // already decided — caller must not act twice
    }
    boolean anyRejected =
        paymentStatus == PaymentStatus.FAILED || inventoryStatus == InventoryStatus.REJECTED;
    boolean bothSucceeded =
        paymentStatus == PaymentStatus.COMPLETED && inventoryStatus == InventoryStatus.RESERVED;

    if (anyRejected) {
      outcome = SagaOutcome.CANCELLED;
    } else if (bothSucceeded) {
      outcome = SagaOutcome.CONFIRMED;
    } else {
      return null; // still waiting on the other leg
    }
    updatedAt = now;
    return outcome;
  }

  public UUID getOrderId() {
    return orderId;
  }

  public PaymentStatus getPaymentStatus() {
    return paymentStatus;
  }

  public InventoryStatus getInventoryStatus() {
    return inventoryStatus;
  }

  public SagaOutcome getOutcome() {
    return outcome;
  }
}
