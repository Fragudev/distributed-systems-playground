package dev.fgutierrez.dsplayground.saga.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * inventory-service's own local model of a reservation — mirrors payment.Payment's role: its
 * existence for a given order makes onOrderCreated idempotent, its status makes compensation
 * (release) idempotent too.
 */
@Entity
@Table(name = "inventory_reservation")
public class InventoryReservation {

  @Id private UUID id;

  @Column(name = "order_id", nullable = false, unique = true)
  private UUID orderId;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private InventoryStatus status;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  protected InventoryReservation() {}

  public InventoryReservation(UUID orderId, InventoryStatus status, Instant now) {
    this.id = UUID.randomUUID();
    this.orderId = orderId;
    this.status = status;
    this.createdAt = now;
    this.updatedAt = now;
  }

  public void release(Instant now) {
    if (status == InventoryStatus.RESERVED) {
      status = InventoryStatus.RELEASED;
      updatedAt = now;
    }
  }

  public UUID getOrderId() {
    return orderId;
  }

  public InventoryStatus getStatus() {
    return status;
  }
}
