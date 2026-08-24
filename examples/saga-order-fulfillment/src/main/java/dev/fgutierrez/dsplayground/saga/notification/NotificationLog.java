package dev.fgutierrez.dsplayground.saga.notification;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Stands in for "an email/SMS went out" — this example only cares that notification-service reacted
 * to the cancellation, not that it actually sent anything, so a log row is enough. Its existence
 * per order is the idempotency guard, same pattern as every other consumer here.
 */
@Entity
@Table(name = "notification_log")
public class NotificationLog {

  @Id private UUID id;

  @Column(name = "order_id", nullable = false, unique = true)
  private UUID orderId;

  @Column(nullable = false)
  private String message;

  @Column(name = "sent_at", nullable = false)
  private Instant sentAt;

  protected NotificationLog() {}

  public NotificationLog(UUID orderId, String message, Instant sentAt) {
    this.id = UUID.randomUUID();
    this.orderId = orderId;
    this.message = message;
    this.sentAt = sentAt;
  }

  public UUID getOrderId() {
    return orderId;
  }

  public String getMessage() {
    return message;
  }
}
