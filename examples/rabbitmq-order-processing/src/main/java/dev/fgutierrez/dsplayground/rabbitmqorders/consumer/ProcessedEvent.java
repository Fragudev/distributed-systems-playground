package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * One row per (consumer group, event) successfully processed. Checked at the start of every
 * listener before doing any work — see ADR 0005 — and doubles as the queryable proof that a given
 * consumer group actually handled a given event.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

  @EmbeddedId private ProcessedEventId id;

  @Column(name = "processed_at", nullable = false)
  private Instant processedAt;

  protected ProcessedEvent() {}

  public ProcessedEvent(String consumerGroup, UUID eventId, Instant processedAt) {
    this.id = new ProcessedEventId(consumerGroup, eventId);
    this.processedAt = processedAt;
  }

  public ProcessedEventId getId() {
    return id;
  }

  public Instant getProcessedAt() {
    return processedAt;
  }
}
