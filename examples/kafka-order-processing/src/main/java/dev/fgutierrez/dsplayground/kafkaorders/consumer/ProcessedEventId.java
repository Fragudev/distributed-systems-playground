package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class ProcessedEventId implements Serializable {

  @Column(name = "consumer_group", nullable = false)
  private String consumerGroup;

  @Column(name = "event_id", nullable = false)
  private UUID eventId;

  protected ProcessedEventId() {}

  public ProcessedEventId(String consumerGroup, UUID eventId) {
    this.consumerGroup = consumerGroup;
    this.eventId = eventId;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public UUID getEventId() {
    return eventId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ProcessedEventId that)) return false;
    return Objects.equals(consumerGroup, that.consumerGroup)
        && Objects.equals(eventId, that.eventId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroup, eventId);
  }
}
