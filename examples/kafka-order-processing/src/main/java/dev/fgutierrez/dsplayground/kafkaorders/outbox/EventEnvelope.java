package dev.fgutierrez.dsplayground.kafkaorders.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * CloudEvents-inspired envelope, kept consistent with the one described in the project plan so
 * every example in this playground that publishes an event uses the same shape.
 */
public record EventEnvelope(
    UUID eventId,
    String type,
    String source,
    String subject,
    Instant time,
    UUID correlationId,
    UUID causationId,
    Object payload) {

  public static EventEnvelope forNewAggregate(
      String type, String source, String subject, Instant time, Object payload) {
    UUID eventId = UUID.randomUUID();
    // No prior event caused this one, and nothing to correlate against yet — the event's own id
    // doubles as the start of a correlation chain a consumer could extend.
    return new EventEnvelope(eventId, type, source, subject, time, eventId, null, payload);
  }
}
