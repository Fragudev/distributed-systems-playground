package dev.fgutierrez.dsplayground.rabbitmqorders.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * Same CloudEvents-inspired envelope as every other example in this playground — see
 * examples/outbox/.../EventEnvelope.java.
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
    return new EventEnvelope(eventId, type, source, subject, time, eventId, null, payload);
  }
}
