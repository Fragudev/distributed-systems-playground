package dev.fgutierrez.dsplayground.outbox.outbox;

/**
 * Seam between OutboxRelay and the actual broker client, so tests can simulate "the process died
 * right here" (see FlakyEventPublisher) without mocking Kafka itself.
 */
public interface EventPublisher {

  void publish(String topic, String key, String payload);
}
