package dev.fgutierrez.dsplayground.outbox.support;

import dev.fgutierrez.dsplayground.outbox.outbox.EventPublishException;
import dev.fgutierrez.dsplayground.outbox.outbox.EventPublisher;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delegates to a real EventPublisher on every call except the {@code failOnCallNumber}-th
 * (1-indexed), which throws instead — simulating "the process died right here, on this specific
 * attempt" without mocking Kafka away entirely. The failing call throws before reaching the
 * delegate, so the broker never sees that attempt at all: a crash before the send, not a crash
 * after an ack the relay never got to record. A restart naturally retries and succeeds, since only
 * that one call number is rigged to fail.
 */
public class FlakyEventPublisher implements EventPublisher {

  private final EventPublisher delegate;
  private final int failOnCallNumber;
  private final AtomicInteger callCount = new AtomicInteger();

  public FlakyEventPublisher(EventPublisher delegate, int failOnCallNumber) {
    this.delegate = delegate;
    this.failOnCallNumber = failOnCallNumber;
  }

  @Override
  public void publish(String topic, String key, String payload) {
    if (callCount.incrementAndGet() == failOnCallNumber) {
      throw new EventPublishException(topic, key, new IllegalStateException("simulated crash"));
    }
    delegate.publish(topic, key, payload);
  }
}
