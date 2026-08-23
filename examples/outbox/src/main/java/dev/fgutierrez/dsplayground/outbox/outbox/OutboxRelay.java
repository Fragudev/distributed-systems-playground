package dev.fgutierrez.dsplayground.outbox.outbox;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls the outbox table and publishes whatever hasn't been published yet, oldest first.
 *
 * <p>Each row is published and marked in its own step (not one big transaction spanning the whole
 * batch), so a crash between two rows leaves everything before it durably published and everything
 * from that point on untouched — exactly what the next poll needs to pick up cleanly. See
 * OutboxFailureTest for the scenario this is built to survive, and the example README for the one
 * gap this doesn't close (a crash between the broker ack and the DB write below).
 */
@Component
public class OutboxRelay {

  public static final String TOPIC = "order.created.v1";

  private final OutboxEventRepository outboxEventRepository;
  private final EventPublisher eventPublisher;
  private final Clock clock;

  public OutboxRelay(
      OutboxEventRepository outboxEventRepository, EventPublisher eventPublisher, Clock clock) {
    this.outboxEventRepository = outboxEventRepository;
    this.eventPublisher = eventPublisher;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "PT2S")
  void poll() {
    publishPending();
  }

  /**
   * Returns how many rows were published. Left package-visible-via-public (not package-private) so
   * tests can drive it directly instead of waiting on the scheduler.
   */
  public int publishPending() {
    List<OutboxEvent> pending = outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc();
    int published = 0;
    for (OutboxEvent event : pending) {
      eventPublisher.publish(TOPIC, event.getAggregateId().toString(), event.getPayload());
      event.markPublished(Instant.now(clock));
      outboxEventRepository.save(event);
      published++;
    }
    return published;
  }
}
