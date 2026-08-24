package dev.fgutierrez.dsplayground.saga.outbox;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls the outbox table and publishes whatever hasn't been published yet, oldest first — same
 * shape as every other example's OutboxRelay, with one generalization this one actually needs:
 * every participant here (order-service, payment-service, inventory-service, the saga coordinator
 * itself) writes to the same outbox_event table, each with its own event type, so the topic to
 * publish to comes from the row (event.getEventType()) instead of a single hardcoded constant. A
 * single-topic outbox was enough when only order.created.v1 ever got published; a choreographed
 * saga publishes six different event types from four different places.
 *
 * <p>Each row is published and marked in its own step (not one big transaction spanning the whole
 * batch), so a crash between two rows leaves everything before it durably published and everything
 * from that point on untouched — see docs/adr/0003-transactional-outbox.md.
 */
@Component
public class OutboxRelay {

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
      eventPublisher.publish(
          event.getEventType(), event.getAggregateId().toString(), event.getPayload());
      event.markPublished(Instant.now(clock));
      outboxEventRepository.save(event);
      published++;
    }
    return published;
  }
}
