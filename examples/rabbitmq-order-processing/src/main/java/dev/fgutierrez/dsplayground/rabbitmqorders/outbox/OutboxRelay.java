package dev.fgutierrez.dsplayground.rabbitmqorders.outbox;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Same shape as OutboxRelay in the outbox/kafka-order-processing examples — the only thing that
 * changes across all three is which EventPublisher gets injected. ROUTING_KEY (not TOPIC: Kafka
 * terminology doesn't apply to RabbitMQ) is what OrderService.createOrder tags every outbox row
 * with, and what RabbitConfig's queues are bound to on the fixed order-events exchange.
 */
@Component
public class OutboxRelay {

  public static final String ROUTING_KEY = "order.created.v1";

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

  public int publishPending() {
    List<OutboxEvent> pending = outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc();
    int published = 0;
    for (OutboxEvent event : pending) {
      eventPublisher.publish(ROUTING_KEY, event.getAggregateId().toString(), event.getPayload());
      event.markPublished(Instant.now(clock));
      outboxEventRepository.save(event);
      published++;
    }
    return published;
  }
}
