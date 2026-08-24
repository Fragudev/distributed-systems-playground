package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import java.time.Clock;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Independent consumer group: gets its own full copy of order.created.v1, not a share of
 * inventory-service's messages — that's the fan-out consumer groups give you.
 */
@Component
public class PaymentEventListener {

  static final String CONSUMER_GROUP = "payment-service";

  private final ProcessedEventRepository processedEventRepository;
  private final ProcessingMetrics metrics;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public PaymentEventListener(
      ProcessedEventRepository processedEventRepository,
      ProcessingMetrics metrics,
      ObjectMapper objectMapper,
      Clock clock) {
    this.processedEventRepository = processedEventRepository;
    this.metrics = metrics;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = OutboxRelay.TOPIC, groupId = CONSUMER_GROUP)
  public void onOrderCreated(String payload) {
    IncomingOrderCreatedEvent event = IncomingOrderCreatedEvent.parse(payload, objectMapper);
    ProcessedEventId id = new ProcessedEventId(CONSUMER_GROUP, event.eventId());
    if (processedEventRepository.existsById(id)) {
      metrics.recordDuplicate(CONSUMER_GROUP);
      return;
    }
    processedEventRepository.save(
        new ProcessedEvent(CONSUMER_GROUP, event.eventId(), Instant.now(clock)));
    metrics.recordProcessed(CONSUMER_GROUP);
  }
}
