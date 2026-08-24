package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import java.time.Clock;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Also the one listener in this example wired to fail on demand (via InventoryAvailabilityChecker)
 * so PoisonMessageAndReplayTest can exercise retry → DLT → replay end to end. PaymentEventListener
 * and NotificationEventListener are deliberately plain — repeating the same failure machinery on
 * all three would test the same mechanism three times, not three different things.
 */
@Component
public class InventoryEventListener {

  static final String CONSUMER_GROUP = "inventory-service";

  private final ProcessedEventRepository processedEventRepository;
  private final InventoryAvailabilityChecker availabilityChecker;
  private final ProcessingMetrics metrics;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public InventoryEventListener(
      ProcessedEventRepository processedEventRepository,
      InventoryAvailabilityChecker availabilityChecker,
      ProcessingMetrics metrics,
      ObjectMapper objectMapper,
      Clock clock) {
    this.processedEventRepository = processedEventRepository;
    this.availabilityChecker = availabilityChecker;
    this.metrics = metrics;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = OutboxRelay.TOPIC, groupId = CONSUMER_GROUP)
  public void onOrderCreated(String payload) {
    IncomingOrderCreatedEvent event = IncomingOrderCreatedEvent.parse(payload, objectMapper);
    ProcessedEventId id = new ProcessedEventId(CONSUMER_GROUP, event.eventId());

    // Idempotency guard (ADR 0005): at-least-once delivery means this listener WILL see the same
    // event more than once eventually (redelivery after a rebalance, a replay from the DLT, ...).
    // Checking first makes reprocessing a no-op instead of double-reserving inventory.
    if (processedEventRepository.existsById(id)) {
      metrics.recordDuplicate(CONSUMER_GROUP);
      return;
    }

    if (!availabilityChecker.isAvailable()) {
      metrics.recordFailed(CONSUMER_GROUP);
      throw new IllegalStateException("Inventory system unavailable for order " + event.orderId());
    }

    processedEventRepository.save(
        new ProcessedEvent(CONSUMER_GROUP, event.eventId(), Instant.now(clock)));
    metrics.recordProcessed(CONSUMER_GROUP);
  }
}
