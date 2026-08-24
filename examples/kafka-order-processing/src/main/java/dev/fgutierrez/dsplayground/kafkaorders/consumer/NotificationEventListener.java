package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import java.time.Clock;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Third independent consumer group — see PaymentEventListener for why this is deliberately simple
 * rather than repeating InventoryEventListener's failure-injection seam.
 */
@Component
public class NotificationEventListener {

  static final String CONSUMER_GROUP = "notification-service";

  private final ProcessedEventRepository processedEventRepository;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public NotificationEventListener(
      ProcessedEventRepository processedEventRepository, ObjectMapper objectMapper, Clock clock) {
    this.processedEventRepository = processedEventRepository;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = OutboxRelay.TOPIC, groupId = CONSUMER_GROUP)
  public void onOrderCreated(String payload) {
    IncomingOrderCreatedEvent event = IncomingOrderCreatedEvent.parse(payload, objectMapper);
    ProcessedEventId id = new ProcessedEventId(CONSUMER_GROUP, event.eventId());
    if (processedEventRepository.existsById(id)) {
      return;
    }
    processedEventRepository.save(
        new ProcessedEvent(CONSUMER_GROUP, event.eventId(), Instant.now(clock)));
  }
}
