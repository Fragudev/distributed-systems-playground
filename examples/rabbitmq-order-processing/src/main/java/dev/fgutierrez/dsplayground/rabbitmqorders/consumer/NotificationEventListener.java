package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import java.time.Clock;
import java.time.Instant;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Third independent queue — see PaymentEventListener for why this is deliberately simple rather
 * than repeating InventoryEventListener's DLX/retry machinery.
 */
@Component
public class NotificationEventListener {

  static final String CONSUMER_GROUP = "notification-service";

  private final ProcessedEventRepository processedEventRepository;
  private final ProcessingMetrics metrics;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public NotificationEventListener(
      ProcessedEventRepository processedEventRepository,
      ProcessingMetrics metrics,
      ObjectMapper objectMapper,
      Clock clock) {
    this.processedEventRepository = processedEventRepository;
    this.metrics = metrics;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @RabbitListener(queues = RabbitConfig.NOTIFICATION_QUEUE)
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
