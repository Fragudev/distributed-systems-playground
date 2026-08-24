package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * The one listener in this example wired to fail on demand, mirroring InventoryEventListener in
 * kafka-order-processing — same reason: exercising the failure machinery once is enough, repeating
 * it on all three queues would test the same mechanism three times.
 *
 * <p>Unlike Kafka's DefaultErrorHandler, nothing here retries in-process. A failed message is
 * rejected without requeue; RabbitConfig's queue arguments (not this class) route it through the
 * TTL-delayed retry queue and back. This class's only job on failure is deciding, via the standard
 * {@code x-death} header RabbitMQ stamps on every dead-lettered message, whether this is "retry
 * again" or "give up and publish to the final DLQ directly."
 */
@Component
public class InventoryEventListener {

  static final String CONSUMER_GROUP = "inventory-service";
  private static final int MAX_ATTEMPTS = 3;

  private final ProcessedEventRepository processedEventRepository;
  private final InventoryAvailabilityChecker availabilityChecker;
  private final RabbitTemplate rabbitTemplate;
  private final ProcessingMetrics metrics;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public InventoryEventListener(
      ProcessedEventRepository processedEventRepository,
      InventoryAvailabilityChecker availabilityChecker,
      RabbitTemplate rabbitTemplate,
      ProcessingMetrics metrics,
      ObjectMapper objectMapper,
      Clock clock) {
    this.processedEventRepository = processedEventRepository;
    this.availabilityChecker = availabilityChecker;
    this.rabbitTemplate = rabbitTemplate;
    this.metrics = metrics;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @RabbitListener(queues = RabbitConfig.INVENTORY_QUEUE)
  public void onOrderCreated(Message message) {
    String payload = new String(message.getBody(), StandardCharsets.UTF_8);
    IncomingOrderCreatedEvent event = IncomingOrderCreatedEvent.parse(payload, objectMapper);
    ProcessedEventId id = new ProcessedEventId(CONSUMER_GROUP, event.eventId());

    if (processedEventRepository.existsById(id)) {
      metrics.recordDuplicate(CONSUMER_GROUP);
      return;
    }

    if (!availabilityChecker.isAvailable()) {
      metrics.recordFailed(CONSUMER_GROUP);
      int attemptsSoFar = 1 + retryQueueDeathCount(message);
      if (attemptsSoFar >= MAX_ATTEMPTS) {
        // Given up: publish straight to the final DLQ ourselves and return normally (Spring AMQP
        // acks the original), rather than rejecting it into the retry loop yet again.
        rabbitTemplate.send("", RabbitConfig.INVENTORY_DLQ, message);
        return;
      }
      throw new IllegalStateException("Inventory system unavailable for order " + event.orderId());
    }

    processedEventRepository.save(
        new ProcessedEvent(CONSUMER_GROUP, event.eventId(), Instant.now(clock)));
    metrics.recordProcessed(CONSUMER_GROUP);
  }

  /**
   * How many times this message has already been through inventory-service.retry, per RabbitMQ's
   * own bookkeeping — not a counter this class maintains itself.
   */
  private int retryQueueDeathCount(Message message) {
    Object header = message.getMessageProperties().getHeaders().get("x-death");
    if (!(header instanceof List<?> deaths)) {
      return 0;
    }
    for (Object entry : deaths) {
      if (entry instanceof Map<?, ?> death
          && RabbitConfig.INVENTORY_RETRY_QUEUE.equals(String.valueOf(death.get("queue")))) {
        Object count = death.get("count");
        if (count instanceof Number number) {
          return number.intValue();
        }
      }
    }
    return 0;
  }
}
