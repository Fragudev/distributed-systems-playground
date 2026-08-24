package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import java.time.Clock;
import java.time.Instant;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Independent queue bound to the same exchange: gets its own full copy of order.created.v1, not a
 * share of inventory-service's messages — the RabbitMQ shape of the same fan-out consumer groups
 * give in the Kafka example.
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

  @RabbitListener(queues = RabbitConfig.PAYMENT_QUEUE)
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
