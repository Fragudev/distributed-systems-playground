package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import dev.fgutierrez.dsplayground.rabbitmqorders.order.Order;
import dev.fgutierrez.dsplayground.rabbitmqorders.order.OrderLine;
import dev.fgutierrez.dsplayground.rabbitmqorders.order.OrderService;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxEvent;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxEventRepository;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.rabbitmqorders.support.PostgresAndRabbitIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Three queues bound to one exchange: the RabbitMQ shape of the fan-out consumer groups give in
 * kafka-order-processing. Every assertion is scoped to this test's own event id rather than a
 * global row count, since both tests below share one Spring context (and one database).
 */
class FanOutTest extends PostgresAndRabbitIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private OutboxEventRepository outboxEventRepository;
  @Autowired private ProcessedEventRepository processedEventRepository;
  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private ObjectMapper objectMapper;

  @Test
  void allThreeQueuesProcessTheSameEventIndependently() {
    UUID eventId = createOrderAndCapturePublishedEventId("customer-1", "widget");

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> assertThat(processedEventRepository.findByIdEventId(eventId)).hasSize(3));

    List<ProcessedEvent> processed = processedEventRepository.findByIdEventId(eventId);
    assertThat(processed.stream().map(p -> p.getId().getConsumerGroup()).toList())
        .containsExactlyInAnyOrder(
            InventoryEventListener.CONSUMER_GROUP,
            PaymentEventListener.CONSUMER_GROUP,
            NotificationEventListener.CONSUMER_GROUP);
  }

  @Test
  void redeliveringTheSameEventIsANoOpForAnAlreadyProcessedConsumer() {
    UUID eventId = createOrderAndCapturePublishedEventId("customer-2", "gadget");

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> assertThat(processedEventRepository.findByIdEventId(eventId)).hasSize(3));

    String payload = payloadFor(eventId);
    // Simulates redelivery — e.g. a manual replay of a message that, unlike
    // PoisonMessageAndReplayTest's scenario, actually succeeded the first time. Same exchange,
    // same routing key, same payload OutboxRelay already sent.
    rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE, RabbitConfig.ROUTING_KEY, payload);

    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(processedEventRepository.findByIdEventId(eventId)).hasSize(3));
  }

  private UUID createOrderAndCapturePublishedEventId(String customerId, String productId) {
    Order order =
        orderService.createOrder(customerId, List.of(new OrderLine(productId, 1, BigDecimal.TEN)));
    outboxRelay.publishPending();
    return IncomingOrderCreatedEvent.parse(payload(order.getId()), objectMapper).eventId();
  }

  private String payload(UUID orderId) {
    OutboxEvent row =
        outboxEventRepository.findAll().stream()
            .filter(e -> e.getAggregateId().equals(orderId))
            .findFirst()
            .orElseThrow();
    return row.getPayload();
  }

  private String payloadFor(UUID eventId) {
    return outboxEventRepository.findAll().stream()
        .filter(
            e ->
                IncomingOrderCreatedEvent.parse(e.getPayload(), objectMapper)
                    .eventId()
                    .equals(eventId))
        .findFirst()
        .orElseThrow()
        .getPayload();
  }
}
