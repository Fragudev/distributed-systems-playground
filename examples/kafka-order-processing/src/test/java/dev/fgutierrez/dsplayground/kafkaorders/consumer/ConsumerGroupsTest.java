package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.kafkaorders.order.Order;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderLine;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderService;
import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.kafkaorders.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Consumer groups fan out, they don't share the load of one topic like competing consumers on a
 * queue would: each of the three groups here gets its own full copy of the same event. Every
 * assertion is scoped to this test's own event id rather than a global row count, since both tests
 * below share one Spring context (and one database) within this class.
 */
class ConsumerGroupsTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private ProcessedEventRepository processedEventRepository;
  @Autowired private ObjectMapper objectMapper;

  @Test
  void allThreeConsumerGroupsProcessTheSameEventIndependently() {
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

    // Simulates redelivery — e.g. a consumer-group rebalance replaying already-committed offsets,
    // or a DLT replay of a message that, unlike PoisonMessageAndReplayTest's scenario, actually
    // succeeded the first time. Same key, same payload OutboxRelay already sent.
    String payload = latestPayloadFor(eventId);
    publish(OutboxRelay.TOPIC, extractOrderId(payload), payload);

    // No new row appears for this event — the idempotency guard in each listener treats
    // redelivery as a no-op, not a second reservation/charge/notification.
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
    String payload =
        consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10)).stream()
            .filter(r -> r.key().equals(order.getId().toString()))
            .findFirst()
            .orElseThrow()
            .value();
    return IncomingOrderCreatedEvent.parse(payload, objectMapper).eventId();
  }

  private String latestPayloadFor(UUID eventId) {
    return consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10)).stream()
        .filter(
            r -> IncomingOrderCreatedEvent.parse(r.value(), objectMapper).eventId().equals(eventId))
        .findFirst()
        .orElseThrow()
        .value();
  }

  private String extractOrderId(String payload) {
    return IncomingOrderCreatedEvent.parse(payload, objectMapper).orderId().toString();
  }
}
