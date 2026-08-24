package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import dev.fgutierrez.dsplayground.rabbitmqorders.order.OrderLine;
import dev.fgutierrez.dsplayground.rabbitmqorders.order.OrderService;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.rabbitmqorders.support.PostgresAndRabbitIntegrationTest;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

/**
 * The project's failure scenario 3: the RabbitMQ shape of the same downstream-outage scenario
 * kafka-order-processing's PoisonMessageAndReplayTest exercises, through nack → DLX → TTL-delayed
 * retry → back to the main queue → ... → a final DLQ once inventory-service gives up. See
 * RabbitConfig for the queue arguments that make this entirely declarative — no manual retry-topic
 * code the way Kafka needs.
 */
@Import(PoisonMessageAndReplayTest.ControllableAvailabilityConfig.class)
class PoisonMessageAndReplayTest extends PostgresAndRabbitIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private ProcessedEventRepository processedEventRepository;
  @Autowired private ControllableInventoryChecker inventoryChecker;
  @Autowired private RabbitTemplate rabbitTemplate;
  @Autowired private ObjectMapper objectMapper;

  @Test
  void retriesThenDeadLettersThenSucceedsOnReplayAfterTheCauseIsFixed() {
    inventoryChecker.setAvailable(false);

    var order =
        orderService.createOrder("customer-1", List.of(new OrderLine("widget", 1, BigDecimal.TEN)));
    outboxRelay.publishPending();

    // Two TTL-delayed retry hops (RabbitConfig's 1s retry queue) before InventoryEventListener
    // gives up on the third attempt and publishes here itself.
    Message dead = rabbitTemplate.receive(RabbitConfig.INVENTORY_DLQ, 20_000);
    assertThat(dead).as("the message should have landed on the DLQ").isNotNull();
    String payload = new String(dead.getBody(), StandardCharsets.UTF_8);

    var event = IncomingOrderCreatedEvent.parse(payload, objectMapper);
    assertThat(
            processedEventRepository.existsById(
                new ProcessedEventId(InventoryEventListener.CONSUMER_GROUP, event.eventId())))
        .as("inventory-service never succeeded — no row, not a stuck partial one")
        .isFalse();

    // The other two queues had their own copy and were never affected by inventory-service's
    // trouble.
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(
                        processedEventRepository.existsById(
                            new ProcessedEventId(
                                PaymentEventListener.CONSUMER_GROUP, event.eventId())))
                    .isTrue());

    // "Fix" the cause, then replay: republish onto the main exchange with the original routing
    // key, exactly what an operator would do from the RabbitMQ Management UI's "Move messages"
    // action on the DLQ.
    inventoryChecker.setAvailable(true);
    rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE, RabbitConfig.ROUTING_KEY, payload);

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(
                        processedEventRepository.existsById(
                            new ProcessedEventId(
                                InventoryEventListener.CONSUMER_GROUP, event.eventId())))
                    .isTrue());
  }

  @TestConfiguration
  static class ControllableAvailabilityConfig {

    @Bean
    @Primary
    InventoryAvailabilityChecker controllableInventoryChecker() {
      return new ControllableInventoryChecker();
    }
  }

  static class ControllableInventoryChecker implements InventoryAvailabilityChecker {

    private volatile boolean available = true;

    void setAvailable(boolean available) {
      this.available = available;
    }

    @Override
    public boolean isAvailable() {
      return available;
    }
  }
}
