package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderLine;
import dev.fgutierrez.dsplayground.kafkaorders.order.OrderService;
import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.kafkaorders.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

/**
 * The project's failure scenario 2: a message inventory-service can never process (downstream
 * "outage", simulated via ControllableInventoryChecker) retries with backoff, then lands on
 * order.created.v1.DLT. Once the cause is fixed, replaying it — exactly what scripts/replay-dlq.sh
 * automates against a real cluster — reprocesses it successfully. The other two consumer groups are
 * unaffected throughout: their own copy of the same event never touches this failure at all.
 */
@Import(PoisonMessageAndReplayTest.ControllableAvailabilityConfig.class)
class PoisonMessageAndReplayTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private ProcessedEventRepository processedEventRepository;
  @Autowired private ControllableInventoryChecker inventoryChecker;
  @Autowired private ObjectMapper objectMapper;

  @Test
  void retriesThenDeadLettersThenSucceedsOnReplayAfterTheCauseIsFixed() {
    inventoryChecker.setAvailable(false);

    var order =
        orderService.createOrder("customer-1", List.of(new OrderLine("widget", 1, BigDecimal.TEN)));
    outboxRelay.publishPending();

    // 3 attempts with backoff, then DefaultErrorHandler routes it to the DLT — see
    // KafkaConsumerConfig for the exact retry configuration this proves out.
    List<ConsumerRecord<String, String>> dlt =
        consumeAll(OutboxRelay.TOPIC + ".DLT", Duration.ofSeconds(20));
    assertThat(dlt).hasSize(1);
    assertThat(dlt.get(0).key()).isEqualTo(order.getId().toString());

    var event = IncomingOrderCreatedEvent.parse(dlt.get(0).value(), objectMapper);
    assertThat(
            processedEventRepository.existsById(
                new ProcessedEventId(InventoryEventListener.CONSUMER_GROUP, event.eventId())))
        .as("inventory-service never succeeded — no row, not a stuck partial one")
        .isFalse();

    // The other two groups have their own copy and were never affected by inventory-service's
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

    // "Fix" the cause, then replay — the same mechanics as scripts/replay-dlq.sh: read from the
    // DLT, republish onto the original topic with the same key.
    inventoryChecker.setAvailable(true);
    publish(OutboxRelay.TOPIC, dlt.get(0).key(), dlt.get(0).value());

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
