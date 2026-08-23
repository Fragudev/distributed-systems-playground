package dev.fgutierrez.dsplayground.outbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.fgutierrez.dsplayground.outbox.order.OrderLine;
import dev.fgutierrez.dsplayground.outbox.order.OrderService;
import dev.fgutierrez.dsplayground.outbox.support.FlakyEventPublisher;
import dev.fgutierrez.dsplayground.outbox.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The failure scenario from the project plan: "kill the relay mid-batch; on restart, the poller
 * only republishes what's still pending." Three orders, three pending outbox rows; the second
 * publish attempt is rigged to fail (see FlakyEventPublisher), simulating a crash between the first
 * and second row of the batch.
 */
@Import(OutboxFailureTest.FlakyPublisherConfig.class)
class OutboxFailureTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private OutboxEventRepository outboxEventRepository;

  @Test
  void aCrashMidBatchLeavesEarlierRowsPublishedAndLaterRowsPendingForTheNextPoll() {
    var order1 =
        orderService.createOrder("customer-1", List.of(new OrderLine("a", 1, BigDecimal.TEN)));
    var order2 =
        orderService.createOrder("customer-2", List.of(new OrderLine("b", 1, BigDecimal.TEN)));
    var order3 =
        orderService.createOrder("customer-3", List.of(new OrderLine("c", 1, BigDecimal.TEN)));

    // First poll: order1 publishes fine, order2's attempt is the rigged failure — the "crash".
    assertThatThrownBy(outboxRelay::publishPending).isInstanceOf(EventPublishException.class);

    assertThat(publishedAggregateIds()).containsExactly(order1.getId());

    // "Restart": the same relay is polled again. order2's retry succeeds this time (only call #2
    // was ever rigged to fail), and order3 — never attempted during the crash — publishes too.
    int publishedOnRetry = outboxRelay.publishPending();
    assertThat(publishedOnRetry).isEqualTo(2);

    assertThat(publishedAggregateIds())
        .containsExactlyInAnyOrder(order1.getId(), order2.getId(), order3.getId());

    var records = consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10));
    assertThat(records).as("no loss and no duplicates on the broker itself").hasSize(3);
    assertThat(records.stream().map(r -> r.key()).collect(Collectors.toSet()))
        .containsExactlyInAnyOrder(
            order1.getId().toString(), order2.getId().toString(), order3.getId().toString());
  }

  private List<UUID> publishedAggregateIds() {
    return outboxEventRepository.findAll().stream()
        .filter(e -> e.getPublishedAt() != null)
        .map(OutboxEvent::getAggregateId)
        .toList();
  }

  @TestConfiguration
  static class FlakyPublisherConfig {

    @Bean
    @Primary
    EventPublisher flakyEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
      return new FlakyEventPublisher(new KafkaEventPublisher(kafkaTemplate), 2);
    }
  }
}
