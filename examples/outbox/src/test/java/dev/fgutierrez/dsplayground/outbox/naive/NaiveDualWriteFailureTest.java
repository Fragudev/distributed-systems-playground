package dev.fgutierrez.dsplayground.outbox.naive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.fgutierrez.dsplayground.outbox.order.OrderLine;
import dev.fgutierrez.dsplayground.outbox.order.OrderRepository;
import dev.fgutierrez.dsplayground.outbox.outbox.EventPublishException;
import dev.fgutierrez.dsplayground.outbox.outbox.EventPublisher;
import dev.fgutierrez.dsplayground.outbox.outbox.KafkaEventPublisher;
import dev.fgutierrez.dsplayground.outbox.outbox.OutboxEventRepository;
import dev.fgutierrez.dsplayground.outbox.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.outbox.support.FlakyEventPublisher;
import dev.fgutierrez.dsplayground.outbox.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Makes the "naive solution" section of the README concrete: a failed publish after the order is
 * already committed doesn't roll anything back, and — unlike OutboxFailureTest — there is no row
 * anywhere for anything to retry from. The event is gone for good.
 */
@Import(NaiveDualWriteFailureTest.AlwaysFailingPublisherConfig.class)
class NaiveDualWriteFailureTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private NaiveOrderService naiveOrderService;
  @Autowired private OrderRepository orderRepository;
  @Autowired private OutboxEventRepository outboxEventRepository;

  @Test
  void aFailedPublishAfterCommitPermanentlyLosesTheEvent() {
    assertThatThrownBy(
            () ->
                naiveOrderService.createOrder(
                    "customer-1", List.of(new OrderLine("widget", 1, BigDecimal.TEN))))
        .isInstanceOf(EventPublishException.class);

    assertThat(orderRepository.count())
        .as("the naive bug: the order is committed even though the event never went out")
        .isEqualTo(1);

    assertThat(outboxEventRepository.count())
        .as("there is no outbox row for anything to retry from — the naive path never writes one")
        .isZero();

    assertThat(consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(3)))
        .as("the event is simply gone")
        .isEmpty();
  }

  @TestConfiguration
  static class AlwaysFailingPublisherConfig {

    @Bean
    @Primary
    EventPublisher alwaysFailingEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
      return new FlakyEventPublisher(new KafkaEventPublisher(kafkaTemplate), 1);
    }
  }
}
