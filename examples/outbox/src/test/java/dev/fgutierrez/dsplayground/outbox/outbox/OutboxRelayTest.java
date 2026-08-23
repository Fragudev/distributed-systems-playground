package dev.fgutierrez.dsplayground.outbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import dev.fgutierrez.dsplayground.outbox.order.OrderLine;
import dev.fgutierrez.dsplayground.outbox.order.OrderService;
import dev.fgutierrez.dsplayground.outbox.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OutboxRelayTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private OutboxEventRepository outboxEventRepository;

  @Test
  void publishesAPendingRowAndMarksItPublished() {
    var order =
        orderService.createOrder(
            "customer-1", List.of(new OrderLine("widget", 2, new BigDecimal("9.99"))));

    OutboxEvent row = outboxEventRepository.findAll().get(0);
    assertThat(row.getPublishedAt()).isNull();

    int published = outboxRelay.publishPending();

    assertThat(published).isEqualTo(1);
    OutboxEvent afterPublish = outboxEventRepository.findById(row.getId()).orElseThrow();
    assertThat(afterPublish.getPublishedAt()).isNotNull();

    List<ConsumerRecord<String, String>> records =
        consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10));
    assertThat(records).hasSize(1);
    assertThat(records.get(0).key()).isEqualTo(order.getId().toString());
    assertThat(records.get(0).value()).contains(order.getId().toString()).contains("customer-1");
  }
}
