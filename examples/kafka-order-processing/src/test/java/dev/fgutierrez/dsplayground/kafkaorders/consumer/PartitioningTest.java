package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.kafkaorders.support.PostgresAndKafkaIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * order.created.v1 has 3 partitions (KafkaConsumerConfig#orderCreatedTopic) and is always published
 * keyed by orderId — this is what that buys: ordering preserved per order, parallelism across
 * orders.
 */
class PartitioningTest extends PostgresAndKafkaIntegrationTest {

  @Test
  void eventsForTheSameOrderAlwaysLandOnTheSamePartition() {
    String orderId = UUID.randomUUID().toString();
    IntStream.range(0, 5).forEach(i -> publish(OutboxRelay.TOPIC, orderId, "payload-" + i));

    List<ConsumerRecord<String, String>> records =
        consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10));
    Set<Integer> partitions =
        records.stream()
            .filter(r -> r.key().equals(orderId))
            .map(ConsumerRecord::partition)
            .collect(Collectors.toSet());

    assertThat(partitions)
        .as("all 5 events for the same order must land on one partition")
        .hasSize(1);
  }

  @Test
  void differentOrdersSpreadAcrossPartitions() {
    IntStream.range(0, 20)
        .forEach(i -> publish(OutboxRelay.TOPIC, UUID.randomUUID().toString(), "payload-" + i));

    List<ConsumerRecord<String, String>> records =
        consumeAll(OutboxRelay.TOPIC, Duration.ofSeconds(10));
    Set<Integer> partitions =
        records.stream().map(ConsumerRecord::partition).collect(Collectors.toSet());

    assertThat(partitions)
        .as("20 distinct order ids should not all funnel onto a single partition")
        .hasSizeGreaterThan(1);
  }
}
