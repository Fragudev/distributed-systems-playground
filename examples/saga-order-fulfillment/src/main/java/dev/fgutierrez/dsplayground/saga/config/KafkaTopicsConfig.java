package dev.fgutierrez.dsplayground.saga.config;

import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * One partition each: this example is about the choreography and compensation flow, not
 * partitioning/ordering (already demonstrated in kafka-order-processing — see its
 * PartitioningTest). Every event here still carries orderId as its key, so a future switch to
 * multiple partitions wouldn't break per-order ordering.
 */
@Configuration
public class KafkaTopicsConfig {

  @Bean
  NewTopic orderCreatedTopic() {
    return TopicBuilder.name(EventTypes.ORDER_CREATED).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic orderCancelledTopic() {
    return TopicBuilder.name(EventTypes.ORDER_CANCELLED).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic paymentCompletedTopic() {
    return TopicBuilder.name(EventTypes.PAYMENT_COMPLETED).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic paymentFailedTopic() {
    return TopicBuilder.name(EventTypes.PAYMENT_FAILED).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic inventoryReservedTopic() {
    return TopicBuilder.name(EventTypes.INVENTORY_RESERVED).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic inventoryRejectedTopic() {
    return TopicBuilder.name(EventTypes.INVENTORY_REJECTED).partitions(1).replicas(1).build();
  }
}
