package dev.fgutierrez.dsplayground.kafkaorders.config;

import dev.fgutierrez.dsplayground.kafkaorders.outbox.OutboxRelay;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

  /**
   * order.created.v1 with three partitions, keyed by orderId (already how OutboxRelay publishes):
   * same order always lands on the same partition (ordering preserved per order), different orders
   * spread across partitions (parallelism across orders). See PartitioningTest.
   */
  @Bean
  NewTopic orderCreatedTopic() {
    return TopicBuilder.name(OutboxRelay.TOPIC).partitions(3).replicas(1).build();
  }

  /**
   * Retries a failing listener 3 times total with exponential backoff, then gives up and routes the
   * record to `order.created.v1.DLT` — see PoisonMessageAndReplayTest for the exact count, proved
   * rather than assumed. Bean name matches Spring Kafka's default listener container factory name,
   * so every @KafkaListener in this example picks this up without being told to.
   */
  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory,
      KafkaTemplate<String, String> kafkaTemplate) {
    ExponentialBackOff backOff = new ExponentialBackOff(200L, 2.0);
    backOff.setMaxAttempts(3);
    // Spring Kafka's own default DLT naming is "<topic>-dlt"; this project's convention
    // (docs/adr, the example README) is "<topic>.DLT", so the destination is spelled out
    // explicitly rather than relying on the library default. Partition -1 lets the producer pick
    // a partition normally instead of trying to reuse the original record's partition index,
    // which may not exist on a DLT topic with a different partition count.
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafkaTemplate, (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
  }
}
