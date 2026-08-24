package dev.fgutierrez.dsplayground.saga.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Plain at-most-default-retry container factory — no DeadLetterPublishingRecoverer here. Retry and
 * DLT handling are already demonstrated end-to-end in kafka-order-processing; adding the same
 * machinery here would just be noise around what this example is actually about (choreography and
 * compensation). Bean name matches Spring Kafka's default listener container factory name, so
 * every @KafkaListener across all four participants picks this up without being told to.
 */
@Configuration
public class KafkaListenerConfig {

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    return factory;
  }
}
