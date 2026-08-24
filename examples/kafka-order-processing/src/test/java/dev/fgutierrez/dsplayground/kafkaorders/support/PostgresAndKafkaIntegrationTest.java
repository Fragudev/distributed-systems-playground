package dev.fgutierrez.dsplayground.kafkaorders.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/** Shared Testcontainers Postgres + Kafka setup for every integration test in this example. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class PostgresAndKafkaIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

  // The generic org.testcontainers.kafka.KafkaContainer against the raw apache/kafka image hits
  // a known advertised-listener bug on this host's Docker networking (KAFKA_ADVERTISED_LISTENERS
  // resolves to the nonroutable 0.0.0.0). ConfluentKafkaContainer is the actively-maintained,
  // battle-tested path and sidesteps it entirely; the app/production compose stack still uses
  // apache/kafka directly (docker-compose.yml), where the listeners are set by hand.
  @Container
  static final ConfluentKafkaContainer kafka =
      new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1");

  @DynamicPropertySource
  static void dynamicProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  /** Reads every record currently on the topic, waiting up to {@code timeout} for the first one. */
  protected List<ConsumerRecord<String, String>> consumeAll(String topic, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + System.nanoTime());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + timeout.toNanos();
      // Keep polling past the first non-empty batch for a short grace period: the topic can
      // legitimately deliver in more than one batch even for a handful of records.
      Duration remaining = timeout;
      while (System.nanoTime() < deadline) {
        ConsumerRecords<String, String> batch =
            consumer.poll(
                remaining.compareTo(Duration.ofSeconds(2)) > 0 ? Duration.ofSeconds(2) : remaining);
        batch.forEach(records::add);
        remaining = Duration.ofNanos(deadline - System.nanoTime());
        if (batch.isEmpty() && !records.isEmpty()) {
          break;
        }
      }
    }
    return records;
  }

  /**
   * Publishes one raw message directly, bypassing the outbox/relay — used by tests that only care
   * about consumer-side behavior (partitioning, replay) and don't need a real order.
   */
  protected void publish(String topic, String key, String value) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      try {
        producer.send(new ProducerRecord<>(topic, key, value)).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while publishing to " + topic, e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to publish to " + topic, e);
      }
    }
  }
}
