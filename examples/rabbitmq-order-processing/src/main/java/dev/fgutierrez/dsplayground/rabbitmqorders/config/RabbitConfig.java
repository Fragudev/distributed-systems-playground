package dev.fgutierrez.dsplayground.rabbitmqorders.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The whole point of comparison with Kafka: this retry/DLQ chain for inventory-service is entirely
 * queue configuration, declared once at startup — no manual retry-topic juggling, no application
 * code deciding which topic to republish to. See docs/adr/0006-retry-dlq-strategy.md.
 */
@Configuration
public class RabbitConfig {

  public static final String EXCHANGE = "order-events";
  public static final String ROUTING_KEY = "order.created.v1";

  public static final String PAYMENT_QUEUE = "payment-service";
  public static final String NOTIFICATION_QUEUE = "notification-service";

  public static final String INVENTORY_QUEUE = "inventory-service";
  public static final String INVENTORY_RETRY_QUEUE = "inventory-service.retry";
  public static final String INVENTORY_DLQ = "inventory-service.dlq";

  /**
   * Fixed backoff delay for inventory-service's retry hop. Kafka's ExponentialBackOff grows per
   * attempt; a single-TTL RabbitMQ retry queue like this one doesn't, unless you chain several wait
   * queues with increasing TTLs — a real operational trade-off, not an oversight, documented in the
   * example README rather than papered over with extra queues here.
   */
  private static final int RETRY_DELAY_MS = 1000;

  @Bean
  TopicExchange orderEventsExchange() {
    return new TopicExchange(EXCHANGE, true, false);
  }

  @Bean
  Queue paymentQueue() {
    return QueueBuilder.durable(PAYMENT_QUEUE).build();
  }

  @Bean
  Queue notificationQueue() {
    return QueueBuilder.durable(NOTIFICATION_QUEUE).build();
  }

  /**
   * On reject-without-requeue, RabbitMQ dead-letters here via the default exchange straight to the
   * retry queue — "x-dead-letter-exchange"="" plus a routing key is the standard way to dead-letter
   * directly to a named queue.
   */
  @Bean
  Queue inventoryQueue() {
    return QueueBuilder.durable(INVENTORY_QUEUE)
        .withArgument("x-dead-letter-exchange", "")
        .withArgument("x-dead-letter-routing-key", INVENTORY_RETRY_QUEUE)
        .build();
  }

  /**
   * Never consumed directly: a message sits here for RETRY_DELAY_MS, then RabbitMQ dead-letters it
   * again — this time back onto the main queue for another attempt. The delay itself is entirely
   * this queue's TTL; InventoryEventListener never sleeps.
   */
  @Bean
  Queue inventoryRetryQueue() {
    return QueueBuilder.durable(INVENTORY_RETRY_QUEUE)
        .withArgument("x-message-ttl", RETRY_DELAY_MS)
        .withArgument("x-dead-letter-exchange", "")
        .withArgument("x-dead-letter-routing-key", INVENTORY_QUEUE)
        .build();
  }

  /**
   * The end of the line: InventoryEventListener publishes here explicitly once it has counted
   * enough retries via the x-death header, instead of letting the queue's own DLX send it back
   * around the retry loop again.
   */
  @Bean
  Queue inventoryDlq() {
    return QueueBuilder.durable(INVENTORY_DLQ).build();
  }

  @Bean
  Binding paymentBinding(Queue paymentQueue, TopicExchange orderEventsExchange) {
    return BindingBuilder.bind(paymentQueue).to(orderEventsExchange).with(ROUTING_KEY);
  }

  @Bean
  Binding notificationBinding(Queue notificationQueue, TopicExchange orderEventsExchange) {
    return BindingBuilder.bind(notificationQueue).to(orderEventsExchange).with(ROUTING_KEY);
  }

  @Bean
  Binding inventoryBinding(Queue inventoryQueue, TopicExchange orderEventsExchange) {
    return BindingBuilder.bind(inventoryQueue).to(orderEventsExchange).with(ROUTING_KEY);
  }
}
