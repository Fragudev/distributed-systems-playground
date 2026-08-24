package dev.fgutierrez.dsplayground.rabbitmqorders.outbox;

/**
 * Seam between OutboxRelay and the actual broker client — same role as EventPublisher in the
 * outbox/kafka-order-processing examples, adapted to RabbitMQ's model: {@code routingKey} selects
 * which bound queues receive the message (there's a single fixed exchange, see RabbitConfig), and
 * {@code correlationId} carries the aggregate id for tracing rather than for partitioning — that
 * concept doesn't exist in RabbitMQ, see the example README.
 */
public interface EventPublisher {

  void publish(String routingKey, String correlationId, String payload);
}
