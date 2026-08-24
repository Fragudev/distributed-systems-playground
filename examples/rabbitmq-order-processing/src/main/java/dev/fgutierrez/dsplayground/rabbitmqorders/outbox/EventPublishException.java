package dev.fgutierrez.dsplayground.rabbitmqorders.outbox;

public class EventPublishException extends RuntimeException {

  public EventPublishException(String routingKey, String correlationId, Throwable cause) {
    super(
        "Failed to publish event with routing key '"
            + routingKey
            + "' and correlation id '"
            + correlationId
            + "'",
        cause);
  }
}
