package dev.fgutierrez.dsplayground.saga.outbox;

public class EventPublishException extends RuntimeException {

  public EventPublishException(String topic, String key, Throwable cause) {
    super("Failed to publish event to topic '" + topic + "' with key '" + key + "'", cause);
  }
}
