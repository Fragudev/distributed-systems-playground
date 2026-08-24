package dev.fgutierrez.dsplayground.saga.outbox;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventPublisher implements EventPublisher {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public void publish(String topic, String key, String payload) {
    try {
      // Blocking on purpose: the relay must know the broker actually acknowledged the write
      // before it marks the row published. Fire-and-forget here would silently reintroduce the
      // same "maybe it arrived, maybe it didn't" gap the outbox pattern exists to close.
      kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventPublishException(topic, key, e);
    } catch (ExecutionException | TimeoutException e) {
      throw new EventPublishException(topic, key, e);
    }
  }
}
