package dev.fgutierrez.dsplayground.rabbitmqorders.outbox;

import dev.fgutierrez.dsplayground.rabbitmqorders.config.RabbitConfig;
import java.nio.charset.StandardCharsets;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class RabbitEventPublisher implements EventPublisher {

  private final RabbitTemplate rabbitTemplate;

  public RabbitEventPublisher(RabbitTemplate rabbitTemplate) {
    this.rabbitTemplate = rabbitTemplate;
  }

  @Override
  public void publish(String routingKey, String correlationId, String payload) {
    Message message =
        new Message(
            payload.getBytes(StandardCharsets.UTF_8),
            MessagePropertiesBuilder.newInstance().setCorrelationId(correlationId).build());
    try {
      // Publisher confirms (spring.rabbitmq.publisher-confirm-type=correlated): the relay must
      // know the broker actually accepted the message before marking the row published — same
      // rationale as blocking on the Kafka producer's future in the Kafka examples.
      rabbitTemplate.invoke(
          operations -> {
            operations.send(RabbitConfig.EXCHANGE, routingKey, message);
            operations.waitForConfirmsOrDie(10_000);
            return null;
          });
    } catch (AmqpException e) {
      throw new EventPublishException(routingKey, correlationId, e);
    }
  }
}
