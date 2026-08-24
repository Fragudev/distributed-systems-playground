package dev.fgutierrez.dsplayground.saga.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import dev.fgutierrez.dsplayground.saga.outbox.IncomingEvent;
import java.time.Clock;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only participant that reacts to order.cancelled.v1 without compensating anything — it has no
 * state of its own to undo, so it just tells the customer. Doesn't listen to order.created.v1 or
 * the outcome topics at all; per the plan's event table this service only cares about the terminal
 * failure case.
 */
@Component
public class NotificationEventListener {

  static final String CONSUMER_GROUP = "notification-service";

  private final NotificationLogRepository notificationLogRepository;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public NotificationEventListener(
      NotificationLogRepository notificationLogRepository, ObjectMapper objectMapper, Clock clock) {
    this.notificationLogRepository = notificationLogRepository;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = EventTypes.ORDER_CANCELLED, groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrderCancelled(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    if (notificationLogRepository.findByOrderId(event.orderId()).isPresent()) {
      return; // idempotent — already notified for this order
    }
    String reason = event.payload().path("reason").asText("unspecified");
    String message = "Order " + event.orderId() + " was cancelled: " + reason;
    notificationLogRepository.save(
        new NotificationLog(event.orderId(), message, Instant.now(clock)));
  }
}
