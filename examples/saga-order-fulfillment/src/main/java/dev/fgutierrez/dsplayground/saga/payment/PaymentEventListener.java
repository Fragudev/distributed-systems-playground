package dev.fgutierrez.dsplayground.saga.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelope;
import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelopeWriter;
import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import dev.fgutierrez.dsplayground.saga.outbox.IncomingEvent;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEvent;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEventRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * One participant in the choreography: reacts to order.created.v1 on its own, with no one telling
 * it to — that's what makes this choreography rather than orchestration (ADR 0008). Publishes its
 * own outcome via the same outbox pattern every publisher in this playground uses, and separately
 * reacts to order.cancelled.v1 to compensate — never called directly by SagaCoordinator or anyone
 * else.
 */
@Component
public class PaymentEventListener {

  static final String CONSUMER_GROUP = "payment-service";
  private static final String AGGREGATE_TYPE = "Payment";
  private static final String SOURCE = "payment-service";

  private final PaymentRepository paymentRepository;
  private final PaymentAuthorizer authorizer;
  private final OutboxEventRepository outboxEventRepository;
  private final EventEnvelopeWriter envelopeWriter;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public PaymentEventListener(
      PaymentRepository paymentRepository,
      PaymentAuthorizer authorizer,
      OutboxEventRepository outboxEventRepository,
      EventEnvelopeWriter envelopeWriter,
      ObjectMapper objectMapper,
      Clock clock) {
    this.paymentRepository = paymentRepository;
    this.authorizer = authorizer;
    this.outboxEventRepository = outboxEventRepository;
    this.envelopeWriter = envelopeWriter;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = EventTypes.ORDER_CREATED, groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrderCreated(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    if (paymentRepository.findByOrderId(event.orderId()).isPresent()) {
      return; // idempotent — this order already has a payment outcome
    }

    BigDecimal amount = new BigDecimal(event.payload().get("totalAmount").asText());
    boolean approved = authorizer.authorize(event.orderId(), amount);
    Instant now = Instant.now(clock);

    Payment payment =
        new Payment(
            event.orderId(),
            approved ? PaymentStatus.COMPLETED : PaymentStatus.FAILED,
            amount,
            now);
    paymentRepository.save(payment);
    outboxEventRepository.save(toOutcomeOutboxEvent(payment, now));
  }

  @KafkaListener(topics = EventTypes.ORDER_CANCELLED, groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrderCancelled(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    // No outbox event here: nothing in this example needs to react to "a refund happened," and
    // the refund itself only matters as of this listener's own state — see the example README.
    paymentRepository
        .findByOrderId(event.orderId())
        .ifPresent(
            payment -> {
              payment.refund(Instant.now(clock)); // no-op unless it was actually COMPLETED
              paymentRepository.save(payment);
            });
  }

  private OutboxEvent toOutcomeOutboxEvent(Payment payment, Instant now) {
    String type =
        payment.getStatus() == PaymentStatus.COMPLETED
            ? EventTypes.PAYMENT_COMPLETED
            : EventTypes.PAYMENT_FAILED;
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            type,
            SOURCE,
            "payment/" + payment.getOrderId(),
            now,
            new PaymentOutcomePayload(payment.getOrderId()));
    return new OutboxEvent(
        AGGREGATE_TYPE, payment.getOrderId(), type, envelopeWriter.write(envelope), now);
  }
}
