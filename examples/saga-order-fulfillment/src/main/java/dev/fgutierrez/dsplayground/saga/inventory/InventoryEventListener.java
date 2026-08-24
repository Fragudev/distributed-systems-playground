package dev.fgutierrez.dsplayground.saga.inventory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelope;
import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelopeWriter;
import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import dev.fgutierrez.dsplayground.saga.outbox.IncomingEvent;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEvent;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * inventory-service's own choreography participant — structurally identical to PaymentEventListener
 * (same idempotency guard, same outbox publish, same compensation on order.cancelled.v1). Kept as a
 * full duplicate rather than a shared base class: the two listeners only look alike today, and a
 * shared abstraction would have to be guessed at before a third participant ever shows up to
 * justify it.
 */
@Component
public class InventoryEventListener {

  static final String CONSUMER_GROUP = "inventory-service";
  private static final String AGGREGATE_TYPE = "InventoryReservation";
  private static final String SOURCE = "inventory-service";

  private final InventoryReservationRepository reservationRepository;
  private final InventoryAvailabilityChecker checker;
  private final OutboxEventRepository outboxEventRepository;
  private final EventEnvelopeWriter envelopeWriter;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public InventoryEventListener(
      InventoryReservationRepository reservationRepository,
      InventoryAvailabilityChecker checker,
      OutboxEventRepository outboxEventRepository,
      EventEnvelopeWriter envelopeWriter,
      ObjectMapper objectMapper,
      Clock clock) {
    this.reservationRepository = reservationRepository;
    this.checker = checker;
    this.outboxEventRepository = outboxEventRepository;
    this.envelopeWriter = envelopeWriter;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(topics = EventTypes.ORDER_CREATED, groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrderCreated(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    if (reservationRepository.findByOrderId(event.orderId()).isPresent()) {
      return; // idempotent — this order already has an inventory outcome
    }

    List<String> productIds = productIdsOf(event.payload());
    boolean available = checker.isAvailable(event.orderId(), productIds);
    Instant now = Instant.now(clock);

    InventoryReservation reservation =
        new InventoryReservation(
            event.orderId(), available ? InventoryStatus.RESERVED : InventoryStatus.REJECTED, now);
    reservationRepository.save(reservation);
    outboxEventRepository.save(toOutcomeOutboxEvent(reservation, now));
  }

  @KafkaListener(topics = EventTypes.ORDER_CANCELLED, groupId = CONSUMER_GROUP)
  @Transactional
  public void onOrderCancelled(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    reservationRepository
        .findByOrderId(event.orderId())
        .ifPresent(
            reservation -> {
              reservation.release(Instant.now(clock)); // no-op unless it was actually RESERVED
              reservationRepository.save(reservation);
            });
  }

  private static List<String> productIdsOf(JsonNode payload) {
    return payload.get("lines").findValuesAsText("productId");
  }

  private OutboxEvent toOutcomeOutboxEvent(InventoryReservation reservation, Instant now) {
    String type =
        reservation.getStatus() == InventoryStatus.RESERVED
            ? EventTypes.INVENTORY_RESERVED
            : EventTypes.INVENTORY_REJECTED;
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            type,
            SOURCE,
            "inventory/" + reservation.getOrderId(),
            now,
            new InventoryOutcomePayload(reservation.getOrderId()));
    return new OutboxEvent(
        AGGREGATE_TYPE, reservation.getOrderId(), type, envelopeWriter.write(envelope), now);
  }
}
