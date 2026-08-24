package dev.fgutierrez.dsplayground.saga.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.saga.inventory.InventoryStatus;
import dev.fgutierrez.dsplayground.saga.order.OrderService;
import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import dev.fgutierrez.dsplayground.saga.outbox.IncomingEvent;
import dev.fgutierrez.dsplayground.saga.payment.PaymentStatus;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only component in this example that watches both legs of the saga and decides the order's
 * fate. It never calls payment or inventory directly — it only reacts to what they've already
 * published, then drives OrderService (confirm or cancel), which is itself just another participant
 * publishing order.cancelled.v1 for payment/inventory to compensate against. That one- directional
 * flow (participants -> coordinator -> order, never coordinator -> participants) is what keeps this
 * choreography rather than orchestration — see ADR 0008.
 */
@Component
public class SagaCoordinator {

  static final String CONSUMER_GROUP = "saga-coordinator";
  private static final String CANCEL_REASON = "compensation triggered by saga coordinator";

  private final SagaStateRepository sagaStateRepository;
  private final OrderService orderService;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public SagaCoordinator(
      SagaStateRepository sagaStateRepository,
      OrderService orderService,
      ObjectMapper objectMapper,
      Clock clock) {
    this.sagaStateRepository = sagaStateRepository;
    this.orderService = orderService;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @KafkaListener(
      topics = {EventTypes.PAYMENT_COMPLETED, EventTypes.PAYMENT_FAILED},
      groupId = CONSUMER_GROUP)
  @Transactional
  public void onPaymentOutcome(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    PaymentStatus status =
        event.type().equals(EventTypes.PAYMENT_COMPLETED)
            ? PaymentStatus.COMPLETED
            : PaymentStatus.FAILED;
    stateFor(event.orderId()).recordPaymentOutcome(status, Instant.now(clock));
    decideAndAct(event.orderId());
  }

  @KafkaListener(
      topics = {EventTypes.INVENTORY_RESERVED, EventTypes.INVENTORY_REJECTED},
      groupId = CONSUMER_GROUP)
  @Transactional
  public void onInventoryOutcome(String payload) {
    IncomingEvent event = IncomingEvent.parse(payload, objectMapper);
    InventoryStatus status =
        event.type().equals(EventTypes.INVENTORY_RESERVED)
            ? InventoryStatus.RESERVED
            : InventoryStatus.REJECTED;
    stateFor(event.orderId()).recordInventoryOutcome(status, Instant.now(clock));
    decideAndAct(event.orderId());
  }

  private SagaState stateFor(UUID orderId) {
    return sagaStateRepository
        .findById(orderId)
        .orElseGet(() -> sagaStateRepository.save(new SagaState(orderId, Instant.now(clock))));
  }

  private void decideAndAct(UUID orderId) {
    SagaState state = sagaStateRepository.findById(orderId).orElseThrow();
    SagaOutcome outcome = state.decideOutcome(Instant.now(clock));
    if (outcome == null) {
      return; // still waiting on the other leg, or already decided by a prior delivery
    }
    sagaStateRepository.save(state);
    switch (outcome) {
      case CONFIRMED -> orderService.confirmOrder(orderId);
      case CANCELLED -> orderService.cancelOrder(orderId, CANCEL_REASON);
    }
  }
}
