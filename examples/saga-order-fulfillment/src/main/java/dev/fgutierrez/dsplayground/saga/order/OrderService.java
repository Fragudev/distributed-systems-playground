package dev.fgutierrez.dsplayground.saga.order;

import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelope;
import dev.fgutierrez.dsplayground.saga.outbox.EventEnvelopeWriter;
import dev.fgutierrez.dsplayground.saga.outbox.EventTypes;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEvent;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

  private static final String AGGREGATE_TYPE = "Order";
  private static final String SOURCE = "saga-order-fulfillment";

  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final EventEnvelopeWriter envelopeWriter;
  private final Clock clock;

  public OrderService(
      OrderRepository orderRepository,
      OutboxEventRepository outboxEventRepository,
      EventEnvelopeWriter envelopeWriter,
      Clock clock) {
    this.orderRepository = orderRepository;
    this.outboxEventRepository = outboxEventRepository;
    this.envelopeWriter = envelopeWriter;
    this.clock = clock;
  }

  /**
   * The order and the event that announces it are written in the same transaction (the
   * transactional outbox pattern — see examples/outbox for the naive dual-write this replaces and
   * why). There is no window in which one exists without the other.
   */
  @Transactional
  public Order createOrder(String customerId, List<OrderLine> lines) {
    Order order = orderRepository.save(Order.create(customerId, lines, clock));
    outboxEventRepository.save(toCreatedOutboxEvent(order));
    return order;
  }

  /**
   * No event published here on purpose: nothing in this example needs to react to a successful
   * confirmation, so there's nothing to announce — see README §3.
   */
  @Transactional
  public void confirmOrder(UUID orderId) {
    Order order = requireOrder(orderId);
    order.markConfirmed();
    orderRepository.save(order);
  }

  @Transactional
  public void cancelOrder(UUID orderId, String reason) {
    Order order = requireOrder(orderId);
    order.markCancelled();
    orderRepository.save(order);
    outboxEventRepository.save(toCancelledOutboxEvent(order, reason));
  }

  private Order requireOrder(UUID orderId) {
    return orderRepository
        .findById(orderId)
        .orElseThrow(() -> new IllegalStateException("Unknown order " + orderId));
  }

  private OutboxEvent toCreatedOutboxEvent(Order order) {
    Instant now = Instant.now(clock);
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            EventTypes.ORDER_CREATED,
            SOURCE,
            "order/" + order.getId(),
            now,
            OrderCreatedPayload.from(order));
    return new OutboxEvent(
        AGGREGATE_TYPE,
        order.getId(),
        EventTypes.ORDER_CREATED,
        envelopeWriter.write(envelope),
        now);
  }

  private OutboxEvent toCancelledOutboxEvent(Order order, String reason) {
    Instant now = Instant.now(clock);
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            EventTypes.ORDER_CANCELLED,
            SOURCE,
            "order/" + order.getId(),
            now,
            new OrderCancelledPayload(order.getId(), reason));
    return new OutboxEvent(
        AGGREGATE_TYPE,
        order.getId(),
        EventTypes.ORDER_CANCELLED,
        envelopeWriter.write(envelope),
        now);
  }
}
