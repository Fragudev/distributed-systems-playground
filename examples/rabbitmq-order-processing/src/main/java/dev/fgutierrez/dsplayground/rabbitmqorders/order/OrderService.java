package dev.fgutierrez.dsplayground.rabbitmqorders.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.EventEnvelope;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxEvent;
import dev.fgutierrez.dsplayground.rabbitmqorders.outbox.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

  private static final String AGGREGATE_TYPE = "Order";
  private static final String EVENT_TYPE = "order.created.v1";
  private static final String SOURCE = "outbox-example";

  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public OrderService(
      OrderRepository orderRepository,
      OutboxEventRepository outboxEventRepository,
      ObjectMapper objectMapper,
      Clock clock) {
    this.orderRepository = orderRepository;
    this.outboxEventRepository = outboxEventRepository;
    this.objectMapper = objectMapper;
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
    outboxEventRepository.save(toOutboxEvent(order));
    return order;
  }

  private OutboxEvent toOutboxEvent(Order order) {
    Instant now = Instant.now(clock);
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            EVENT_TYPE, SOURCE, "order/" + order.getId(), now, OrderCreatedPayload.from(order));
    return new OutboxEvent(AGGREGATE_TYPE, order.getId(), EVENT_TYPE, writeJson(envelope), now);
  }

  private String writeJson(EventEnvelope envelope) {
    try {
      return objectMapper.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      // Only possible here if EventEnvelope stops being trivially serializable — a programming
      // error, not a runtime condition callers should have to handle.
      throw new IllegalStateException("Failed to serialize event envelope", e);
    }
  }
}
