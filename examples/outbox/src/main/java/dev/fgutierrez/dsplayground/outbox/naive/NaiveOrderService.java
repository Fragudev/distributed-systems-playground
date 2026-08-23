package dev.fgutierrez.dsplayground.outbox.naive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fgutierrez.dsplayground.outbox.order.Order;
import dev.fgutierrez.dsplayground.outbox.order.OrderCreatedPayload;
import dev.fgutierrez.dsplayground.outbox.order.OrderLine;
import dev.fgutierrez.dsplayground.outbox.order.OrderRepository;
import dev.fgutierrez.dsplayground.outbox.outbox.EventEnvelope;
import dev.fgutierrez.dsplayground.outbox.outbox.EventPublisher;
import dev.fgutierrez.dsplayground.outbox.outbox.OutboxRelay;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * The "naive solution" from the example README, kept as real, runnable code instead of a
 * description — see NaiveDualWriteFailureTest. Not wired into any controller; this class exists
 * only to demonstrate the bug OrderService's transactional outbox fixes.
 *
 * <p>The bug: {@link OrderRepository#save} commits on its own (Spring Data repository methods are
 * transactional by default), and the publish call below happens strictly after that commit, with
 * nothing tying the two together. If the publish fails, the order is already durably saved and the
 * event is gone — there is no outbox row here for anything to retry from.
 */
@Service
public class NaiveOrderService {

  // Same event type as the fixed path (OutboxRelay.TOPIC) on purpose: this is meant to be the
  // same event, published badly, not a different one.
  private static final String EVENT_TYPE = OutboxRelay.TOPIC;
  private static final String SOURCE = "outbox-example-naive";

  private final OrderRepository orderRepository;
  private final EventPublisher eventPublisher;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  public NaiveOrderService(
      OrderRepository orderRepository,
      EventPublisher eventPublisher,
      ObjectMapper objectMapper,
      Clock clock) {
    this.orderRepository = orderRepository;
    this.eventPublisher = eventPublisher;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  public Order createOrder(String customerId, List<OrderLine> lines) {
    Order saved = orderRepository.save(Order.create(customerId, lines, clock));
    eventPublisher.publish(EVENT_TYPE, saved.getId().toString(), toJson(saved));
    return saved;
  }

  private String toJson(Order order) {
    Instant now = Instant.now(clock);
    EventEnvelope envelope =
        EventEnvelope.forNewAggregate(
            EVENT_TYPE, SOURCE, "order/" + order.getId(), now, OrderCreatedPayload.from(order));
    try {
      return objectMapper.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize event envelope", e);
    }
  }
}
