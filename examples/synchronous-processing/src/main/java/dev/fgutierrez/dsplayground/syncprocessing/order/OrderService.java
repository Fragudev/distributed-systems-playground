package dev.fgutierrez.dsplayground.syncprocessing.order;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

  private final OrderRepository orderRepository;
  private final Clock clock;

  public OrderService(OrderRepository orderRepository, Clock clock) {
    this.orderRepository = orderRepository;
    this.clock = clock;
  }

  /**
   * Persists the order and all of its lines in one transaction: either the whole aggregate is
   * written, or none of it is. See OrderTransactionBoundaryTest for what happens when a line
   * violates the database's own constraints.
   */
  @Transactional
  public Order createOrder(String customerId, List<OrderLine> lines) {
    return orderRepository.save(Order.create(customerId, lines, clock));
  }

  @Transactional(readOnly = true)
  public Order getOrder(UUID id) {
    return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
  }
}
