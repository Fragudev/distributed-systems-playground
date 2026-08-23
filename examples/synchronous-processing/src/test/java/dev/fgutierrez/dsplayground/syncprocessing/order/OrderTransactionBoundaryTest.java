package dev.fgutierrez.dsplayground.syncprocessing.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.fgutierrez.dsplayground.syncprocessing.support.PostgresIntegrationTest;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * The API layer's Bean Validation (@Positive on OrderLineRequest.quantity) is what a real client
 * hits, so it never lets a zero-quantity line reach the service. This test bypasses that layer on
 * purpose — building the OrderLine entity directly — to prove the second line of defense: the
 * database's own CHECK constraint (see V2__create_orders_schema.sql) still rejects it, and
 * OrderService.createOrder's single @Transactional boundary means the order itself is never left
 * behind half-written.
 */
class OrderTransactionBoundaryTest extends PostgresIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OrderRepository orderRepository;

  @Test
  void rollsBackTheWholeOrderWhenALineViolatesTheDatabaseConstraint() {
    long before = orderRepository.count();

    List<OrderLine> lines =
        List.of(
            new OrderLine("widget", 2, new BigDecimal("9.99")),
            new OrderLine("broken-line", 0, new BigDecimal("5.00")));

    assertThatThrownBy(() -> orderService.createOrder("customer-99", lines))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(orderRepository.count())
        .as("the valid first line must not leave an orphaned order behind")
        .isEqualTo(before);
  }
}
