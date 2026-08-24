package dev.fgutierrez.dsplayground.saga.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.fgutierrez.dsplayground.saga.inventory.InventoryReservationRepository;
import dev.fgutierrez.dsplayground.saga.inventory.InventoryStatus;
import dev.fgutierrez.dsplayground.saga.order.Order;
import dev.fgutierrez.dsplayground.saga.order.OrderLine;
import dev.fgutierrez.dsplayground.saga.order.OrderRepository;
import dev.fgutierrez.dsplayground.saga.order.OrderService;
import dev.fgutierrez.dsplayground.saga.order.OrderStatus;
import dev.fgutierrez.dsplayground.saga.outbox.OutboxRelay;
import dev.fgutierrez.dsplayground.saga.payment.PaymentRepository;
import dev.fgutierrez.dsplayground.saga.payment.PaymentStatus;
import dev.fgutierrez.dsplayground.saga.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Both legs succeed, independently and without either one waiting for the other, and the saga
 * coordinator converges the order to CONFIRMED once both outcomes are in. Nothing in this test
 * drives payment or inventory directly — it only creates the order and publishes the outbox, then
 * waits for the choreography to run itself.
 */
class SagaHappyPathTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private OrderRepository orderRepository;
  @Autowired private PaymentRepository paymentRepository;
  @Autowired private InventoryReservationRepository inventoryReservationRepository;

  @Test
  void bothLegsSucceedingConvergesTheOrderToConfirmed() {
    Order order =
        orderService.createOrder("customer-1", List.of(new OrderLine("widget", 2, BigDecimal.TEN)));
    outboxRelay.publishPending();

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              // Publishes from payment/inventory (payment.completed.v1, inventory.reserved.v1)
              // only get relayed off their own outbox rows on the next OutboxRelay#poll tick, so
              // pumping it here — rather than only in @Scheduled — is what keeps this test from
              // depending on wall-clock timing.
              outboxRelay.publishPending();
              assertThat(orderRepository.findById(order.getId()).orElseThrow().getStatus())
                  .isEqualTo(OrderStatus.CONFIRMED);
            });

    assertThat(paymentRepository.findByOrderId(order.getId()).orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.COMPLETED);
    assertThat(
            inventoryReservationRepository.findByOrderId(order.getId()).orElseThrow().getStatus())
        .isEqualTo(InventoryStatus.RESERVED);
  }
}
