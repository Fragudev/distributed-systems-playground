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
import dev.fgutierrez.dsplayground.saga.support.ControllableInventoryConfig;
import dev.fgutierrez.dsplayground.saga.support.PostgresAndKafkaIntegrationTest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Failure scenario 5 from the plan: payment succeeds but inventory rejects the order. The saga
 * coordinator doesn't wait for a doomed order to finish failing before compensating what already
 * succeeded — it cancels the order as soon as the rejection is known, which triggers payment's own
 * compensation (refund) via order.cancelled.v1. Awaitility proves all three write models converge
 * to their correct terminal state within a bounded time, rather than assuming it.
 */
@Import(ControllableInventoryConfig.class)
class SagaCompensationTest extends PostgresAndKafkaIntegrationTest {

  @Autowired private OrderService orderService;
  @Autowired private OutboxRelay outboxRelay;
  @Autowired private OrderRepository orderRepository;
  @Autowired private PaymentRepository paymentRepository;
  @Autowired private InventoryReservationRepository inventoryReservationRepository;
  @Autowired private ControllableInventoryConfig.ControllableInventoryAvailabilityChecker checker;

  @Test
  void inventoryRejectionCompensatesTheAlreadyCompletedPayment() {
    Order order =
        orderService.createOrder(
            "customer-2", List.of(new OrderLine("out-of-stock-widget", 1, BigDecimal.TEN)));
    checker.rejectOrder(order.getId());
    outboxRelay.publishPending();

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              outboxRelay.publishPending();
              assertThat(orderRepository.findById(order.getId()).orElseThrow().getStatus())
                  .isEqualTo(OrderStatus.CANCELLED);
            });

    assertThat(
            inventoryReservationRepository.findByOrderId(order.getId()).orElseThrow().getStatus())
        .isEqualTo(InventoryStatus.REJECTED);

    // Payment had already completed by the time inventory rejected — this is the actual
    // compensation, not just "never charged in the first place".
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () ->
                assertThat(paymentRepository.findByOrderId(order.getId()).orElseThrow().getStatus())
                    .isEqualTo(PaymentStatus.REFUNDED));
  }
}
