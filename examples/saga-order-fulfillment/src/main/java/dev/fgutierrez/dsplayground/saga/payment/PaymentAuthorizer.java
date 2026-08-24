package dev.fgutierrez.dsplayground.saga.payment;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Seam between PaymentEventListener and whatever a real payment processor would be — same "wrap a
 * real collaborator, swap it in tests" approach as every controllable checker elsewhere in this
 * playground (EventPublisher, InventoryAvailabilityChecker, ...).
 */
public interface PaymentAuthorizer {

  boolean authorize(UUID orderId, BigDecimal amount);
}
