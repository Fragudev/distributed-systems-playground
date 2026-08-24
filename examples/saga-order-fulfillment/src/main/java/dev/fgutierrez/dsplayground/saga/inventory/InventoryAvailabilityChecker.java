package dev.fgutierrez.dsplayground.saga.inventory;

import java.util.List;
import java.util.UUID;

/**
 * Same controllable-collaborator seam as PaymentAuthorizer — swapped out in tests to force the
 * rejection path that drives compensation (failure scenario 5).
 */
public interface InventoryAvailabilityChecker {

  boolean isAvailable(UUID orderId, List<String> productIds);
}
