package dev.fgutierrez.dsplayground.saga.support;

import dev.fgutierrez.dsplayground.saga.inventory.InventoryAvailabilityChecker;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Swaps in a fake that starts out available for every order (same happy-path default as
 * AlwaysAvailableInventoryChecker) but lets a test mark specific orders as out of stock — the
 * controllable-collaborator seam InventoryAvailabilityChecker exists for.
 */
@TestConfiguration
public class ControllableInventoryConfig {

  @Bean
  @Primary
  ControllableInventoryAvailabilityChecker inventoryAvailabilityChecker() {
    return new ControllableInventoryAvailabilityChecker();
  }

  public static class ControllableInventoryAvailabilityChecker
      implements InventoryAvailabilityChecker {

    private final Set<UUID> rejectedOrders = ConcurrentHashMap.newKeySet();

    public void rejectOrder(UUID orderId) {
      rejectedOrders.add(orderId);
    }

    @Override
    public boolean isAvailable(UUID orderId, List<String> productIds) {
      return !rejectedOrders.contains(orderId);
    }
  }
}
