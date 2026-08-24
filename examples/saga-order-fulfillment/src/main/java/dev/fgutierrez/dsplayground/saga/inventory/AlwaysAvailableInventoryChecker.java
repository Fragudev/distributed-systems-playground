package dev.fgutierrez.dsplayground.saga.inventory;

import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class AlwaysAvailableInventoryChecker implements InventoryAvailabilityChecker {

  @Override
  public boolean isAvailable(UUID orderId, List<String> productIds) {
    return true;
  }
}
