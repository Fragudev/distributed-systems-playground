package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import org.springframework.stereotype.Component;

@Component
public class AlwaysAvailableInventoryChecker implements InventoryAvailabilityChecker {

  @Override
  public boolean isAvailable() {
    return true;
  }
}
