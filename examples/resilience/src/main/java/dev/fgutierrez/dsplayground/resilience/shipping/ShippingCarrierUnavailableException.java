package dev.fgutierrez.dsplayground.resilience.shipping;

import java.util.UUID;

public class ShippingCarrierUnavailableException extends RuntimeException {

  public ShippingCarrierUnavailableException(UUID orderId) {
    super("Shipping carrier unavailable for order " + orderId);
  }
}
