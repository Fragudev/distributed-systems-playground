package dev.fgutierrez.dsplayground.resilience.shipping;

/**
 * What ShippingGateway.requestShipment resolves to — always, even when the dependency is down.
 * There's deliberately no "failed" outcome here: a shipping-carrier hiccup degrades to PENDING
 * rather than failing order creation. See ResilientShippingGateway#fallback.
 */
public record ShippingConfirmation(Status status) {

  public enum Status {
    CONFIRMED,
    PENDING_CONFIRMATION
  }

  public static ShippingConfirmation confirmed() {
    return new ShippingConfirmation(Status.CONFIRMED);
  }

  public static ShippingConfirmation pending() {
    return new ShippingConfirmation(Status.PENDING_CONFIRMATION);
  }
}
