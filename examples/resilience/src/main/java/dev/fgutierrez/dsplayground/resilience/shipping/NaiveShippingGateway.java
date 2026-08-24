package dev.fgutierrez.dsplayground.resilience.shipping;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The naive solution from the example README, kept as real, runnable code — see
 * NaiveShippingGatewayFailureTest. Deliberately not a Spring bean: it exists only to demonstrate
 * the failure ResilientShippingGateway's Bulkhead/TimeLimiter/CircuitBreaker fix, not to be wired
 * into the app.
 *
 * <p>The bug: nothing here bounds how long the call can take, how many can run at once, or stops
 * retrying a carrier that's already down. A slow carrier means a request thread blocks for exactly
 * as long as the carrier takes — with enough concurrent orders, that's every request thread the
 * server has, not just the ones talking to shipping.
 */
public class NaiveShippingGateway implements ShippingGateway {

  private final ShippingSimulator simulator;

  public NaiveShippingGateway(ShippingSimulator simulator) {
    this.simulator = simulator;
  }

  @Override
  public CompletableFuture<ShippingConfirmation> requestShipment(UUID orderId) {
    simulator.callCarrier(orderId);
    return CompletableFuture.completedFuture(ShippingConfirmation.confirmed());
  }
}
