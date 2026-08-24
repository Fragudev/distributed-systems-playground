package dev.fgutierrez.dsplayground.resilience.shipping;

import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Stands in for a real shipping-carrier API this example doesn't have one of. Toggled live via
 * ShippingAdminController for the demo, or directly in tests — the same "controllable collaborator"
 * seam as InventoryAvailabilityChecker in the Kafka/RabbitMQ examples, just with more than one
 * failure shape (slow vs. down) since that's the point of this example.
 */
@Component
public class ShippingSimulator {

  public enum Mode {
    NORMAL,
    SLOW,
    FAILING
  }

  private final long slowDelayMs;
  private volatile Mode mode = Mode.NORMAL;

  public ShippingSimulator(
      @Value("${resilience.shipping-simulator.slow-delay-ms:2000}") long slowDelayMs) {
    this.slowDelayMs = slowDelayMs;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public Mode getMode() {
    return mode;
  }

  /**
   * What ShippingGateway implementations actually call. Blocks the calling thread for SLOW — on
   * purpose, that's the dependency this example's resilience patterns exist to survive.
   */
  public void callCarrier(UUID orderId) {
    switch (mode) {
      case NORMAL -> {
        // instant success
      }
      case SLOW -> sleepUninterruptibly();
      case FAILING -> throw new ShippingCarrierUnavailableException(orderId);
    }
  }

  private void sleepUninterruptibly() {
    try {
      Thread.sleep(slowDelayMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while simulating a slow carrier", e);
    }
  }
}
