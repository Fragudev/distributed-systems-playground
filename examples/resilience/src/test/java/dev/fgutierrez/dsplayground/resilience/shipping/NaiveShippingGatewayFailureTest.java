package dev.fgutierrez.dsplayground.resilience.shipping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Makes the "naive solution" section of the README concrete: no Spring context needed here, because
 * the bug is entirely in NaiveShippingGateway's own logic — it never bounds how long a slow carrier
 * gets to hold the calling thread.
 */
class NaiveShippingGatewayFailureTest {

  private static final long SLOW_DELAY_MS = 300;

  @Test
  void blocksForTheFullDurationOfASlowCarrierWithNoProtection() {
    ShippingSimulator simulator = new ShippingSimulator(SLOW_DELAY_MS);
    simulator.setMode(ShippingSimulator.Mode.SLOW);
    NaiveShippingGateway gateway = new NaiveShippingGateway(simulator);

    long start = System.nanoTime();
    ShippingConfirmation confirmation = gateway.requestShipment(UUID.randomUUID()).join();
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertThat(confirmation.status()).isEqualTo(ShippingConfirmation.Status.CONFIRMED);
    assertThat(elapsedMs)
        .as("nothing bounds the wait — it blocks for the carrier's full delay, unprotected")
        .isGreaterThanOrEqualTo(SLOW_DELAY_MS);
  }
}
