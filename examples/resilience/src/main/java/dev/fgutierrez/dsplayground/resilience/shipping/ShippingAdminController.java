package dev.fgutierrez.dsplayground.resilience.shipping;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Toggles the simulated carrier's behavior for the live demo — see the example README's "Running
 * this example" section. Not something a real system would expose; this is the chaos dial for this
 * playground specifically.
 */
@RestController
@RequestMapping("/admin/shipping-simulator")
public class ShippingAdminController {

  private final ShippingSimulator simulator;

  public ShippingAdminController(ShippingSimulator simulator) {
    this.simulator = simulator;
  }

  @GetMapping
  public ShippingSimulator.Mode getMode() {
    return simulator.getMode();
  }

  @PostMapping
  public void setMode(@RequestBody SetModeRequest request) {
    simulator.setMode(request.mode());
  }

  public record SetModeRequest(ShippingSimulator.Mode mode) {}
}
