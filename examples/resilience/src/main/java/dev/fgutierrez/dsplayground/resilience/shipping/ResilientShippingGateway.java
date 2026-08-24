package dev.fgutierrez.dsplayground.resilience.shipping;

import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Component;

/**
 * The improved solution (README §3): the same call NaiveShippingGateway makes, wrapped with three
 * Resilience4j patterns configured in application.yml under resilience4j.*.instances.shipping —
 * bulkhead (bounds concurrent in-flight calls to the thread pool below), time limiter (bounds how
 * long any one call can take), circuit breaker (stops calling a carrier that's already failing
 * instead of queueing every new order behind it). {@link #fallback} is what makes this graceful
 * degradation rather than just "fail differently": whatever trips — timeout, open circuit, full
 * bulkhead — the order still gets created; only its shipping confirmation is deferred.
 */
@Component
public class ResilientShippingGateway implements ShippingGateway {

  private final ShippingSimulator simulator;

  public ResilientShippingGateway(ShippingSimulator simulator) {
    this.simulator = simulator;
  }

  @CircuitBreaker(name = "shipping", fallbackMethod = "fallback")
  @Bulkhead(name = "shipping", type = Bulkhead.Type.THREADPOOL)
  @TimeLimiter(name = "shipping")
  @Override
  public CompletableFuture<ShippingConfirmation> requestShipment(UUID orderId) {
    return CompletableFuture.supplyAsync(
        () -> {
          simulator.callCarrier(orderId);
          return ShippingConfirmation.confirmed();
        });
  }

  // Package-private, not private: Resilience4j invokes this reflectively without forcing
  // accessibility, so a private method fails with IllegalAccessException at the first fallback.
  @SuppressWarnings("unused") // invoked reflectively by the fallbackMethod attribute above
  CompletableFuture<ShippingConfirmation> fallback(UUID orderId, Throwable cause) {
    return CompletableFuture.completedFuture(ShippingConfirmation.pending());
  }
}
