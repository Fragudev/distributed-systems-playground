package dev.fgutierrez.dsplayground.resilience.api;

import static org.assertj.core.api.Assertions.assertThat;

import dev.fgutierrez.dsplayground.resilience.shipping.ShippingSimulator;
import dev.fgutierrez.dsplayground.resilience.support.PostgresIntegrationTest;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * The project's failure scenario 4, end to end through the real HTTP API: a slow or failing
 * shipping carrier never turns order creation into a 500 — it turns it into a 201 with
 * shippingStatus=PENDING_CONFIRMATION. Every test resets the circuit breaker afterward since all
 * three share this class's Spring context (see the lesson from kafka-order-processing's
 * ConsumerGroupsTest — cross-test isolation has to be explicit here too).
 */
class OrderApiResilienceTest extends PostgresIntegrationTest {

  // Stays PER_METHOD (the default) rather than @TestInstance(PER_CLASS): PER_CLASS changes
  // extension callback ordering enough that Testcontainers no longer reliably starts the static
  // @Container in PostgresIntegrationTest before @DynamicPropertySource reads its mapped port —
  // confirmed by a real ApplicationContext failure to load. This guard gets the same "exactly
  // once" warm-up without touching the instance lifecycle.
  private static final AtomicBoolean warmedUp = new AtomicBoolean(false);

  @Autowired private TestRestTemplate restTemplate;
  @Autowired private ShippingSimulator simulator;
  @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;

  // One throwaway call before the first real test, absorbing the bulkhead thread pool's cold
  // start (thread creation, first-time AOP proxy/reflection resolution through Resilience4j)
  // outside of any assertion. Without this, whichever test JUnit happens to run first pays that
  // cost inline and can spuriously blow the 500ms time limiter on a loaded CI runner even in
  // NORMAL mode, which never actually sleeps — a real flake this fixed, not a hypothetical one.
  @BeforeEach
  void warmUpShippingGatewayOnce() {
    if (warmedUp.compareAndSet(false, true)) {
      simulator.setMode(ShippingSimulator.Mode.NORMAL);
      createOrder();
      circuitBreakerRegistry.circuitBreaker("shipping").reset();
    }
  }

  @AfterEach
  void resetSharedState() {
    simulator.setMode(ShippingSimulator.Mode.NORMAL);
    circuitBreakerRegistry.circuitBreaker("shipping").reset();
  }

  @Test
  void confirmsShippingWhenTheCarrierIsHealthy() {
    simulator.setMode(ShippingSimulator.Mode.NORMAL);

    ResponseEntity<OrderResponse> response = createOrder();

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(response.getBody().shippingStatus()).isEqualTo("CONFIRMED");
  }

  @Test
  void degradesGracefullyWhenTheCarrierIsSlow() {
    simulator.setMode(ShippingSimulator.Mode.SLOW);

    ResponseEntity<OrderResponse> response = createOrder();

    assertThat(response.getStatusCode())
        .as("the order is still created — a slow carrier isn't the order's problem")
        .isEqualTo(HttpStatus.CREATED);
    assertThat(response.getBody().shippingStatus()).isEqualTo("PENDING_CONFIRMATION");
  }

  @Test
  void openCircuitAfterRepeatedFailuresAndKeepsDegradingGracefully() {
    simulator.setMode(ShippingSimulator.Mode.FAILING);

    // minimum-number-of-calls=5 in application.yml — enough failing calls to trip the breaker.
    IntStream.range(0, 6).forEach(i -> createOrder());

    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("shipping");
    assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

    // Once open, the circuit breaker short-circuits before even attempting the carrier — proven
    // by elapsed time, not just the eventual outcome.
    long startNanos = System.nanoTime();
    ResponseEntity<OrderResponse> response = createOrder();
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(response.getBody().shippingStatus()).isEqualTo("PENDING_CONFIRMATION");
    assertThat(elapsedMs)
        .as("an open circuit fails fast — it never actually waits on the carrier")
        .isLessThan(200);
  }

  @Test
  void bulkheadCapsConcurrentCallsButEveryOrderStillSucceeds() throws Exception {
    simulator.setMode(ShippingSimulator.Mode.SLOW);
    int concurrentRequests = 10; // well beyond the bulkhead's capacity (2 core + 4 max + 2 queued)

    ExecutorService executor = Executors.newFixedThreadPool(concurrentRequests);
    try {
      List<Callable<ResponseEntity<OrderResponse>>> tasks =
          IntStream.range(0, concurrentRequests)
              .<Callable<ResponseEntity<OrderResponse>>>mapToObj(i -> this::createOrder)
              .toList();
      List<Future<ResponseEntity<OrderResponse>>> futures =
          executor.invokeAll(tasks, 15, TimeUnit.SECONDS);

      List<ResponseEntity<OrderResponse>> responses =
          futures.stream().map(this::getUnchecked).toList();

      assertThat(responses)
          .as("no request ever fails outright, regardless of whether the bulkhead let it through")
          .allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
      long degraded =
          responses.stream()
              .filter(r -> "PENDING_CONFIRMATION".equals(r.getBody().shippingStatus()))
              .count();
      assertThat(degraded)
          .as("with 10 concurrent slow calls against a 2-4-2 bulkhead, at least some must degrade")
          .isGreaterThan(0);
    } finally {
      executor.shutdown();
    }
  }

  private ResponseEntity<OrderResponse> getUnchecked(Future<ResponseEntity<OrderResponse>> future) {
    try {
      return future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ResponseEntity<OrderResponse> createOrder() {
    CreateOrderRequest request =
        new CreateOrderRequest(
            "customer-1", List.of(new OrderLineRequest("widget", 1, new BigDecimal("9.99"))));
    return restTemplate.postForEntity("/api/v1/orders", request, OrderResponse.class);
  }
}
