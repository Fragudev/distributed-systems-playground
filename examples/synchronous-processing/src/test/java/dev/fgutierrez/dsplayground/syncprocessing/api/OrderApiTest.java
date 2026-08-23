package dev.fgutierrez.dsplayground.syncprocessing.api;

import static org.assertj.core.api.Assertions.assertThat;

import dev.fgutierrez.dsplayground.syncprocessing.support.PostgresIntegrationTest;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

class OrderApiTest extends PostgresIntegrationTest {

  @Autowired private TestRestTemplate restTemplate;

  @Test
  void createsAnOrderAndReturnsItWithALocationHeader() {
    CreateOrderRequest request =
        new CreateOrderRequest(
            "customer-42",
            List.of(
                new OrderLineRequest("widget", 2, new BigDecimal("9.99")),
                new OrderLineRequest("gadget", 1, new BigDecimal("19.50"))));

    ResponseEntity<OrderResponse> response =
        restTemplate.postForEntity("/api/v1/orders", request, OrderResponse.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(response.getHeaders().getLocation()).isNotNull();

    OrderResponse body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.customerId()).isEqualTo("customer-42");
    assertThat(body.status()).isEqualTo("CREATED");
    assertThat(body.totalAmount()).isEqualByComparingTo("39.48");
    assertThat(body.lines()).hasSize(2);

    ResponseEntity<OrderResponse> fetched =
        restTemplate.getForEntity(response.getHeaders().getLocation(), OrderResponse.class);
    assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(fetched.getBody().id()).isEqualTo(body.id());
  }

  @Test
  void rejectsAnOrderWithNoLinesAsAProblemDetail() {
    CreateOrderRequest request = new CreateOrderRequest("customer-42", List.of());

    ResponseEntity<ProblemDetail> response =
        restTemplate.postForEntity("/api/v1/orders", request, ProblemDetail.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getHeaders().getContentType())
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getStatus()).isEqualTo(400);
  }

  @Test
  void returnsAProblemDetailWhenTheOrderDoesNotExist() {
    ResponseEntity<ProblemDetail> response =
        restTemplate.getForEntity("/api/v1/orders/" + UUID.randomUUID(), ProblemDetail.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getTitle()).isEqualTo("Order not found");
  }
}
