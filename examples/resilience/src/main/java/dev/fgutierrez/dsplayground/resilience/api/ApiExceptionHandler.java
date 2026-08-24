package dev.fgutierrez.dsplayground.resilience.api;

import dev.fgutierrez.dsplayground.resilience.order.OrderNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Only handles what Spring's built-in problem-details support (spring.mvc.problemdetails.enabled)
 * doesn't already cover for free — bean-validation failures (400) already come back as RFC 9457
 * Problem Details without any code here.
 */
@RestControllerAdvice
class ApiExceptionHandler {

  @ExceptionHandler(OrderNotFoundException.class)
  ProblemDetail handleOrderNotFound(OrderNotFoundException ex) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    problem.setTitle("Order not found");
    return problem;
  }
}
