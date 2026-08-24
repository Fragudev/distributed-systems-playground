package dev.fgutierrez.dsplayground.saga.payment;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class AlwaysApprovePaymentAuthorizer implements PaymentAuthorizer {

  @Override
  public boolean authorize(UUID orderId, BigDecimal amount) {
    return true;
  }
}
