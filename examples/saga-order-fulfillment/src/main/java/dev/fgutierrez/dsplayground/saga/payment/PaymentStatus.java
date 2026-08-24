package dev.fgutierrez.dsplayground.saga.payment;

public enum PaymentStatus {
  COMPLETED,
  FAILED,
  /**
   * The compensating outcome, applied only to a payment that was COMPLETED — see
   * PaymentEventListener#onOrderCancelled.
   */
  REFUNDED
}
